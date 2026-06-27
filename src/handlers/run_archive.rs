// src/handlers/run_archive.rs

//! Archive fallback — reconstructs run metadata and logs from
//! tekton-results Postgres + Loki, for PipelineRuns no longer in the
//! cluster. This is the Rust equivalent of query-run.sh, but structured
//! so the SSE handler can stream it the same way as a live run instead of
//! printing it.
//!
//! NOTE: this hits Postgres and Loki directly over the network (same as
//! query-run.sh execs into the postgres/loki pods via kubectl). Since this
//! service runs in-cluster, we connect straight to the service DNS names
//! instead of shelling out through kubectl exec — that's the "as native
//! as possible" approach for a long-running service rather than a
//! one-shot script.

use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::NoTls;

use crate::models::run_stream::{LogLine, RunMeta, RunSource, RunStatus, StepMeta, TaskMeta};

// in run_archive.rs, replace the const block with:

fn pg_host() -> String {
    std::env::var("TEKTON_RESULTS_PG_HOST")
        .unwrap_or_else(|_| "tekton-results-postgres.tekton-pipelines.svc.cluster.local".into())
}
fn pg_user() -> String {
    std::env::var("TEKTON_RESULTS_PG_USER")
        .unwrap_or_else(|_| "tekton".into())
}
fn pg_db() -> String {
    std::env::var("TEKTON_RESULTS_PG_DB")
        .unwrap_or_else(|_| "tekton-results".into())
}
fn pg_port() -> u16 {
    std::env::var("TEKTON_RESULTS_PG_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5432)
}

const LOKI_BASE_URL: &str = "http://loki.logging.svc.cluster.local:3100";
const LOKI_LOOKBACK_DAYS: i64 = 31;

#[derive(Debug)]
pub enum ArchiveError {
    NotFound,
    Db(String),
    Loki(String),
}

impl std::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchiveError::NotFound => write!(f, "run not found in archive"),
            ArchiveError::Db(e) => write!(f, "postgres error: {e}"),
            ArchiveError::Loki(e) => write!(f, "loki error: {e}"),
        }
    }
}

// ── Postgres ───────────────────────────────────────────────────────────
//
// tekton-results stores each PipelineRun/TaskRun as a JSON blob under
// `records.data` — the same table query-run.sh's `pg()` helper hits via
// psql. We use `tokio-postgres` directly: a true async client, so the
// SSE handler's polling loop never blocks the tokio runtime waiting on a
// DB round trip (unlike diesel's sync `PgConnection`, which would need
// `spawn_blocking` for every call). Since this is read-only, ad hoc JSON
// querying against a database this service has no schema/migrations
// for, a raw async client is a better fit than wiring up diesel for it.
//
// Add to Cargo.toml:
//   tokio-postgres = { version = "0.7", features = ["with-serde_json-1"] }

async fn pg_connect() -> Result<tokio_postgres::Client, ArchiveError> {
    let pg_pass = std::env::var("TEKTON_RESULTS_PG_PASSWORD")
        .map_err(|_| ArchiveError::Db("TEKTON_RESULTS_PG_PASSWORD not set".into()))?;

    let conn_str = format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=5",
        pg_host(), pg_port(), pg_user(), pg_pass, pg_db()
    );

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| ArchiveError::Db(e.to_string()))?;

    // Drive the connection in the background; log if it dies mid-query.
    // This is a short-lived connection per request — fine at the call
    // volume an archive-fallback path sees. If this becomes hot, swap in
    // a pool (bb8 + bb8-postgres / deadpool-postgres) without changing
    // any call site below.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tekton-results postgres connection error: {e}");
        }
    });

    Ok(client)
}

/// Look up the stored PipelineRun + TaskRun records for `run_name`.
/// Returns `Err(ArchiveError::NotFound)` if there's no PipelineRun record
/// at all — the caller's signal that this run name doesn't exist
/// anywhere, live or archived.
async fn fetch_pg_records(run_name: &str) -> Result<(Value, Vec<Value>), ArchiveError> {
    let client = pg_connect().await?;

    let pr_row = client
        .query_opt(
            "SELECT name, data::text AS data FROM records \
             WHERE type = 'tekton.dev/v1.PipelineRun' \
               AND data->'metadata'->>'name' = $1 \
             ORDER BY created_time DESC LIMIT 1",
            &[&run_name],
        )
        .await
        .map_err(|e| ArchiveError::Db(e.to_string()))?
        .ok_or(ArchiveError::NotFound)?;

    let pr_data_text: String = pr_row.get("data");
    let pr_data: Value = serde_json::from_str(&pr_data_text)
        .map_err(|e| ArchiveError::Db(format!("bad JSON in records.data: {e}")))?;

    let tr_rows = client
        .query(
            "SELECT name, data::text AS data FROM records \
             WHERE type = 'tekton.dev/v1.TaskRun' \
               AND data->'metadata'->'labels'->>'tekton.dev/pipelineRun' = $1 \
             ORDER BY created_time ASC",
            &[&run_name],
        )
        .await
        .map_err(|e| ArchiveError::Db(e.to_string()))?;

    let taskruns = tr_rows
        .into_iter()
        .filter_map(|row| {
            let text: String = row.get("data");
            serde_json::from_str::<Value>(&text).ok()
        })
        .collect();

    Ok((pr_data, taskruns))
}

fn cond_status_reason(obj: &Value) -> (Option<String>, Option<String>) {
    let cond = obj
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first());

    let status = cond
        .and_then(|c| c.get("status"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string());
    let reason = cond
        .and_then(|c| c.get("reason"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string());

    (status, reason)
}

fn steps_from_archived_taskrun(tr_data: &Value) -> Vec<StepMeta> {
    tr_data
        .get("status")
        .and_then(|s| s.get("steps"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|s| {
                    let name = s
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let container = s
                        .get("container")
                        .and_then(|c| c.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("step-{}", name));

                    let (status, reason) = if let Some(t) = s.get("terminated") {
                        let exit_code = t.get("exitCode").and_then(|v| v.as_i64()).unwrap_or(-1);
                        let reason = t.get("reason").and_then(|r| r.as_str()).map(String::from);
                        let status = if exit_code == 0 {
                            RunStatus::Succeeded
                        } else {
                            RunStatus::Failed
                        };
                        (status, reason)
                    } else {
                        // Archived runs are always terminal by definition —
                        // an unterminated step here means it never ran.
                        (RunStatus::Unknown, None)
                    };

                    StepMeta {
                        name,
                        container,
                        status,
                        reason,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Build a complete `RunMeta` for an archived run from Postgres records
/// alone (no logs yet — those stream separately from Loki).
pub async fn fetch_archived_meta(run_name: &str) -> Result<RunMeta, ArchiveError> {
    let (pr_data, tr_data_list) = fetch_pg_records(run_name).await?;

    let pipeline_name = pr_data
        .get("spec")
        .and_then(|s| s.get("pipelineRef"))
        .and_then(|r| r.get("name"))
        .and_then(|n| n.as_str())
        .map(String::from);

    let (pr_status, pr_reason) = cond_status_reason(&pr_data);
    let run_status = RunStatus::from_condition(pr_status.as_deref(), true);

    // `runAfter` lives on the *PipelineRun's* snapshotted pipeline spec
    // (status.pipelineSpec.tasks[].runAfter), not on the TaskRun -- same
    // place the live path (`run_discovery::declared_task_order`) reads it
    // from. tekton-results preserves this whole blob verbatim, so it's
    // available identically for archived runs; build a quick name ->
    // depends_on lookup once, up front, rather than re-scanning per task.
    let depends_on_by_task: std::collections::HashMap<String, Vec<String>> = pr_data
        .get("status")
        .and_then(|s| s.get("pipelineSpec"))
        .and_then(|p| p.get("tasks"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let name = t.get("name")?.as_str()?.to_string();
                    let depends_on = t
                        .get("runAfter")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    Some((name, depends_on))
                })
                .collect()
        })
        .unwrap_or_default();

    let mut tasks = Vec::with_capacity(tr_data_list.len());
    for tr_data in tr_data_list {
        let taskrun_name = tr_data
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("unknown-taskrun")
            .to_string();

        let pipeline_task_name = tr_data
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.get("tekton.dev/pipelineTask"))
            .and_then(|n| n.as_str())
            .map(String::from)
            .unwrap_or_else(|| taskrun_name.clone());

        let task_ref = tr_data
            .get("spec")
            .and_then(|s| s.get("taskRef"))
            .and_then(|r| r.get("name"))
            .and_then(|n| n.as_str())
            .map(String::from);

        let (tr_status, tr_reason) = cond_status_reason(&tr_data);
        let status = RunStatus::from_condition(tr_status.as_deref(), true);
        let steps = steps_from_archived_taskrun(&tr_data);

        let pod_name = tr_data
            .get("status")
            .and_then(|s| s.get("podName"))
            .and_then(|n| n.as_str())
            .map(String::from)
            .or_else(|| Some(format!("{taskrun_name}-pod")));

        let depends_on = depends_on_by_task.get(&pipeline_task_name).cloned().unwrap_or_default();

        tasks.push(TaskMeta {
            name: pipeline_task_name,
            task_ref,
            taskrun_name,
            pod_name,
            status,
            reason: tr_reason,
            steps,
            depends_on,
        });
    }

    Ok(RunMeta {
        run_name: run_name.to_string(),
        source: RunSource::Archive,
        pipeline_name,
        status: run_status,
        reason: pr_reason,
        tasks,
    })
}

// ── Loki ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct LokiResponse {
    data: LokiData,
}

#[derive(Debug, Deserialize)]
struct LokiData {
    result: Vec<LokiStream>,
}

#[derive(Debug, Deserialize)]
struct LokiStream {
    stream: std::collections::HashMap<String, String>,
    values: Vec<(String, String)>,
}

/// Pull every log line Loki has for `run_name`, labeled and time-sorted,
/// as flat `LogLine`s the SSE handler can stream task/step at a time —
/// equivalent to `loki_query()` + `parse_logs()` in query-run.sh, minus
/// the pretty-printing (the FE owns presentation here).
pub async fn fetch_archived_logs(run_name: &str) -> Result<Vec<LogLine>, ArchiveError> {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let start_ns = now_ns.saturating_sub(
        Duration::from_secs((LOKI_LOOKBACK_DAYS * 86_400) as u64).as_nanos(),
    );

    let query = format!(r#"{{pipelinerun="{run_name}"}}"#);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| ArchiveError::Loki(e.to_string()))?;

    let resp = client
        .get(format!("{LOKI_BASE_URL}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.as_str()),
            ("limit", "5000"),
            ("start", start_ns.to_string().as_str()),
            ("end", now_ns.to_string().as_str()),
            ("direction", "forward"),
        ])
        .send()
        .await
        .map_err(|e| ArchiveError::Loki(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(ArchiveError::Loki(format!(
            "loki returned HTTP {}",
            resp.status()
        )));
    }

    let parsed: LokiResponse = resp
        .json()
        .await
        .map_err(|e| ArchiveError::Loki(format!("bad JSON from loki: {e}")))?;

    let mut streams = parsed.data.result;
    // Sort streams by first entry timestamp, same as query-run.sh's
    // `results.sort(key=lambda s: s['values'][0][0] ...)`.
    streams.sort_by(|a, b| {
        let a_ts = a.values.first().map(|(t, _)| t.as_str()).unwrap_or("0");
        let b_ts = b.values.first().map(|(t, _)| t.as_str()).unwrap_or("0");
        a_ts.cmp(b_ts)
    });

    let mut lines = Vec::new();
    for stream in streams {
        // Loki labels carry which task/step this stream belongs to — the
        // pipeline's pod labels (set by Tekton) are forwarded into Loki's
        // label set by the cluster's log shipper.
        let task = stream
            .stream
            .get("task")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let step = stream
            .stream
            .get("container")
            .map(|c| c.trim_start_matches("step-").to_string())
            .unwrap_or_else(|| "unknown".to_string());

        for (ts_ns, raw_line) in stream.values {
            // Strip carriage-return progress spam, same as query-run.sh:
            // `line.split('\r')[-1].strip()`.
            let line = raw_line
                .rsplit('\r')
                .next()
                .unwrap_or(&raw_line)
                .trim()
                .to_string();
            if line.is_empty() {
                continue;
            }
            lines.push(LogLine {
                task: task.clone(),
                step: step.clone(),
                line,
                timestamp: Some(ts_ns),
            });
        }
    }

    Ok(lines)
}