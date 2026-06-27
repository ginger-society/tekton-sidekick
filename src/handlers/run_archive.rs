// src/handlers/run_archive.rs

use std::time::Duration;

use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::Value;

use crate::models::run_stream::{LogLine, RunMeta, RunSource, RunStatus, StepMeta, TaskMeta};

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

// ── Postgres ───────────────────────────────────────────────────────────────

/// Borrow a connection from the pool — no TCP handshake, no auth round
/// trip; the pool keeps connections warm between requests.
async fn pg_conn(pool: &Pool) -> Result<deadpool_postgres::Client, ArchiveError> {
    pool.get().await.map_err(|e| ArchiveError::Db(e.to_string()))
}

// Updated fetch_pg_records — also return the run's time window
async fn fetch_pg_records(
    pool: &Pool,
    run_name: &str,
) -> Result<(Value, Vec<Value>, u128, u128), ArchiveError> {
    let client = pg_conn(pool).await?;

    let pr_row = client
        .query_opt(
            "SELECT name, data::text AS data, created_time, updated_time FROM records \
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

    // Extract the run's actual time window from Postgres so Loki doesn't
    // have to scan 31 days. Add a 5-minute buffer on each side to absorb
    // clock skew between the cluster and Loki's ingestion timestamp.
    let created_time: std::time::SystemTime = pr_row.get("created_time");
    let updated_time: std::time::SystemTime = pr_row.get("updated_time");

    let buffer = std::time::Duration::from_secs(300); // 5 min each side
    let start_ns = created_time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .saturating_sub(buffer)
        .as_nanos();
    let end_ns = updated_time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .checked_add(buffer)
        .unwrap_or_else(|| updated_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default())
        .as_nanos();

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

    Ok((pr_data, taskruns, start_ns, end_ns))
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

pub async fn fetch_archived_meta(pool: &Pool, run_name: &str) -> Result<RunMeta, ArchiveError> {
    let (pr_data, tr_data_list, _, _) = fetch_pg_records(pool, run_name).await?;

    let pipeline_name = pr_data
        .get("spec")
        .and_then(|s| s.get("pipelineRef"))
        .and_then(|r| r.get("name"))
        .and_then(|n| n.as_str())
        .map(String::from);

    let (pr_status, pr_reason) = cond_status_reason(&pr_data);
    let run_status = RunStatus::from_condition(pr_status.as_deref(), true);

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

        let depends_on = depends_on_by_task
            .get(&pipeline_task_name)
            .cloned()
            .unwrap_or_default();

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

// ── Loki ───────────────────────────────────────────────────────────────────

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

// Updated fetch_archived_logs_raw — accepts explicit time window
async fn fetch_archived_logs_raw(
    run_name: &str,
    start_ns: u128,
    end_ns: u128,
) -> Result<Vec<LogLine>, ArchiveError> {
    let query = format!(r#"{{pipelinerun="{run_name}"}}"#);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| ArchiveError::Loki(e.to_string()))?;

    let resp = client
        .get(format!("{LOKI_BASE_URL}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.as_str()),
            ("limit", "5000"),
            ("start", start_ns.to_string().as_str()),
            ("end", end_ns.to_string().as_str()),
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
    streams.sort_by(|a, b| {
        let a_ts = a.values.first().map(|(t, _)| t.as_str()).unwrap_or("0");
        let b_ts = b.values.first().map(|(t, _)| t.as_str()).unwrap_or("0");
        a_ts.cmp(b_ts)
    });

    let mut lines = Vec::new();
    for stream in streams {
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

/// Fetch logs + remap Loki's task-ref-based `task` label back to the
/// pipeline task name, running the PG and Loki queries concurrently.
// Updated fetch_archived_logs_for_run — threads the time window through
pub async fn fetch_archived_logs_for_run(
    pool: &Pool,
    run_name: &str,
) -> Result<Vec<LogLine>, ArchiveError> {
    // PG first (fast) to get the time window, then fire Loki with it.
    // We can't fully parallelize here since Loki needs the window from PG,
    // but PG is a pooled in-cluster call so it's cheap (< 50ms typically).
    let (_, tr_data_list, start_ns, end_ns) = fetch_pg_records(pool, run_name).await?;

    let mut lines = fetch_archived_logs_raw(run_name, start_ns, end_ns).await?;

    let task_ref_to_pipeline_task: std::collections::HashMap<String, String> = tr_data_list
        .iter()
        .filter_map(|tr| {
            let pipeline_task_name = tr
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.get("tekton.dev/pipelineTask"))
                .and_then(|n| n.as_str())
                .map(String::from)?;
            let task_ref_name = tr
                .get("spec")
                .and_then(|s| s.get("taskRef"))
                .and_then(|r| r.get("name"))
                .and_then(|n| n.as_str())
                .map(String::from)?;
            Some((task_ref_name, pipeline_task_name))
        })
        .collect();

    for line in &mut lines {
        if let Some(mapped) = task_ref_to_pipeline_task.get(&line.task) {
            line.task = mapped.clone();
        }
    }

    Ok(lines)
}