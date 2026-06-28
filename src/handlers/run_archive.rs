// src/handlers/run_archive.rs

use std::time::Duration;

use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::Value;

use crate::models::run_stream::{LogLine, RunMeta, RunSource, RunStatus, StepMeta, TaskMeta};

const LOKI_BASE_URL: &str = "http://loki.logging.svc.cluster.local:3100";
const LOKI_LOOKBACK_DAYS: i64 = 31;

// Label key used by the remote-task-controller on proxy TaskRuns.
// No slash — see customrun.rs and run_discovery.rs for the full explanation.
const CUSTOMRUN_LABEL: &str = "remotetask-customrun";

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

// ── TaskRun data model helpers ─────────────────────────────────────────────

/// Returns true if this TaskRun is a proxy created by the
/// remote-task-controller (i.e. it has the `remotetask-customrun` label).
/// Proxy TaskRuns need special treatment in a few places:
///
///  1. They are owned by a CustomRun, not the PipelineRun directly — so
///     tekton-results may not have ingested them under the PipelineRun
///     label at all (see `fetch_pg_records` for the two-path query).
///  2. Their `status.steps[]` entry is named "run" (the runner step) rather
///     than a task-specific name — which is fine; we surface it as-is.
///  3. Their Loki logs are tagged with a `pipelinerun` label that IS the
///     PipelineRun name (because the controller forwarded the
///     `tekton.dev/pipelineRun` label onto them), so the existing Loki
///     query already finds their logs without any special handling.
fn is_proxy_taskrun(tr_data: &Value) -> bool {
    tr_data
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.get(CUSTOMRUN_LABEL))
        .is_some()
}

// Updated fetch_pg_records — also return the run's time window and handles
// proxy TaskRuns that may be stored under a different owner in tekton-results.
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

    // Primary query: TaskRuns labelled directly with the PipelineRun name.
    // This covers both normal TaskRuns AND proxy TaskRuns whose controller
    // forwarded the `tekton.dev/pipelineRun` label (which the controller
    // does as of the current remote-task-controller implementation).
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

    let mut taskruns: Vec<Value> = tr_rows
        .into_iter()
        .filter_map(|row| {
            let text: String = row.get("data");
            serde_json::from_str::<Value>(&text).ok()
        })
        .collect();

    // Secondary query: proxy TaskRuns that are owned by a CustomRun (not the
    // PipelineRun directly). tekton-results watches ownerReferences to decide
    // what to store, so a proxy TaskRun whose ownerRef is a CustomRun may NOT
    // have been ingested under the PipelineRun label — but it WILL appear as
    // a record if tekton-results indexed it at all, and we can find it via
    // the `remotetask-customrun` label.
    //
    // We also need the CustomRun names that are children of this PipelineRun
    // so we can narrow the search. Those names come from the PipelineRun's
    // `status.childReferences` where kind = CustomRun.
    let customrun_names: Vec<String> = pr_data
        .get("status")
        .and_then(|s| s.get("childReferences"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    let kind = c.get("kind")?.as_str()?;
                    if kind != "CustomRun" {
                        return None;
                    }
                    c.get("name")?.as_str().map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    if !customrun_names.is_empty() {
        // Collect names of proxy TaskRuns we already found via the primary
        // query (to avoid duplicates in the fallback).
        let already_found: std::collections::HashSet<String> = taskruns
            .iter()
            .filter_map(|tr| {
                tr.get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        for customrun_name in &customrun_names {
            // The proxy TaskRun is named `<customrun-name>-exec` by convention.
            let proxy_name = format!("{customrun_name}-exec");
            if already_found.contains(&proxy_name) {
                continue;
            }

            // Try by exact name first (cheap).
            let proxy_row = client
                .query_opt(
                    "SELECT data::text AS data FROM records \
                     WHERE type = 'tekton.dev/v1.TaskRun' \
                       AND data->'metadata'->>'name' = $1 \
                     ORDER BY created_time DESC LIMIT 1",
                    &[&proxy_name],
                )
                .await
                .map_err(|e| ArchiveError::Db(e.to_string()))?;

            if let Some(row) = proxy_row {
                let text: String = row.get("data");
                if let Ok(tr_data) = serde_json::from_str::<Value>(&text) {
                    taskruns.push(tr_data);
                    continue;
                }
            }

            // Fallback: search by the remotetask-customrun label (handles any
            // naming deviation from the convention above).
            let label_rows = client
                .query(
                    "SELECT data::text AS data FROM records \
                     WHERE type = 'tekton.dev/v1.TaskRun' \
                       AND data->'metadata'->'labels'->>'remotetask-customrun' = $1 \
                     ORDER BY created_time DESC LIMIT 1",
                    &[&customrun_name],
                )
                .await
                .map_err(|e| ArchiveError::Db(e.to_string()))?;

            for row in label_rows {
                let text: String = row.get("data");
                if let Ok(tr_data) = serde_json::from_str::<Value>(&text) {
                    let name = tr_data
                        .get("metadata")
                        .and_then(|m| m.get("name"))
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string();
                    if !already_found.contains(&name) {
                        taskruns.push(tr_data);
                    }
                }
            }
        }

        // Re-sort by creationTimestamp after potentially adding proxy TaskRuns
        // out of order. Use string comparison — RFC3339 sorts lexicographically.
        taskruns.sort_by(|a, b| {
            let a_ts = a
                .get("metadata")
                .and_then(|m| m.get("creationTimestamp"))
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let b_ts = b
                .get("metadata")
                .and_then(|m| m.get("creationTimestamp"))
                .and_then(|t| t.as_str())
                .unwrap_or("");
            a_ts.cmp(b_ts)
        });
    }

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

    // Build depends_on from the PipelineRun's snapshotted pipelineSpec, same
    // as the live path.
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

    // Build a mapping: CustomRun name → pipelineTaskName, so we can assign
    // proxy TaskRuns to the right pipeline task slot.
    //
    // A proxy TaskRun's `tekton.dev/pipelineTask` label gives us the
    // pipelineTaskName directly (the controller forwards it). For cases where
    // that label is missing we fall back to deriving it from the
    // `remotetask-customrun` label (CustomRun name) + the childReferences map.
    let customrun_to_pipeline_task: std::collections::HashMap<String, String> = pr_data
        .get("status")
        .and_then(|s| s.get("childReferences"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    let kind = c.get("kind")?.as_str()?;
                    if kind != "CustomRun" {
                        return None;
                    }
                    let customrun_name = c.get("name")?.as_str()?.to_string();
                    let pipeline_task = c.get("pipelineTaskName")?.as_str()?.to_string();
                    Some((customrun_name, pipeline_task))
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

        // Determine the pipeline task name. For proxy TaskRuns the controller
        // forwards `tekton.dev/pipelineTask`, so prefer that label. Fall back
        // to the customrun → pipelineTask mapping via the `remotetask-customrun`
        // label, and finally to the taskrun name itself (same as before).
        let pipeline_task_name = tr_data
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.get("tekton.dev/pipelineTask"))
            .and_then(|n| n.as_str())
            .map(String::from)
            .or_else(|| {
                // Try to map via the remotetask-customrun label.
                let customrun_name = tr_data
                    .get("metadata")
                    .and_then(|m| m.get("labels"))
                    .and_then(|l| l.get(CUSTOMRUN_LABEL))
                    .and_then(|n| n.as_str())?;
                customrun_to_pipeline_task.get(customrun_name).cloned()
            })
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
    // The pipelinerun label is set both on normal TaskRun pods AND on
    // proxy TaskRun pods (the controller forwards tekton.dev/pipelineRun).
    // So a single `{pipelinerun="<run_name>"}` Loki query covers all logs.
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
/// pipeline task name. This remapping now handles both normal TaskRuns
/// (whose `task` Loki label is the taskRef name) and proxy TaskRuns
/// (whose `task` Loki label may be the CustomRun / proxy TaskRun name
/// rather than the pipelineTask name).
pub async fn fetch_archived_logs_for_run(
    pool: &Pool,
    run_name: &str,
) -> Result<Vec<LogLine>, ArchiveError> {
    // PG first (fast) to get the time window and TaskRun metadata.
    let (pr_data, tr_data_list, start_ns, end_ns) = fetch_pg_records(pool, run_name).await?;

    let mut lines = fetch_archived_logs_raw(run_name, start_ns, end_ns).await?;

    // Build two remapping tables:
    //
    // 1. taskRef name → pipelineTask name (same as before, for normal TaskRuns).
    // 2. proxy TaskRun name → pipelineTask name (new, for CustomRun children).
    //    Loki's `task` label on proxy TaskRun pods will be the proxy TaskRun's
    //    name (or possibly the CustomRun name), not the pipelineTask name.
    let mut task_ref_to_pipeline_task: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut taskrun_name_to_pipeline_task: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    for tr in &tr_data_list {
        let pipeline_task_name = tr
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.get("tekton.dev/pipelineTask"))
            .and_then(|n| n.as_str())
            .map(String::from);

        let taskrun_name = tr
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(String::from);

        let task_ref_name = tr
            .get("spec")
            .and_then(|s| s.get("taskRef"))
            .and_then(|r| r.get("name"))
            .and_then(|n| n.as_str())
            .map(String::from);

        if let (Some(pipeline_task), Some(task_ref)) = (&pipeline_task_name, task_ref_name) {
            task_ref_to_pipeline_task.insert(task_ref, pipeline_task.clone());
        }

        if is_proxy_taskrun(tr) {
            // For proxy TaskRuns, also map the taskrun name itself so Loki
            // lines tagged with the proxy TaskRun name get remapped.
            if let (Some(pipeline_task), Some(tr_name)) = (pipeline_task_name, taskrun_name) {
                taskrun_name_to_pipeline_task.insert(tr_name, pipeline_task);
            }
        }
    }

    for line in &mut lines {
        // Try taskRef remapping first (normal TaskRuns).
        if let Some(mapped) = task_ref_to_pipeline_task.get(&line.task) {
            line.task = mapped.clone();
        } else if let Some(mapped) = taskrun_name_to_pipeline_task.get(&line.task) {
            // Proxy TaskRun: Loki tagged the line with the proxy TaskRun
            // name instead of a taskRef name.
            line.task = mapped.clone();
        }
    }

    Ok(lines)
}