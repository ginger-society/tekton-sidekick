// src/handlers/run_event_stream.rs

use deadpool_postgres::Pool;
use rocket::response::stream::{Event, EventStream};
use rocket::tokio::select;
use rocket::tokio::sync::mpsc;
use rocket::Shutdown;

use crate::db::k8s_tekton::{condition_reason, condition_status, get_pipelinerun};
use crate::handlers::run_archive::{fetch_archived_logs_for_run, fetch_archived_meta, ArchiveError};
use crate::handlers::run_discovery::{discover_live_run, refresh_step_status, refresh_task, DiscoverError};
use crate::handlers::step_log_stream::{stream_step_logs, wait_for_container_ready, WaitResult};
use crate::models::run_stream::{
    LogLine, RunDone, RunSource, RunStatus, StepStatusUpdate, StreamError, TaskMeta,
    TaskStatusUpdate,
};

const NAMESPACE: &str = "default";
const TASKRUN_TERMINAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const TASKRUN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Pool is passed in from the Rocket route handler via `&rocket::State<Pool>`.
pub fn run_event_stream(
    run_name: String,
    mut shutdown: Shutdown,
    pool: Pool,   // ← cloned from State<Pool> before entering EventStream!
) -> EventStream![] {
    EventStream! {
        let start = std::time::Instant::now();

        // ── Step 1: discover — live first, archive on NotFound ──────────
        let meta = match discover_live_run(NAMESPACE, &run_name).await {
            Ok(meta) => meta,
            Err(DiscoverError::NotFound) => {
                match fetch_archived_meta(&pool, &run_name).await {
                    Ok(meta) => meta,
                    Err(ArchiveError::NotFound) => {
                        yield Event::json(&StreamError {
                            message: format!(
                                "PipelineRun '{run_name}' was not found live or in the archive"
                            ),
                        })
                        .event("error");
                        return;
                    }
                    Err(e) => {
                        yield Event::json(&StreamError {
                            message: e.to_string(),
                        })
                        .event("error");
                        return;
                    }
                }
            }
            Err(DiscoverError::Kube(e)) => {
                yield Event::json(&StreamError {
                    message: format!("error reading PipelineRun from cluster: {e}"),
                })
                .event("error");
                return;
            }
        };

        // ── Step 2: emit meta immediately ───────────────────────────────
        let source = meta.source;
        yield Event::json(&meta).event("meta").id("0");

        match source {
            RunSource::Archive => {
                // Spawn log fetch in background so the meta event above is
                // already flushed to the client while we're still querying.
                let (log_tx, mut log_rx) = mpsc::unbounded_channel::<LogLine>();
                let run_name_clone = run_name.clone();
                let pool_clone = pool.clone();

                rocket::tokio::spawn(async move {
                    match fetch_archived_logs_for_run(&pool_clone, &run_name_clone).await {
                        Ok(mut lines) => {
                            lines.sort_by(|a, b| {
                                a.timestamp
                                    .as_deref()
                                    .unwrap_or("0")
                                    .cmp(b.timestamp.as_deref().unwrap_or("0"))
                            });
                            for line in lines {
                                if log_tx.send(line).is_err() {
                                    break; // client disconnected
                                }
                            }
                        }
                        Err(e) => {
                            // Log the error server-side; the done event
                            // below still closes the stream cleanly.
                            eprintln!("archive log fetch error: {e}");
                        }
                    }
                });

                let mut i = 0usize;
                loop {
                    let next = select! {
                        line = log_rx.recv() => line,
                        _ = &mut shutdown => None,
                    };
                    let Some(line) = next else { break; };
                    yield Event::json(&line).event("log").id(format!("log-{i}"));
                    i += 1;
                }

                yield Event::json(&RunDone {
                    run_name: run_name.clone(),
                    status: meta.status,
                    reason: meta.reason.clone(),
                    duration_seconds: None,
                })
                .event("done");
            }

            RunSource::Tekton => {
                // ── unchanged from original ──────────────────────────────
                let (tx, mut rx) = mpsc::unbounded_channel::<SseItem>();

                for task_snapshot in meta.tasks.clone() {
                    let tx = tx.clone();
                    let shutdown = shutdown.clone();
                    let run_name = run_name.clone();
                    rocket::tokio::spawn(async move {
                        run_task_worker(run_name, task_snapshot, tx, shutdown).await;
                    });
                }
                drop(tx);

                let mut event_id: u64 = 1;
                let mut any_task_failed = false;
                let mut tasks_remaining = meta.tasks.len();

                loop {
                    if tasks_remaining == 0 {
                        break;
                    }
                    let next = select! {
                        item = rx.recv() => item,
                        _ = &mut shutdown => None,
                    };
                    let Some(item) = next else {
                        if tasks_remaining > 0 {
                            return;
                        }
                        break;
                    };

                    event_id += 1;
                    match item {
                        SseItem::Log(log) => {
                            yield Event::json(&log).event("log").id(event_id.to_string());
                        }
                        SseItem::StepStatus(upd) => {
                            yield Event::json(&upd)
                                .event("step-status")
                                .id(event_id.to_string());
                        }
                        SseItem::TaskStatus(upd) => {
                            let failed = upd.status == RunStatus::Failed;
                            yield Event::json(&upd)
                                .event("task-status")
                                .id(event_id.to_string());
                            if failed {
                                any_task_failed = true;
                            }
                        }
                        SseItem::Error(err) => {
                            yield Event::json(&err).event("error").id(event_id.to_string());
                        }
                        SseItem::TaskDone => {
                            tasks_remaining -= 1;
                        }
                    }
                }

                let (run_status, run_reason) = if any_task_failed {
                    (RunStatus::Failed, None)
                } else {
                    let terminal = select! {
                        r = wait_for_pipelinerun_terminal_status(&run_name) => Some(r),
                        _ = &mut shutdown => None,
                    };
                    let Some(result) = terminal else { return };
                    result
                };

                yield Event::json(&RunDone {
                    run_name: run_name.clone(),
                    status: run_status,
                    reason: run_reason,
                    duration_seconds: Some(start.elapsed().as_secs() as i64),
                })
                .event("done");
            }
        }
    }
}

// ── All helpers below are unchanged from original ──────────────────────────

enum SseItem {
    Log(LogLine),
    StepStatus(StepStatusUpdate),
    TaskStatus(TaskStatusUpdate),
    Error(StreamError),
    TaskDone,
}

async fn run_task_worker(
    run_name: String,
    task_snapshot: TaskMeta,
    tx: mpsc::UnboundedSender<SseItem>,
    mut shutdown: Shutdown,
) {
    let task: TaskMeta = if task_snapshot.taskrun_name.is_empty() {
        let resolved = select! {
            r = poll_for_task_start(&run_name, &task_snapshot.name) => Some(r),
            _ = &mut shutdown => None,
        };
        match resolved {
            Some(Some(t)) => t,
            Some(None) => {
                let _ = tx.send(SseItem::TaskStatus(TaskStatusUpdate {
                    task: task_snapshot.name.clone(),
                    status: RunStatus::Pending,
                    reason: Some("task never started".to_string()),
                }));
                let _ = tx.send(SseItem::TaskDone);
                return;
            }
            None => {
                let _ = tx.send(SseItem::TaskDone);
                return;
            }
        }
    } else {
        task_snapshot
    };

    let pod_name = task
        .pod_name
        .clone()
        .unwrap_or_else(|| format!("{}-pod", task.taskrun_name));

    for step in &task.steps {
        if matches!(step.status, RunStatus::Pending) {
            let ready = select! {
                r = wait_for_container_ready(&pod_name, &step.container) => Some(r),
                _ = &mut shutdown => None,
            };
            match ready {
                Some(WaitResult::Ready) => {}
                Some(WaitResult::TimedOut) => {
                    let _ = tx.send(SseItem::Error(StreamError {
                        message: format!(
                            "timed out waiting for {}/{} to start",
                            task.name, step.name
                        ),
                    }));
                    continue;
                }
                None => {
                    let _ = tx.send(SseItem::TaskDone);
                    return;
                }
            }
        }

        let (log_tx, mut log_rx) = mpsc::unbounded_channel::<(Option<String>, String)>();
        let pod_name_cloned = pod_name.clone();
        let container_cloned = step.container.clone();
        rocket::tokio::spawn(async move {
            stream_step_logs(&pod_name_cloned, &container_cloned, log_tx).await;
        });

        loop {
            let next = select! {
                line = log_rx.recv() => line,
                _ = &mut shutdown => break,
            };
            match next {
                Some((timestamp, line)) => {
                    if tx
                        .send(SseItem::Log(LogLine {
                            task: task.name.clone(),
                            step: step.name.clone(),
                            line,
                            timestamp,
                        }))
                        .is_err()
                    {
                        return;
                    }
                }
                None => break,
            }
        }

        let fresh_step = poll_fresh_step_status(&task.taskrun_name, &step.name)
            .await
            .unwrap_or_else(|| step.clone());

        if tx
            .send(SseItem::StepStatus(StepStatusUpdate {
                task: task.name.clone(),
                step: step.name.clone(),
                status: fresh_step.status,
                reason: fresh_step.reason.clone(),
            }))
            .is_err()
        {
            return;
        }
    }

    let terminal = select! {
        r = wait_for_taskrun_terminal_status(&task.taskrun_name) => Some(r),
        _ = &mut shutdown => None,
    };
    let Some((final_status, final_reason)) = terminal else {
        let _ = tx.send(SseItem::TaskDone);
        return;
    };

    let _ = tx.send(SseItem::TaskStatus(TaskStatusUpdate {
        task: task.name.clone(),
        status: final_status,
        reason: final_reason,
    }));
    let _ = tx.send(SseItem::TaskDone);
}

async fn poll_for_task_start(run_name: &str, pipeline_task_name: &str) -> Option<TaskMeta> {
    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

    let start = std::time::Instant::now();
    loop {
        match refresh_task(NAMESPACE, run_name, pipeline_task_name).await {
            Ok(Some(task)) => {
                if !task.steps.is_empty() || task.status.is_terminal() {
                    return Some(task);
                }
            }
            Ok(None) => {}
            Err(_) => {}
        }
        if start.elapsed() >= TIMEOUT {
            return None;
        }
        rocket::tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn poll_fresh_step_status(
    taskrun_name: &str,
    step_name: &str,
) -> Option<crate::models::run_stream::StepMeta> {
    const MAX_ATTEMPTS: u8 = 5;
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

    for attempt in 0..MAX_ATTEMPTS {
        if let Some(step) = refresh_step_status(NAMESPACE, taskrun_name, step_name).await {
            if step.status.is_terminal() {
                return Some(step);
            }
            if attempt + 1 == MAX_ATTEMPTS {
                return Some(step);
            }
        }
        rocket::tokio::time::sleep(RETRY_INTERVAL).await;
    }
    None
}

async fn wait_for_taskrun_terminal_status(taskrun_name: &str) -> (RunStatus, Option<String>) {
    let start = std::time::Instant::now();
    loop {
        match crate::db::k8s_tekton::get_taskrun(NAMESPACE, taskrun_name).await {
            Ok(Some(tr)) => {
                let status = condition_status(&tr);
                let reason = condition_reason(&tr);
                match status.as_deref() {
                    Some("True") => return (RunStatus::Succeeded, reason),
                    Some("False") => return (RunStatus::Failed, reason),
                    _ => {}
                }
            }
            Ok(None) => return (RunStatus::Unknown, Some("TaskRun disappeared".to_string())),
            Err(_) => {}
        }
        if start.elapsed() >= TASKRUN_TERMINAL_TIMEOUT {
            return (
                RunStatus::Unknown,
                Some("timed out waiting for TaskRun".to_string()),
            );
        }
        rocket::tokio::time::sleep(TASKRUN_POLL_INTERVAL).await;
    }
}

async fn wait_for_pipelinerun_terminal_status(run_name: &str) -> (RunStatus, Option<String>) {
    let start = std::time::Instant::now();
    let short_timeout = std::time::Duration::from_secs(30);
    loop {
        match get_pipelinerun(NAMESPACE, run_name).await {
            Ok(Some(pr)) => {
                let status = condition_status(&pr);
                let reason = condition_reason(&pr);
                match status.as_deref() {
                    Some("True") => return (RunStatus::Succeeded, reason),
                    Some("False") => return (RunStatus::Failed, reason),
                    _ => {}
                }
            }
            Ok(None) => return (RunStatus::Unknown, None),
            Err(_) => {}
        }
        if start.elapsed() >= short_timeout {
            return (
                RunStatus::Unknown,
                Some("PipelineRun condition not finalized yet".to_string()),
            );
        }
        rocket::tokio::time::sleep(TASKRUN_POLL_INTERVAL).await;
    }
}