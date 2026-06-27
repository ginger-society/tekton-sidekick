// src/handlers/run_event_stream.rs

//! The actual SSE event sequencing for `GET /runs/<run_name>/stream`.
//!
//! Protocol (each `Event::json(...).event(name)`):
//!   - `meta`          — full RunMeta skeleton, sent immediately (live or archive)
//!   - `task-status`   — a single task's status changed (TaskStatusUpdate)
//!   - `step-status`   — a single step's status changed (StepStatusUpdate)
//!   - `log`           — one labeled log line (LogLine)
//!   - `error`         — something went wrong but the stream continues (StreamError)
//!   - `done`          — terminal event, stream closes after this (RunDone)
//!
//! Mirrors run-pipeline.sh's structure directly:
//!   1. "Inspecting pipeline" / discovery            → `meta` event
//!   2. for each task: for each step: stream logs    → `log` events
//!   3. wait_for_taskrun_terminal + report ✓/✗        → `task-status` events
//!   4. final PipelineRun status                      → `done` event
//!
//! The difference from the bash script: this also yields full meta+logs
//! immediately for tasks/steps that are *already* complete (so a client
//! that connects after the fact, or reconnects, sees the whole picture
//! rather than only "live tail from now").

use rocket::response::stream::{Event, EventStream};
use rocket::tokio::select;
use rocket::tokio::sync::mpsc;
use rocket::Shutdown;

use crate::db::k8s_tekton::{condition_reason, condition_status, get_pipelinerun};
use crate::handlers::run_archive::{ArchiveError, fetch_archived_logs, fetch_archived_logs_for_run, fetch_archived_meta};
use crate::handlers::run_discovery::{discover_live_run, refresh_step_status, refresh_task, DiscoverError};
use crate::handlers::step_log_stream::{stream_step_logs, wait_for_container_ready, WaitResult};
use crate::models::run_stream::{
    LogLine, RunDone, RunSource, RunStatus, StepStatusUpdate, StreamError, TaskMeta, TaskStatusUpdate,
};

const NAMESPACE: &str = "default";
const TASKRUN_TERMINAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const TASKRUN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Build the SSE stream for a given run name. This owns the entire
/// lifecycle: try live Tekton first, fall back to archive, then either
/// tail live logs or replay archived ones.
///
/// `shutdown` is Rocket's per-connection shutdown signal — it resolves
/// when the client disconnects (or the server is shutting down). Every
/// `select!` against it below means an abandoned SSE connection actually
/// stops polling/streaming server-side instead of running forever, which
/// `EventStream!`'s plain generator loop does not do on its own.
pub fn run_event_stream(run_name: String, mut shutdown: Shutdown) -> EventStream![] {
    EventStream! {
        let start = std::time::Instant::now();

        // ── Step 1: discover — live first, archive on NotFound ──────────
        let meta = match discover_live_run(NAMESPACE, &run_name).await {
            Ok(meta) => meta,
            Err(DiscoverError::NotFound) => {
                match fetch_archived_meta(&run_name).await {
                    Ok(meta) => meta,
                    Err(ArchiveError::NotFound) => {
                        yield Event::json(&StreamError {
                            message: format!(
                                "PipelineRun '{run_name}' was not found live or in the archive"
                            ),
                        }).event("error");
                        return;
                    }
                    Err(e) => {
                        yield Event::json(&StreamError { message: e.to_string() }).event("error");
                        return;
                    }
                }
            }
            Err(DiscoverError::Kube(e)) => {
                yield Event::json(&StreamError {
                    message: format!("error reading PipelineRun from cluster: {e}"),
                }).event("error");
                return;
            }
        };

        // ── Step 2: emit meta immediately, regardless of source ─────────
        let source = meta.source;
        yield Event::json(&meta).event("meta").id("0");

        match source {
            RunSource::Archive => {
                // Already terminal by definition — just replay logs, then done.
                for (i, item) in stream_archived_logs(&run_name).await.into_iter().enumerate() {
                    yield Event::json(&item).event("log").id(format!("log-{i}"));
                }
                yield Event::json(&RunDone {
                    run_name: run_name.clone(),
                    status: meta.status,
                    reason: meta.reason.clone(),
                    duration_seconds: None, // unknown for archived runs without start/end recorded here
                }).event("done");
                return;
            }
            RunSource::Tekton => {
                // Live: spawn one worker per task, all running concurrently,
                // all sending their events into a single shared channel
                // that this loop drains and yields from as they arrive.
                //
                // Why: tasks with no `runAfter` dependency between them run
                // concurrently in the cluster (Tekton schedules their pods
                // at the same time). A single sequential "stream task A
                // fully, then task B" loop doesn't reflect that -- it would
                // block on whichever task happens to be listed first while
                // a sibling task runs to completion unseen, then dump that
                // sibling's logs out all at once once the loop finally
                // reaches it. Each worker here independently waits for its
                // task to start (a task blocked behind a dependency just
                // sits quietly in `poll_for_task_start` until Tekton
                // creates its TaskRun -- which naturally only happens once
                // its dependencies finish, so the DAG's ordering falls out
                // for free without parsing `runAfter` here), streams its
                // own steps, and reports its own final status -- all in
                // true real-time arrival order across tasks.
                let (tx, mut rx) = mpsc::unbounded_channel::<SseItem>();

                for task_snapshot in meta.tasks.clone() {
                    let tx = tx.clone();
                    let shutdown = shutdown.clone();
                    let run_name = run_name.clone();
                    rocket::tokio::spawn(async move {
                        run_task_worker(run_name, task_snapshot, tx, shutdown).await;
                    });
                }
                // Drop our own sender so the channel actually closes once
                // every spawned worker has dropped its clone -- otherwise
                // `rx.recv()` below would wait forever for a sender that's
                // never coming back.
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
                        // Either every worker finished (channel closed) or
                        // the client disconnected -- either way, stop.
                        if tasks_remaining > 0 {
                            return; // disconnected mid-stream
                        }
                        break;
                    };

                    event_id += 1;
                    match item {
                        SseItem::Log(log) => {
                            yield Event::json(&log).event("log").id(event_id.to_string());
                        }
                        SseItem::StepStatus(upd) => {
                            yield Event::json(&upd).event("step-status").id(event_id.to_string());
                        }
                        SseItem::TaskStatus(upd) => {
                            let failed = upd.status == RunStatus::Failed;
                            yield Event::json(&upd).event("task-status").id(event_id.to_string());
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
                }).event("done");
            }
        }
    }
}

/// One item a per-task worker can send up to the main SSE loop. Mirrors
/// the event types in the wire protocol 1:1, plus an internal `TaskDone`
/// marker (not itself an SSE event) so the main loop can track how many
/// workers are still running without needing a separate completion
/// channel per task.
enum SseItem {
    Log(LogLine),
    StepStatus(StepStatusUpdate),
    TaskStatus(TaskStatusUpdate),
    Error(StreamError),
    TaskDone,
}

/// Runs everything for a single pipeline task -- wait for it to start (if
/// it hasn't yet), stream each step's logs, report fresh step status,
/// then report the task's final status -- sending every event into the
/// shared `tx` instead of yielding directly. Several of these run
/// concurrently, one per task in the pipeline, which is what actually
/// gives parallel Tekton tasks parallel streaming here.
///
/// Always sends exactly one `SseItem::TaskDone` before returning,
/// regardless of how it exits (success, failure, timeout, or shutdown),
/// so the main loop's `tasks_remaining` countdown can't get stuck waiting
/// on a worker that silently gave up. The one exception is a client
/// disconnect (`shutdown` resolving): at that point nothing is reading
/// `tx` anymore anyway, so sending or not makes no observable difference,
/// but we still send it for consistency and to let the channel close
/// cleanly.
async fn run_task_worker(
    run_name: String,
    task_snapshot: TaskMeta,
    tx: mpsc::UnboundedSender<SseItem>,
    mut shutdown: Shutdown,
) {
    // BUGFIX (carried over from the sequential version): a task can
    // legitimately have no TaskRun yet at the instant `meta` was
    // captured -- `task_snapshot.taskrun_name` is correctly `""` and
    // `task_snapshot.steps` is correctly `[]` for that snapshot. Poll for
    // it to appear (bounded) rather than treating that as permanent.
    let task: TaskMeta = if task_snapshot.taskrun_name.is_empty() {
        let resolved = select! {
            r = poll_for_task_start(&run_name, &task_snapshot.name) => Some(r),
            _ = &mut shutdown => None,
        };
        match resolved {
            Some(Some(t)) => t,
            Some(None) => {
                // Never started within the timeout -- could mean the
                // pipeline failed upstream, or this task was skipped
                // entirely by a Tekton `when` expression and was never
                // going to get a TaskRun at all. Report it as pending
                // rather than silently vanishing from the stream.
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
                return; // client disconnected while waiting
            }
        }
    } else {
        task_snapshot
    };

    let pod_name = task.pod_name.clone().unwrap_or_else(|| format!("{}-pod", task.taskrun_name));

    for step in &task.steps {
        // If the step is already terminal, we don't need to wait for
        // readiness -- kubectl/log_stream will just return the buffered
        // logs for a finished container.
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
                    return; // client disconnected while waiting
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
                    if tx.send(SseItem::Log(LogLine {
                        task: task.name.clone(),
                        step: step.name.clone(),
                        line,
                        timestamp,
                    })).is_err() {
                        // Main loop is gone (client disconnected) --
                        // no point continuing to read this step's logs.
                        return;
                    }
                }
                None => break, // sender dropped — this step's logs are done
            }
        }

        // BUGFIX (carried over): don't re-emit `step.status` here -- it's
        // a snapshot taken once at the start of discovery, before this
        // step's logs had streamed at all, and is very likely stale by
        // now. Re-poll the TaskRun for this step's *current* status, with
        // a few short retries to absorb the brief lag between "container
        // exited / log stream closed" and Tekton's own status catching up.
        let fresh_step = poll_fresh_step_status(&task.taskrun_name, &step.name)
            .await
            .unwrap_or_else(|| step.clone());

        if tx.send(SseItem::StepStatus(StepStatusUpdate {
            task: task.name.clone(),
            step: step.name.clone(),
            status: fresh_step.status,
            reason: fresh_step.reason.clone(),
        })).is_err() {
            return;
        }
    }

    // Source of truth for "is this task actually done" is the TaskRun
    // condition, not "did the last step's container exit" -- mirrors
    // wait_for_taskrun_terminal in run-pipeline.sh exactly, including the
    // reasoning: later steps in the same task could still be starting up
    // even after the current step's container exits.
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
/// least one step -- used for tasks that were still blocked (no TaskRun
/// yet) at the moment the SSE stream started.
///
/// Two things have to be true before the caller can usefully stream this
/// task: the TaskRun has to exist at all, *and* `status.steps[]` has to be
/// populated (Tekton fills this in shortly after the pod starts -- a
/// TaskRun can exist for a brief window with an empty `steps[]`, which
/// would otherwise make the per-step loop in `run_event_stream` silently
/// do nothing for a task that's actually about to run). We also stop
/// early if the TaskRun reaches a terminal condition with no steps ever
/// reported, since polling forever for an empty list to become non-empty
/// makes no sense once the task itself is done.
///
/// Returns `None` if nothing showed up within `timeout` -- the caller
/// then reports the task as still pending rather than waiting forever
/// (covers e.g. a task skipped entirely by a Tekton `when` expression,
/// which never gets a TaskRun at all).
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
                // TaskRun exists but status.steps[] hasn't populated yet
                // -- keep polling rather than handing back an empty list.
            }
            Ok(None) => {
                // No TaskRun yet at all -- keep polling.
            }
            Err(_) => {
                // Transient cluster read error -- keep polling rather
                // than giving up on the first hiccup.
            }
        }

        if start.elapsed() >= TIMEOUT {
            return None;
        }
        rocket::tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Re-poll a single step's status a few times, short-interval, right
/// after its log stream has ended. The container exiting (which ends the
/// `kubectl logs --follow` stream) and Tekton's `TaskRun.status.steps[]`
/// reflecting that as `terminated` are not perfectly simultaneous — there
/// can be a beat of lag. A handful of quick retries absorbs that without
/// adding a noticeable delay to the stream, and without the false
/// "still pending" report this replaces (see the BUGFIX comment at the
/// call site).
async fn poll_fresh_step_status(taskrun_name: &str, step_name: &str) -> Option<crate::models::run_stream::StepMeta> {
    const MAX_ATTEMPTS: u8 = 5;
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

    for attempt in 0..MAX_ATTEMPTS {
        if let Some(step) = refresh_step_status(NAMESPACE, taskrun_name, step_name).await {
            if step.status.is_terminal() {
                return Some(step);
            }
            // Not terminal yet — on the last attempt, return whatever we
            // have rather than nothing, so the caller's fallback-to-stale
            // path isn't the only option.
            if attempt + 1 == MAX_ATTEMPTS {
                return Some(step);
            }
        }
        rocket::tokio::time::sleep(RETRY_INTERVAL).await;
    }
    None
}

/// Poll the TaskRun's own Condition until terminal, same semantics as
/// run-pipeline.sh's `wait_for_taskrun_terminal`, but via kube instead of
/// `kubectl get -o jsonpath`. Returns Unknown/no-reason on timeout so the
/// stream can keep going rather than hanging forever.
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
            Err(_) => {} // transient — keep polling until timeout
        }

        if start.elapsed() >= TASKRUN_TERMINAL_TIMEOUT {
            return (RunStatus::Unknown, Some("timed out waiting for TaskRun".to_string()));
        }
        rocket::tokio::time::sleep(TASKRUN_POLL_INTERVAL).await;
    }
}

/// Same idea, but for the PipelineRun itself — short timeout since by
/// this point all tasks already reported terminal; we're just waiting
/// for Tekton to roll that up into the PipelineRun's own Condition.
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
            return (RunStatus::Unknown, Some("PipelineRun condition not finalized yet".to_string()));
        }
        rocket::tokio::time::sleep(TASKRUN_POLL_INTERVAL).await;
    }
}

/// Fetch archived logs and sort them into roughly the same task-by-task,
/// step-by-step presentation order the live path streams in, so the FE
/// doesn't need two different rendering paths for live vs. archived.
async fn stream_archived_logs(run_name: &str) -> Vec<LogLine> {
    // Use fetch_archived_logs_for_run instead of fetch_archived_logs
    // so Loki's task-ref-based `task` label is remapped to the
    // pipeline task name that the `meta` event uses.
    match fetch_archived_logs_for_run(run_name).await {
        Ok(mut lines) => {
            lines.sort_by(|a, b| {
                (a.task.as_str(), a.step.as_str(), a.timestamp.as_deref().unwrap_or(""))
                    .cmp(&(b.task.as_str(), b.step.as_str(), b.timestamp.as_deref().unwrap_or("")))
            });
            lines
        }
        Err(_) => Vec::new(),
    }
}