// src/handlers/run_discovery.rs

//! Build a `RunMeta` skeleton (+ ongoing status) by reading the live
//! PipelineRun and its child TaskRuns out of the cluster.
//!
//! This intentionally does NOT assume a fixed pipeline name the way
//! run-pipeline.sh's discovery step does (`PIPELINE_NAME="sample-ci-pipeline"`).
//! Instead it walks `PipelineRun.status.childReferences` (Tekton v1) to find
//! the TaskRuns that belong to *this* run, in whatever order Tekton reports
//! them, then reads each TaskRun's own `status.steps[]` for step-level
//! status. This works for any pipeline, and for runs that are mid-flight,
//! finished, or only partially started.

use serde_json::Value;

use crate::db::k8s_tekton::{
    condition_reason, condition_status, get_pipelinerun, get_taskrun, json_get, json_str,
};
use crate::models::run_stream::{RunMeta, RunSource, RunStatus, StepMeta, TaskMeta};

/// One child reference Tekton attaches to PipelineRun.status as the run
/// progresses — `{pipelineTaskName, name, kind, ...}`.
struct ChildRef {
    pipeline_task_name: String,
    taskrun_name: String,
}

fn child_refs(pr_status: &Value) -> Vec<ChildRef> {
    pr_status
        .get("childReferences")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    let pipeline_task_name = c.get("pipelineTaskName")?.as_str()?.to_string();
                    let taskrun_name = c.get("name")?.as_str()?.to_string();
                    Some(ChildRef {
                        pipeline_task_name,
                        taskrun_name,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Fall back path for pre-childReferences Tekton (status.taskRuns map,
/// deprecated but still present on older clusters/CRDs).
fn legacy_task_runs(pr_status: &Value) -> Vec<ChildRef> {
    pr_status
        .get("taskRuns")
        .and_then(|v| v.as_object())
        .map(|map| {
            map.iter()
                .filter_map(|(taskrun_name, v)| {
                    let pipeline_task_name =
                        v.get("pipelineTaskName")?.as_str()?.to_string();
                    Some(ChildRef {
                        pipeline_task_name,
                        taskrun_name: taskrun_name.clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Pull ordered (pipelineTaskName -> taskRef name) declarations from
/// `status.pipelineSpec.tasks[]`, which Tekton snapshots onto the
/// PipelineRun itself — so we don't need a separate `Pipeline` lookup
/// (and it still works if the Pipeline object was since edited/deleted).
fn declared_task_order(pr_status: &Value) -> Vec<(String, Option<String>, Vec<String>)> {
    pr_status
        .get("pipelineSpec")
        .and_then(|v| v.get("tasks"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let name = t.get("name")?.as_str()?.to_string();
                    let task_ref = t
                        .get("taskRef")
                        .and_then(|r| r.get("name"))
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string());
                    let depends_on = t
                        .get("runAfter")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    Some((name, task_ref, depends_on))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Build the step list for a single TaskRun from its own status, which
/// carries one entry per step with name/container/terminated-or-running
/// state — this is the per-step equivalent of the PipelineRun condition,
/// and is what lets us report "step 2 of 5 done" without inspecting pods.
///
/// `pub` because `handlers::run_event_stream` also calls this directly to
/// re-poll a single step's *current* status right after that step's log
/// stream ends — the snapshot taken once at the start of discovery goes
/// stale the moment the step actually progresses (e.g. it can still say
/// "Pending / PodInitializing" long after the step has finished and its
/// logs have already fully streamed out).
pub fn steps_from_taskrun_status(tr_status: &Value) -> Vec<StepMeta> {
    tr_status
        .get("steps")
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
                        let reason = t
                            .get("reason")
                            .and_then(|r| r.as_str())
                            .map(|s| s.to_string());
                        let status = if exit_code == 0 {
                            RunStatus::Succeeded
                        } else {
                            RunStatus::Failed
                        };
                        (status, reason)
                    } else if s.get("running").is_some() {
                        (RunStatus::Running, None)
                    } else if let Some(w) = s.get("waiting") {
                        let reason = w
                            .get("reason")
                            .and_then(|r| r.as_str())
                            .map(|s| s.to_string());
                        (RunStatus::Pending, reason)
                    } else {
                        (RunStatus::Pending, None)
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

/// Pod name Tekton records on the TaskRun once it schedules one. Falls
/// back to the deterministic `<taskrun>-pod` naming convention used by
/// run-pipeline.sh if status hasn't populated it yet (e.g. just created).
fn pod_name_for_taskrun(tr_status: &Value, taskrun_name: &str) -> String {
    tr_status
        .get("podName")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}-pod", taskrun_name))
}

/// Errors that mean "go look in the archive instead" vs real cluster
/// problems the caller should surface.
pub enum DiscoverError {
    NotFound,
    Kube(kube::Error),
}

impl From<kube::Error> for DiscoverError {
    fn from(e: kube::Error) -> Self {
        DiscoverError::Kube(e)
    }
}

/// Fetch the live PipelineRun + its child TaskRuns and assemble a full
/// `RunMeta`. Returns `Err(DiscoverError::NotFound)` if the PipelineRun
/// itself doesn't exist (the only case that should trigger archive
/// fallback — TaskRuns or pods missing mid-discovery are just "pending").
pub async fn discover_live_run(namespace: &str, run_name: &str) -> Result<RunMeta, DiscoverError> {
    let pr = get_pipelinerun(namespace, run_name)
        .await?
        .ok_or(DiscoverError::NotFound)?;

    let empty = Value::Object(Default::default());
    let pr_status = json_get(&pr, &["status"]).unwrap_or(&empty);

    let pipeline_name = json_str(&pr, &["spec", "pipelineRef", "name"]);
    let pr_cond_status = condition_status(&pr);
    let pr_cond_reason = condition_reason(&pr);
    let pr_started = pr_status.get("startTime").is_some();
    let run_status = RunStatus::from_condition(pr_cond_status.as_deref(), pr_started);

    // Prefer status.childReferences (current Tekton v1); fall back to the
    // deprecated status.taskRuns map for older clusters.
    let mut refs = child_refs(pr_status);
    if refs.is_empty() {
        refs = legacy_task_runs(pr_status);
    }

    // Order tasks per the declared pipeline spec when we have it, so the
    // FE sees a stable left-to-right order even if childReferences arrive
    // out of order. Any child ref not in the declared order (shouldn't
    // normally happen) is appended at the end rather than dropped.
    let declared = declared_task_order(pr_status);
    let mut ordered_names: Vec<String> = declared.iter().map(|(n, _, _)| n.clone()).collect();
    for r in &refs {
        if !ordered_names.contains(&r.pipeline_task_name) {
            ordered_names.push(r.pipeline_task_name.clone());
        }
    }

    let task_ref_lookup = |pipeline_task_name: &str| -> Option<String> {
        declared
            .iter()
            .find(|(n, _, _)| n == pipeline_task_name)
            .and_then(|(_, t, _)| t.clone())
    };

    // Separate lookup (rather than folding into task_ref_lookup's tuple)
    // so a task that's missing from `declared` entirely (shouldn't
    // normally happen -- see the comment above) still gets `vec![]`
    // rather than this needing to thread an Option through every call site.
    let depends_on_lookup = |pipeline_task_name: &str| -> Vec<String> {
        declared
            .iter()
            .find(|(n, _, _)| n == pipeline_task_name)
            .map(|(_, _, d)| d.clone())
            .unwrap_or_default()
    };

    let mut tasks = Vec::with_capacity(ordered_names.len());

    for pipeline_task_name in &ordered_names {
        let task_ref = task_ref_lookup(pipeline_task_name);
        let depends_on = depends_on_lookup(pipeline_task_name);

        let matching_ref = refs.iter().find(|r| &r.pipeline_task_name == pipeline_task_name);

        let Some(cref) = matching_ref else {
            // Declared in the pipeline spec but Tekton hasn't created a
            // TaskRun for it yet (e.g. blocked on a prior task / `when`
            // expression) — report it as pending with no steps yet.
            tasks.push(TaskMeta {
                name: pipeline_task_name.clone(),
                task_ref,
                taskrun_name: String::new(),
                pod_name: None,
                status: RunStatus::Pending,
                reason: None,
                steps: Vec::new(),
                depends_on,
            });
            continue;
        };

        match get_taskrun(namespace, &cref.taskrun_name).await {
            Ok(Some(tr)) => {
                let empty_tr = Value::Object(Default::default());
                let tr_status = json_get(&tr, &["status"]).unwrap_or(&empty_tr);
                let tr_cond_status = condition_status(&tr);
                let tr_cond_reason = condition_reason(&tr);
                let tr_started = tr_status.get("startTime").is_some();
                let status = RunStatus::from_condition(tr_cond_status.as_deref(), tr_started);
                let steps = steps_from_taskrun_status(tr_status);
                let pod_name = pod_name_for_taskrun(tr_status, &cref.taskrun_name);

                tasks.push(TaskMeta {
                    name: pipeline_task_name.clone(),
                    task_ref,
                    taskrun_name: cref.taskrun_name.clone(),
                    pod_name: Some(pod_name),
                    status,
                    reason: tr_cond_reason,
                    steps,
                    depends_on,
                });
            }
            Ok(None) => {
                // TaskRun was referenced but is gone (race / already
                // garbage-collected) — still pending from our perspective.
                tasks.push(TaskMeta {
                    name: pipeline_task_name.clone(),
                    task_ref,
                    taskrun_name: cref.taskrun_name.clone(),
                    pod_name: None,
                    status: RunStatus::Pending,
                    reason: None,
                    steps: Vec::new(),
                    depends_on,
                });
            }
            Err(e) => return Err(DiscoverError::Kube(e)),
        }
    }

    Ok(RunMeta {
        run_name: run_name.to_string(),
        source: RunSource::Tekton,
        pipeline_name,
        status: run_status,
        reason: pr_cond_reason,
        tasks,
    })
}

/// Re-fetch a single step's *current* status from its TaskRun.
///
/// Used by `handlers::run_event_stream` right after a step's log stream
/// ends, instead of trusting the `StepMeta` snapshot captured once at the
/// start of discovery. That snapshot is taken before logs start
/// streaming, so for a step that was "Pending / PodInitializing" at
/// discovery time, naively re-emitting it after streaming finishes would
/// report a step as still pending even though its logs have already
/// fully streamed out and the step has actually completed.
///
/// Returns `None` if the TaskRun or the named step can no longer be found
/// (TaskRun deleted mid-stream, or the step name doesn't match anything
/// in `status.steps[]`) — callers should keep the last-known `StepMeta`
/// in that case rather than treating it as an error.
pub async fn refresh_step_status(
    namespace: &str,
    taskrun_name: &str,
    step_name: &str,
) -> Option<StepMeta> {
    let tr = get_taskrun(namespace, taskrun_name).await.ok()??;
    let empty = Value::Object(Default::default());
    let tr_status = json_get(&tr, &["status"]).unwrap_or(&empty);
    let steps = steps_from_taskrun_status(tr_status);
    steps.into_iter().find(|s| s.name == step_name)
}

/// Re-fetch a single pipeline task by name, for a task that didn't have a
/// TaskRun yet at the moment `discover_live_run` first ran.
///
/// This is the fix for tasks blocked behind an earlier task at the time a
/// client connects (e.g. `test` and `summarize` waiting on `fetch`): the
/// initial `RunMeta` snapshot correctly reports them as `taskrun_name: ""`,
/// `steps: []` — accurate at that instant — but if the caller treats that
/// as permanent and never looks again, it ends up silently skipping the
/// task forever once its TaskRun *does* appear, even though the task goes
/// on to run and finish normally.
///
/// Looks up the PipelineRun fresh, finds the (now-existing) child
/// reference for `pipeline_task_name`, and rebuilds a `TaskMeta` for it the
/// same way `discover_live_run` does for tasks that already had one.
/// Returns `None` if the TaskRun still doesn't exist yet (caller should
/// keep polling) or the PipelineRun itself is gone.
pub async fn refresh_task(
    namespace: &str,
    run_name: &str,
    pipeline_task_name: &str,
) -> Result<Option<TaskMeta>, DiscoverError> {
    let pr = get_pipelinerun(namespace, run_name)
        .await?
        .ok_or(DiscoverError::NotFound)?;

    let empty = Value::Object(Default::default());
    let pr_status = json_get(&pr, &["status"]).unwrap_or(&empty);

    let mut refs = child_refs(pr_status);
    if refs.is_empty() {
        refs = legacy_task_runs(pr_status);
    }

    let declared = declared_task_order(pr_status);
    let task_ref = declared
        .iter()
        .find(|(n, _, _)| n == pipeline_task_name)
        .and_then(|(_, t, _)| t.clone());
    let depends_on = declared
        .iter()
        .find(|(n, _, _)| n == pipeline_task_name)
        .map(|(_, _, d)| d.clone())
        .unwrap_or_default();

    let Some(cref) = refs.iter().find(|r| r.pipeline_task_name == pipeline_task_name) else {
        // Still hasn't been created — not an error, just "not yet".
        return Ok(None);
    };

    match get_taskrun(namespace, &cref.taskrun_name).await? {
        Some(tr) => {
            let empty_tr = Value::Object(Default::default());
            let tr_status = json_get(&tr, &["status"]).unwrap_or(&empty_tr);
            let tr_cond_status = condition_status(&tr);
            let tr_cond_reason = condition_reason(&tr);
            let tr_started = tr_status.get("startTime").is_some();
            let status = RunStatus::from_condition(tr_cond_status.as_deref(), tr_started);
            let steps = steps_from_taskrun_status(tr_status);
            let pod_name = pod_name_for_taskrun(tr_status, &cref.taskrun_name);

            Ok(Some(TaskMeta {
                name: pipeline_task_name.to_string(),
                task_ref,
                taskrun_name: cref.taskrun_name.clone(),
                pod_name: Some(pod_name),
                status,
                reason: tr_cond_reason,
                steps,
                depends_on,
            }))
        }
        None => Ok(None),
    }
}