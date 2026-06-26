// src/models/run_stream.rs

use serde::{Deserialize, Serialize};

/// Where the data for this run is coming from. The FE can use this to decide
/// whether to show a "live" indicator or an "archived" badge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunSource {
    /// Read live from the Tekton/Kubernetes API.
    Tekton,
    /// Run/TaskRun objects are gone from the cluster; reconstructed from
    /// tekton-results (Postgres) + Loki.
    Archive,
}

/// Coarse status used for both PipelineRun and TaskRun/Step level rollups.
/// Mirrors Tekton's Condition semantics: True = succeeded, False = failed,
/// Unknown = still running, Pending = not started yet (our own addition,
/// since Tekton doesn't always give us a clean "not started" condition).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl RunStatus {
    /// Map a Tekton Condition's `status` ("True"/"False"/"Unknown") plus
    /// whether the resource has started yet into our coarse status.
    pub fn from_condition(status: Option<&str>, started: bool) -> Self {
        match status {
            Some("True") => RunStatus::Succeeded,
            Some("False") => RunStatus::Failed,
            Some("Unknown") | Some(_) => {
                if started {
                    RunStatus::Running
                } else {
                    RunStatus::Pending
                }
            }
            None => {
                if started {
                    RunStatus::Unknown
                } else {
                    RunStatus::Pending
                }
            }
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, RunStatus::Succeeded | RunStatus::Failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepMeta {
    pub name: String,
    pub container: String,
    pub status: RunStatus,
    /// Set once we know it, e.g. "Completed", "Error", "ImagePullBackOff".
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    /// The name of this task *within the pipeline* (`pipelineTaskName`),
    /// which is what's used to build the pod/TaskRun name.
    pub name: String,
    /// The underlying Task name (`taskRef.name`), if known.
    pub task_ref: Option<String>,
    pub taskrun_name: String,
    pub pod_name: Option<String>,
    pub status: RunStatus,
    pub reason: Option<String>,
    pub steps: Vec<StepMeta>,
    /// The `pipelineTaskName`s this task's `runAfter` declares it must
    /// wait for, exactly as written in the pipeline spec snapshotted onto
    /// the (live or archived) run. Empty for a task with no explicit
    /// `runAfter` -- which means "no dependency," not "unknown": such a
    /// task is free to start as soon as the pipeline does (modulo any
    /// implicit ordering from `results`/workspace params, which this
    /// field intentionally does not attempt to infer -- only the
    /// explicit `runAfter` list, which is what a flowchart needs to draw
    /// the actual DAG edges).
    pub depends_on: Vec<String>,
}

/// The very first thing sent down the wire: full skeleton of the run with
/// whatever status we already know about. The FE renders this immediately,
/// then fills in logs/status updates as later events arrive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMeta {
    pub run_name: String,
    pub source: RunSource,
    pub pipeline_name: Option<String>,
    pub status: RunStatus,
    pub reason: Option<String>,
    pub tasks: Vec<TaskMeta>,
}

/// A single labeled log line. `task` and `step` let the FE route the line
/// to the right place in the UI without re-parsing pod/container names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogLine {
    pub task: String,
    pub step: String,
    pub line: String,
    /// RFC3339 timestamp if we have one (kubectl --timestamps / Loki ns ts).
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepStatusUpdate {
    pub task: String,
    pub step: String,
    pub status: RunStatus,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusUpdate {
    pub task: String,
    pub status: RunStatus,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDone {
    pub run_name: String,
    pub status: RunStatus,
    pub reason: Option<String>,
    pub duration_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamError {
    pub message: String,
}