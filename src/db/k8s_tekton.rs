// src/db/k8s_tekton.rs

//! Tekton CRD access via `DynamicObject`.
//!
//! We don't have `tekton-pipeline` Rust types in this workspace, and pulling
//! in a whole generated CRD crate just for PipelineRun/TaskRun/Task is
//! overkill — `kube`'s `DynamicObject` + `ApiResource::from_gvk` covers what
//! we need (get/list by name, read `.status`/`.spec` as raw `serde_json::Value`).
//!
//! Follows the same `get_client()` / `is_unauthorized()` / `handle_unauthorized()`
//! retry shape used elsewhere in this codebase (see k8s.rs, k8s_exec.rs).

use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use kube::Api;
use serde_json::Value;

use super::k8s_client::{get_client, handle_unauthorized, is_unauthorized};

pub const TEKTON_GROUP: &str = "tekton.dev";
pub const TEKTON_VERSION: &str = "v1";

// We use `from_gvk_with_plural` (not `from_gvk`) deliberately: `from_gvk`
// *guesses* the plural from the kind name, and while PipelineRun/TaskRun
// pluralize regularly, there's no reason to rely on a guesser when the
// real plural is a known constant — avoids a class of bugs if kube-rs ever
// changes its pluralization heuristics.
fn pipelinerun_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(TEKTON_GROUP, TEKTON_VERSION, "PipelineRun"),
        "pipelineruns",
    )
}

fn taskrun_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(TEKTON_GROUP, TEKTON_VERSION, "TaskRun"),
        "taskruns",
    )
}

fn pipeline_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(TEKTON_GROUP, TEKTON_VERSION, "Pipeline"),
        "pipelines",
    )
}

fn task_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(TEKTON_GROUP, TEKTON_VERSION, "Task"),
        "tasks",
    )
}

/// Fetch a PipelineRun by name. Returns `Ok(None)` (not an error) on 404 so
/// callers can cleanly fall through to the archive (Postgres/Loki) path —
/// only real connectivity/auth problems propagate as `Err`.
pub async fn get_pipelinerun(
    namespace: &str,
    name: &str,
) -> Result<Option<DynamicObject>, kube::Error> {
    for attempt in 0..2u8 {
        let client = get_client().await;
        let api: Api<DynamicObject> =
            Api::namespaced_with(client, namespace, &pipelinerun_resource());
        match api.get(name).await {
            Ok(obj) => return Ok(Some(obj)),
            Err(kube::Error::Api(e)) if e.code == 404 => return Ok(None),
            Err(ref e) if is_unauthorized(e) && attempt == 0 => {
                handle_unauthorized().await;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

pub async fn get_taskrun(
    namespace: &str,
    name: &str,
) -> Result<Option<DynamicObject>, kube::Error> {
    for attempt in 0..2u8 {
        let client = get_client().await;
        let api: Api<DynamicObject> = Api::namespaced_with(client, namespace, &taskrun_resource());
        match api.get(name).await {
            Ok(obj) => return Ok(Some(obj)),
            Err(kube::Error::Api(e)) if e.code == 404 => return Ok(None),
            Err(ref e) if is_unauthorized(e) && attempt == 0 => {
                handle_unauthorized().await;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

pub async fn get_pipeline(
    namespace: &str,
    name: &str,
) -> Result<Option<DynamicObject>, kube::Error> {
    for attempt in 0..2u8 {
        let client = get_client().await;
        let api: Api<DynamicObject> = Api::namespaced_with(client, namespace, &pipeline_resource());
        match api.get(name).await {
            Ok(obj) => return Ok(Some(obj)),
            Err(kube::Error::Api(e)) if e.code == 404 => return Ok(None),
            Err(ref e) if is_unauthorized(e) && attempt == 0 => {
                handle_unauthorized().await;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

pub async fn get_task(namespace: &str, name: &str) -> Result<Option<DynamicObject>, kube::Error> {
    for attempt in 0..2u8 {
        let client = get_client().await;
        let api: Api<DynamicObject> = Api::namespaced_with(client, namespace, &task_resource());
        match api.get(name).await {
            Ok(obj) => return Ok(Some(obj)),
            Err(kube::Error::Api(e)) if e.code == 404 => return Ok(None),
            Err(ref e) if is_unauthorized(e) && attempt == 0 => {
                handle_unauthorized().await;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

// ── small JSON-pointer style helpers over DynamicObject.data ─────────────

pub fn json_get<'a>(obj: &'a DynamicObject, path: &[&str]) -> Option<&'a Value> {
    let mut cur = &obj.data;
    for seg in path {
        cur = cur.get(*seg)?;
    }
    Some(cur)
}

pub fn json_str(obj: &DynamicObject, path: &[&str]) -> Option<String> {
    json_get(obj, path)?.as_str().map(|s| s.to_string())
}

/// Read `status.conditions[0].status` / `.reason` — the canonical Tekton
/// "is this thing done yet" Condition, same field the bash scripts poll.
pub fn first_condition<'a>(obj: &'a DynamicObject) -> Option<&'a Value> {
    json_get(obj, &["status", "conditions"])?.as_array()?.first()
}

pub fn condition_status(obj: &DynamicObject) -> Option<String> {
    first_condition(obj)?
        .get("status")?
        .as_str()
        .map(|s| s.to_string())
}

pub fn condition_reason(obj: &DynamicObject) -> Option<String> {
    first_condition(obj)?
        .get("reason")?
        .as_str()
        .map(|s| s.to_string())
}

pub async fn list_pipelineruns(
    namespace: &str,
    label_selector: &str,
) -> Result<Vec<DynamicObject>, kube::Error> {
    let lp = ListParams::default().labels(label_selector);
 
    for attempt in 0..2u8 {
        let client = get_client().await;
        let api: Api<DynamicObject> =
            Api::namespaced_with(client, namespace, &pipelinerun_resource());
        match api.list(&lp).await {
            Ok(list) => return Ok(list.items),
            Err(ref e) if is_unauthorized(e) && attempt == 0 => {
                handle_unauthorized().await;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Vec::new())
}