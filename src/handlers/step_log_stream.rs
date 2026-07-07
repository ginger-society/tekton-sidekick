// src/handlers/step_log_stream.rs

//! Per-step log streaming for a Tekton TaskRun's pod.
//!
//! This is the Rust equivalent of `stream_container()` in run-pipeline.sh:
//! it retries until the pod exists AND the named container has left the
//! "waiting" state (an empty/absent containerStatuses entry right after
//! pod creation must be retried, not treated as ready), then follows logs
//! until the container exits. Built on the same `Api<Pod>` + `LogParams`
//! + `log_stream().compat()` pattern already used in db/k8s.rs.

use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::Api;
use tokio::io::AsyncBufReadExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::db::k8s_client::{get_client, handle_unauthorized, is_unauthorized};

const MAX_READY_RETRIES: u32 = 60;
const READY_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug)]
pub enum WaitResult {
    Ready,
    /// Pod or container never showed up in time — caller should report
    /// this as a step-level problem but keep going (mirrors the bash
    /// script's `[timeout]` yellow warning, not a hard pipeline failure).
    AlreadyTerminated,
    TimedOut,
}

/// Block until `pod_name`'s `container` is running or terminated (i.e.
/// logs are guaranteed to be readable), or until we give up.
pub async fn wait_for_container_ready(namespace: &str, pod_name: &str, container: &str) -> WaitResult {
    let mut attempt = 0u32;

    loop {
        let client = get_client().await;
        let api: Api<Pod> = Api::namespaced(client, namespace);

        match api.get(pod_name).await {
            Ok(pod) => {
                let state = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.container_statuses.as_ref())
                    .and_then(|statuses| statuses.iter().find(|c| c.name == container))
                    .and_then(|c| c.state.as_ref());

                if let Some(s) = state {
                    if s.terminated.is_some() {
                        return WaitResult::AlreadyTerminated;
                    }
                    if s.running.is_some() {
                        return WaitResult::Ready;
                    }
                    // still `waiting` — fall through and keep polling
                }
            }
            Err(ref e) if is_unauthorized(e) => {
                handle_unauthorized().await;
                continue;
            }
            Err(_) => {}
        }

        attempt += 1;
        if attempt >= MAX_READY_RETRIES {
            return WaitResult::TimedOut;
        }
        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
}

/// Stream one step container's logs line-by-line into `line_tx`, with
/// timestamps requested from the API server (matches `kubectl logs
/// --timestamps`). Returns once the container exits or the stream ends;
/// errors are sent down the channel as a line rather than propagated, so
/// one bad container doesn't take down the whole SSE response.
pub async fn stream_step_logs(
    namespace: &str,
    pod_name: &str,
    container: &str,
    already_terminated: bool,
    line_tx: tokio::sync::mpsc::UnboundedSender<(Option<String>, String)>,
) {
    let client = get_client().await;
    let api: Api<Pod> = Api::namespaced(client, namespace);

    let params = LogParams {
        follow: !already_terminated,
        timestamps: true,
        container: Some(container.to_string()),
        ..Default::default()
    };

    let stream = match api.log_stream(pod_name, &params).await {
        Ok(s) => s,
        Err(ref e) if is_unauthorized(e) => {
            handle_unauthorized().await;
            let _ = line_tx.send((None, "log stream: refreshing credentials, retrying…".into()));
            return;
        }
        Err(e) => {
            let _ = line_tx.send((None, format!("log stream error: {}", e)));
            return;
        }
    };

    let reader = tokio::io::BufReader::new(stream.compat());
    let mut lines = reader.lines();

    loop {
        match lines.next_line().await {
            Ok(Some(raw_line)) => {
                let (timestamp, line) = split_timestamp(&raw_line);
                if line_tx.send((timestamp, line)).is_err() {
                    return; // receiver dropped — SSE client disconnected
                }
            }
            Ok(None) => return,
            Err(e) => {
                let _ = line_tx.send((None, format!("log stream error: {}", e)));
                return;
            }
        }
    }
}

/// `kubectl logs --timestamps` / the raw API stream prefixes each line
/// with an RFC3339 timestamp + a space. Split it back out so the FE gets
/// a clean `{timestamp, line}` pair instead of re-parsing text.
fn split_timestamp(raw: &str) -> (Option<String>, String) {
    if let Some(idx) = raw.find(' ') {
        let (ts, rest) = raw.split_at(idx);
        if ts.len() >= 20 && ts.contains('T') && (ts.ends_with('Z') || ts.contains('+')) {
            return (Some(ts.to_string()), rest.trim_start().to_string());
        }
    }
    (None, raw.to_string())
}