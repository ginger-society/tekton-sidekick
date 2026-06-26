//! Shared Kubernetes client with automatic 401-recovery.
//!
//! Used by k8_info, k8s_ops, k8s_exec (GUI/TUI/CLI) and also by main.rs
//! (daemon forward loops) so the hook + kubeconfig-reload logic lives in
//! exactly one place.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kube::Client;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tracing::{error, info, warn};

// ── Global state ──────────────────────────────────────────────────────────────

static CLIENT: OnceCell<Arc<Mutex<Client>>> = OnceCell::const_new();
static HOOK_COOLDOWN: OnceCell<Arc<Mutex<Option<Instant>>>> = OnceCell::const_new();

const AUTH_HOOK_COOLDOWN: Duration = Duration::from_secs(180);

// ── Public API ────────────────────────────────────────────────────────────────

/// Returns a clone of the current shared kube client, initialising it on
/// first call.
pub async fn get_client() -> Client {
    let arc = CLIENT
        .get_or_init(|| async {
            let client = Client::try_default()
                .await
                .expect("failed to build initial kube client");
            Arc::new(Mutex::new(client))
        })
        .await;

    arc.lock().await.clone()
}

/// Run the auth-refresh hook (with cooldown) and rebuild the global client
/// from the latest kubeconfig on disk.
///
/// Call this whenever any kube API returns 401.  After it returns, the next
/// `get_client()` call hands out the refreshed client.
pub async fn handle_unauthorized() {
    run_auth_refresh_hook().await;
    rebuild_client().await;
}

/// Like `handle_unauthorized` but also returns the freshly built `Client` so
/// the daemon's forward loop can swap its own `SharedClient` arc without a
/// second lock round-trip.
///
/// Returns `None` if the kubeconfig reload fails.
pub async fn handle_unauthorized_and_get() -> Option<Client> {
    run_auth_refresh_hook().await;
    rebuild_client_inner().await
}

/// Returns `true` if a `kube::Error` is a 401 Unauthorized.
pub fn is_unauthorized(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(ae) if ae.code == 401)
}

// ── Hook runner ───────────────────────────────────────────────────────────────

fn hooks_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
    PathBuf::from(home).join(".ginger-society").join("hooks")
}

pub async fn run_auth_refresh_hook() {
    // CHANGED: every eprintln! in this function is now a tracing:: call.
    // eprintln! writes to raw stderr (fd 2), which goes nowhere useful when
    // launched as a bundled .app (no terminal attached) — it does NOT reach
    // the CappedFileWriter-backed log file that tracing_subscriber writes to
    // in init_logging(). That's almost certainly why hook activity vanished
    // from the log entirely when running from the .app bundle versus from a
    // terminal: it was never a PATH/spawn failure, it was output going to a
    // file descriptor nobody was reading. tracing:: macros go through the
    // same subscriber as every other log line in this codebase, so hook
    // activity will now show up in ~/.ginger-society/logs/ginger-code.log
    // regardless of how the binary was launched.
    let cooldown_arc = HOOK_COOLDOWN
        .get_or_init(|| async { Arc::new(Mutex::new(None)) })
        .await;

    {
        let mut last = cooldown_arc.lock().await;
        let should_run = match *last {
            None    => true,
            Some(t) => t.elapsed() >= AUTH_HOOK_COOLDOWN,
        };
        if !should_run {
            warn!(
                cooldown_secs = AUTH_HOOK_COOLDOWN.as_secs(),
                "auth-refresh hook skipped — within cooldown"
            );
            return;
        }
        *last = Some(Instant::now());
    }

    let hook = hooks_path().join("k8-auth-refresh.sh");
    if !hook.exists() {
        warn!(path = %hook.display(), "auth-refresh hook script not found — skipping");
        return;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let executable = fs::metadata(&hook)
            .map(|m| m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false);
        if !executable {
            warn!(path = %hook.display(), "hook exists but is not executable — skipping");
            return;
        }
    }

    // NEW: log the environment the hook will actually run with. If PATH is
    // ever the real culprit (bundled .app launched without a shell profile),
    // this line tells you immediately rather than requiring guesswork.
    info!(
        path = %hook.display(),
        env_path = %std::env::var("PATH").unwrap_or_default(),
        "running auth-refresh hook"
    );

    let run = tokio::process::Command::new(&hook).output();
    match tokio::time::timeout(Duration::from_secs(30), run).await {
        Ok(Ok(out)) => {
            if !out.stdout.is_empty() {
                info!(
                    stdout = %String::from_utf8_lossy(&out.stdout),
                    "hook stdout"
                );
            }
            if !out.stderr.is_empty() {
                warn!(
                    stderr = %String::from_utf8_lossy(&out.stderr),
                    "hook stderr"
                );
            }
            if out.status.success() {
                info!("auth-refresh hook completed");
            } else {
                error!(
                    exit_code = ?out.status.code(),
                    "hook exited non-zero — continuing anyway"
                );
            }
        }
        Ok(Err(e)) => error!(error = %e, "failed to spawn hook"),
        Err(_)     => error!("hook timed out after 30s"),
    }
}

// ── Client rebuild ────────────────────────────────────────────────────────────

async fn fresh_client() -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let config = kube::Config::from_kubeconfig(
        &kube::config::KubeConfigOptions::default(),
    )
    .await?;
    Ok(Client::try_from(config)?)
}

/// Rebuild and store in the global cell; returns the new client.
async fn rebuild_client_inner() -> Option<Client> {
    info!("rebuilding kube client from latest kubeconfig");

    match fresh_client().await {
        Ok(new_client) => {
            if let Some(arc) = CLIENT.get() {
                *arc.lock().await = new_client.clone();
                info!("client refreshed");
            }
            Some(new_client)
        }
        Err(e) => {
            error!(error = %e, "kubeconfig reload failed");
            None
        }
    }
}

async fn rebuild_client() {
    rebuild_client_inner().await;
}