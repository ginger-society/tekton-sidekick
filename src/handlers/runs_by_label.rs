// src/handlers/runs_by_label.rs
//
// Backing handler for GET /<namespace>/runs-by-label?labels=k1=v1,k2=v2
//
// Two sources, queried concurrently:
//   - live PipelineRuns straight from the k8s API (covers runs tekton-results
//     hasn't archived yet — see db::k8s_tekton::list_pipelineruns)
//   - archived PipelineRuns from tekton-results' Postgres `records` table
//     (covers runs already GC'd from the cluster)
//
// Merged and deduped by name, preferring the live copy when a run exists
// in both — same "live wins" precedent as run_event_stream.rs, which
// always tries discover_live_run before falling back to the archive.

use deadpool_postgres::Pool;

use crate::db::k8s_tekton::list_pipelineruns;
use crate::handlers::label_selector::to_k8s_selector;
use crate::models::run_stream::RunSource;
use crate::models::run_summary::RunSummary;

#[derive(Debug)]
pub enum RunsByLabelError {
    Kube(kube::Error),
    Db(String),
}

impl std::fmt::Display for RunsByLabelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunsByLabelError::Kube(e) => write!(f, "error listing PipelineRuns: {e}"),
            RunsByLabelError::Db(e) => write!(f, "postgres error: {e}"),
        }
    }
}

impl From<kube::Error> for RunsByLabelError {
    fn from(e: kube::Error) -> Self {
        RunsByLabelError::Kube(e)
    }
}

// ── Live (k8s) ───────────────────────────────────────────────────────────

async fn live_summaries(
    namespace: &str,
    pairs: &[(String, String)],
) -> Result<Vec<RunSummary>, RunsByLabelError> {
    let selector = to_k8s_selector(pairs);
    let runs = list_pipelineruns(namespace, &selector).await?;

    Ok(runs
        .into_iter()
        .filter_map(|obj| {
            let name = obj.metadata.name.clone()?;
            // k8s_openapi's `Time` wraps `jiff::Timestamp` in this version
            // (it used to wrap `chrono::DateTime`, which is where
            // `.to_rfc3339()` would have come from). `jiff::Timestamp`
            // doesn't have that method — but it doesn't need to: its
            // `Display`/`to_string()` impl already produces RFC3339
            // output directly (e.g. "2024-07-10T21:19:25.567Z"), so
            // `.to_string()` is the correct replacement, not a shim.
            let created_time = obj.metadata.creation_timestamp.as_ref()?.0.to_string();
            Some(RunSummary {
                name,
                created_time,
                source: RunSource::Tekton,
            })
        })
        .collect())
}

// ── Archive (Postgres) ─────────────────────────────────────────────────────

/// Postgres equivalent of the k8s label selector: every pair must be
/// present in `data->'metadata'->'labels'` (AND semantics, same as k8s).
/// Built as one `jsonb @>` containment check per pair rather than a
/// single combined object, so this still works correctly even if a key
/// is repeated in the query string (last one simply also gets ANDed in,
/// rather than silently overwriting an earlier value the way building one
/// merged JSON object would).
async fn archived_summaries(
    pool: &Pool,
    namespace: &str,
    pairs: &[(String, String)],
) -> Result<Vec<RunSummary>, RunsByLabelError> {
    let client = pool
        .get()
        .await
        .map_err(|e| RunsByLabelError::Db(e.to_string()))?;

    // Build "AND data->'metadata'->'labels' @> $2::jsonb AND ... @> $3::jsonb"
    // dynamically, binding each pair as its own single-key JSON object
    // parameter rather than interpolating label text into the SQL string.
    let mut clauses = String::new();
    let mut json_params: Vec<String> = Vec::with_capacity(pairs.len());
    for (i, (k, v)) in pairs.iter().enumerate() {
        let placeholder = i + 2; // $1 is namespace
        clauses.push_str(&format!(
            " AND data->'metadata'->'labels' @> ${placeholder}::jsonb"
        ));
        json_params.push(serde_json::json!({ k: v }).to_string());
    }

    let query = format!(
        "SELECT data->'metadata'->>'name' AS name, \
                data->'metadata'->>'creationTimestamp' AS created_time \
         FROM records \
         WHERE type = 'tekton.dev/v1.PipelineRun' \
           AND data->'metadata'->>'namespace' = $1 \
           {clauses} \
         ORDER BY created_time DESC"
    );

    // tokio-postgres needs a homogeneous param slice of trait objects.
    // `namespace` is already `&str` (implements ToSql directly — no extra
    // `&` needed, that would make it `&&str` and fail to coerce);
    // `json_params` entries are owned `String`s, so we pass `&String`.
    let namespace_owned = namespace.to_string();
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        Vec::with_capacity(1 + json_params.len());
    params.push(&namespace_owned);
    for p in &json_params {
        params.push(p);
    }

    let rows = client
        .query(query.as_str(), &params)
        .await
        .map_err(|e| RunsByLabelError::Db(e.to_string()))?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let name: Option<String> = row.get("name");
            let created_time: Option<String> = row.get("created_time");
            Some(RunSummary {
                name: name?,
                created_time: created_time?,
                source: RunSource::Archive,
            })
        })
        .collect())
}

// ── Merge ────────────────────────────────────────────────────────────────

pub async fn fetch_runs_by_label(
    pool: &Pool,
    namespace: &str,
    pairs: &[(String, String)],
) -> Result<Vec<RunSummary>, RunsByLabelError> {
    // Run both sources concurrently — independent calls, no shared state.
    let (live_res, archived_res) = tokio::join!(
        live_summaries(namespace, pairs),
        archived_summaries(pool, namespace, pairs),
    );

    let live = live_res?;
    // Archive errors are logged but don't fail the whole request — a
    // Postgres hiccup shouldn't hide runs we can already see live. This
    // mirrors run_archive.rs treating Loki/PG issues as soft failures
    // where reasonable, though here we go a step further and not even
    // bubble it up, since live results alone are still a useful answer.
    let archived = match archived_res {
        Ok(v) => v,
        Err(e) => {
            eprintln!("runs-by-label: archive query failed, returning live-only results: {e}");
            Vec::new()
        }
    };

    let mut seen = std::collections::HashSet::with_capacity(live.len());
    let mut merged = Vec::with_capacity(live.len() + archived.len());

    // Live first — "live wins" on dedup, matching run_event_stream.rs's
    // precedent of always trying the live cluster before the archive.
    for r in live {
        seen.insert(r.name.clone());
        merged.push(r);
    }
    for r in archived {
        if seen.insert(r.name.clone()) {
            merged.push(r);
        }
    }

    merged.sort_by(|a, b| b.created_time.cmp(&a.created_time));
    Ok(merged)
}