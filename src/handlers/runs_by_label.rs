// src/handlers/runs_by_label.rs
//
// Backing handler for GET /<namespace>/runs-by-label?labels=k1=v1,k2=v2&limit=7
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
//
// `limit` is applied independently to each source BEFORE merging (not
// fetch-all-then-truncate): the archive query gets a real SQL `LIMIT`,
// and the live side is sorted by created_time desc and truncated
// client-side (k8s `list()` doesn't guarantee recency ordering, so its
// own `.limit()` can't be used for this — see the comment in
// db::k8s_tekton::list_pipelineruns). This is the faster of the two
// possible designs; the tradeoff, as discussed, is that the final merged
// count can end up slightly under `limit` after dedup if the same run
// appears in both the live and archive top-N slices.

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
    limit: u32,
) -> Result<Vec<RunSummary>, RunsByLabelError> {
    let selector = to_k8s_selector(pairs);
    let runs = list_pipelineruns(namespace, &selector).await?;

    let mut summaries: Vec<RunSummary> = runs
        .into_iter()
        .filter_map(|obj| {
            let name = obj.metadata.name.clone()?;
            // k8s_openapi's `Time` wraps `jiff::Timestamp` in this version
            // (it used to wrap `chrono::DateTime`, which is where
            // `.to_rfc3339()` would have come from). `jiff::Timestamp`
            // has no `to_rfc3339` method — but it doesn't need one: its
            // `Display`/`to_string()` impl already produces RFC3339
            // output directly (e.g. "2024-07-10T21:19:25.567Z"), so
            // `.to_string()` is the correct replacement here.
            let created_time = obj.metadata.creation_timestamp.as_ref()?.0.to_string();
            Some(RunSummary {
                name,
                created_time,
                source: RunSource::Tekton,
            })
        })
        .collect();

    // k8s list() order isn't recency-guaranteed (see the comment on
    // list_pipelineruns's safety cap) — sort newest-first ourselves
    // before truncating, so `limit` actually means "most recent N", not
    // "whatever N the API server happened to return first".
    summaries.sort_by(|a, b| b.created_time.cmp(&a.created_time));
    summaries.truncate(limit as usize);

    Ok(summaries)
}

// ── Archive (Postgres) ─────────────────────────────────────────────────────

/// Postgres equivalent of the k8s label selector: every pair must be
/// present in `data->'metadata'->'labels'` (AND semantics, same as k8s).
/// Built as one `jsonb @>` containment check per pair rather than a
/// single combined object, so this still works correctly even if a key
/// is repeated in the query string (last one simply also gets ANDed in,
/// rather than silently overwriting an earlier value the way building one
/// merged JSON object would).
///
/// Unlike the live side, `LIMIT $N` here is a real, order-respecting SQL
/// limit — `ORDER BY created_time DESC LIMIT $N` is applied by Postgres
/// itself before rows are even returned, so no client-side re-sort is
/// needed for this half.
async fn archived_summaries(
    pool: &Pool,
    namespace: &str,
    pairs: &[(String, String)],
    limit: u32,
) -> Result<Vec<RunSummary>, RunsByLabelError> {
    let client = pool
        .get()
        .await
        .map_err(|e| RunsByLabelError::Db(e.to_string()))?;

    // Build "AND data->'metadata'->'labels' @> $2 AND ... @> $3" dynamically,
    // binding each pair as its own single-key JSON object parameter.
    //
    // Two bugs were fixed here versus earlier drafts of this function:
    //
    // 1. Key bug: `serde_json::json!({ k: v })` does NOT interpolate the
    //    variable `k` into the key position — that line was silently
    //    building the JSON object {"k": "<value>"} every time, with the
    //    literal key name "k", regardless of what label key was actually
    //    requested. Fixed by building a `serde_json::Map` explicitly so
    //    the real key is used.
    //
    // 2. Wire-format bug: an earlier fix serialized the JSON object to a
    //    `String` and relied on `${placeholder}::jsonb` to cast it
    //    server-side. When a placeholder sits directly inside a `::jsonb`
    //    cast, Postgres can infer that placeholder's *native* type as
    //    jsonb, but tokio-postgres was sending the `String` value in
    //    `text` wire format — surfaced at runtime as "error serializing
    //    parameter N". Fixed by binding `serde_json::Value` directly (no
    //    pre-stringify, no `::jsonb` cast needed) — tokio-postgres's
    //    `with-serde_json-1` feature (already enabled per db/pg.rs) gives
    //    `Value` a native `ToSql` impl that targets Postgres's json/jsonb
    //    wire format correctly.
    let mut clauses = String::new();
    let mut json_params: Vec<serde_json::Value> = Vec::with_capacity(pairs.len());
    for (i, (k, v)) in pairs.iter().enumerate() {
        let placeholder = i + 2; // $1 is namespace
        clauses.push_str(&format!(" AND data->'metadata'->'labels' @> ${placeholder}"));

        let mut obj = serde_json::Map::new();
        obj.insert(k.clone(), serde_json::Value::String(v.clone()));
        json_params.push(serde_json::Value::Object(obj));
    }

    // `limit` is the next placeholder after namespace ($1) and all the
    // label params ($2..$N) — bound as a typed param (cast to ::int8)
    // rather than interpolated into the query text, consistent with
    // every other value in this query.
    let limit_placeholder = pairs.len() + 2;
    let limit_i64 = limit as i64;

    let query = format!(
        "SELECT data->'metadata'->>'name' AS name, \
                data->'metadata'->>'creationTimestamp' AS created_time \
         FROM records \
         WHERE type = 'tekton.dev/v1.PipelineRun' \
           AND data->'metadata'->>'namespace' = $1::text \
           {clauses} \
         ORDER BY created_time DESC \
         LIMIT ${limit_placeholder}::int8"
    );

    // tokio-postgres needs a homogeneous param slice of trait objects.
    // `namespace` is bound as an owned `String` (`&str` would become
    // `&&str` here and fail to coerce into `&dyn ToSql`); `json_params`
    // entries are owned `serde_json::Value`s, bound by reference the
    // same way; `limit_i64` likewise needs to outlive the call as an
    // owned binding.
    let namespace_owned = namespace.to_string();
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        Vec::with_capacity(2 + json_params.len());
    params.push(&namespace_owned);
    for p in &json_params {
        params.push(p);
    }
    params.push(&limit_i64);

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
    limit: u32,
) -> Result<Vec<RunSummary>, RunsByLabelError> {
    // Run both sources concurrently — independent calls, no shared state.
    // Each gets the same `limit` applied independently (see module-level
    // doc comment for why: cheaper than fetch-all-then-truncate, at the
    // cost of the final merged count possibly landing a bit under `limit`
    // after dedup).
    let (live_res, archived_res) = tokio::join!(
        live_summaries(namespace, pairs, limit),
        archived_summaries(pool, namespace, pairs, limit),
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

    // Final safety truncate: each source was already capped at `limit`
    // independently, so this merged list is at most 2*limit before dedup
    // — dedup usually brings it back near `limit`, but a final truncate
    // here guarantees the response never exceeds what the caller asked
    // for, even in the rare case both sources contributed close to
    // `limit` distinct entries each.
    merged.truncate(limit as usize);

    Ok(merged)
}