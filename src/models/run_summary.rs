// src/models/run_summary.rs
//
// Lightweight item returned by GET /<namespace>/runs-by-label. Deliberately
// minimal — just enough to identify a run and order it — since callers
// that want full task/step detail can follow up with the existing
// per-run stream/meta endpoints. Keeping this thin means `runs-by-label`
// stays a single k8s list call + a single pg query, with no per-run
// detail fetches.

use serde::Serialize;
use rocket_okapi::JsonSchema;

use crate::models::run_stream::RunSource;

#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct RunSummary {
    pub name: String,
    /// RFC3339. Normalized to this shape regardless of source — live
    /// PipelineRuns give `metadata.creationTimestamp` as RFC3339 already;
    /// the archive path converts its `timestamptz` to match so callers
    /// never have to branch on `source` to parse this field.
    pub created_time: String,
    pub source: RunSource,
}
 