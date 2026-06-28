// src/routes/runs_by_label.rs

use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;
use rocket::response::status;

use deadpool_postgres::Pool;
use rocket_okapi::openapi;

use crate::handlers::label_selector::parse_label_selector;
use crate::handlers::runs_by_label::{fetch_runs_by_label, RunsByLabelError};
use crate::models::run_summary::RunSummary;

const DEFAULT_LIMIT: u32 = 7;
const MAX_LIMIT: u32 = 50;

/// GET /<namespace>/runs-by-label?labels=k1=v1,k2=v2&limit=7
///
/// `labels` takes one or more comma-separated `key=value` pairs, ANDed
/// together — same semantics as a k8s label selector.
///
/// `limit` caps the number of runs returned, most-recent-first. Defaults
/// to 7; values above 50 are clamped to 50. Applied independently to
/// live and archived sources before merging.
#[openapi()]
#[get("/<namespace>/runs-by-label?<labels>&<limit>")]
pub async fn runs_by_label(
    namespace: String,
    labels: String,
    limit: Option<u32>,
    pg_pool: &State<Pool>,
) -> Result<Json<Vec<RunSummary>>, status::Custom<String>> {
    let pairs = parse_label_selector(&labels).map_err(|e| {
        status::Custom(Status::BadRequest, e.to_string())
    })?;

    if pairs.is_empty() {
        return Err(status::Custom(
            Status::BadRequest,
            "labels query param must contain at least one key=value pair".to_string(),
        ));
    }

    let limit = match limit {
        None | Some(0) => DEFAULT_LIMIT,
        Some(n) => n.min(MAX_LIMIT),
    };

    match fetch_runs_by_label(pg_pool.inner(), &namespace, &pairs, limit).await {
        Ok(runs) => Ok(Json(runs)),
        Err(RunsByLabelError::Kube(e)) => Err(status::Custom(
            Status::BadGateway,
            format!("error reading PipelineRuns from cluster: {e}"),
        )),
        Err(RunsByLabelError::Db(e)) => Err(status::Custom(
            Status::BadGateway,
            format!("postgres error: {e}"),
        )),
    }
}