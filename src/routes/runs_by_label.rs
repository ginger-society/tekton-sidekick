// src/routes/runs_by_label.rs

use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{self, Responder};
use rocket::serde::json::Json;
use rocket::State;

use deadpool_postgres::Pool;
use rocket_okapi::okapi::openapi3::Responses;
use rocket_okapi::response::OpenApiResponderInner;
use rocket_okapi::{openapi, JsonSchema};
use serde::Serialize;

use crate::handlers::label_selector::parse_label_selector;
use crate::handlers::runs_by_label::{fetch_runs_by_label, RunsByLabelError};
use crate::models::run_summary::RunSummary;

/// Typed error body so okapi can describe this route's error responses
/// in the generated spec — a raw `(Status, String)` tuple isn't a type
/// okapi knows how to document, so this replaces that pattern with a
/// proper schema'd struct + a hand-written `Responder`/`OpenApiResponderInner`
/// pair.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ErrorBody {
    pub message: String,
}

/// Wraps an `ErrorBody` with the HTTP status it should be returned as.
/// Kept separate from `ErrorBody` itself so the JSON body never contains
/// a `status` field (that belongs in the HTTP response line, not the
/// payload) while still letting each error case pick its own status.
pub struct ApiError {
    pub status: Status,
    pub body: ErrorBody,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        ApiError {
            status: Status::BadRequest,
            body: ErrorBody {
                message: message.into(),
            },
        }
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        ApiError {
            status: Status::BadGateway,
            body: ErrorBody {
                message: message.into(),
            },
        }
    }
}

impl<'r> Responder<'r, 'static> for ApiError {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        let status = self.status;
        let mut res = Json(self.body).respond_to(req)?;
        res.set_status(status);
        Ok(res)
    }
}

/// Tells okapi this responder can return any status — we don't pin it to
/// one fixed code in the spec since `ApiError` covers both 400 (bad
/// label selector) and 502 (upstream k8s/postgres failure). Callers
/// reading the generated spec see `ErrorBody`'s schema either way.
impl OpenApiResponderInner for ApiError {
    fn responses(
        gen: &mut rocket_okapi::gen::OpenApiGenerator,
    ) -> rocket_okapi::Result<Responses> {
        // Reuse Json<ErrorBody>'s own OpenApiResponderInner impl to get a
        // correctly-shaped schema entry, rather than hand-building the
        // okapi Response/MediaType structures ourselves.
        <Json<ErrorBody> as rocket_okapi::response::OpenApiResponderInner>::responses(gen)
    }
}

/// GET /<namespace>/runs-by-label?labels=k1=v1,k2=v2
///
/// `labels` takes one or more comma-separated `key=value` pairs, ANDed
/// together (a run must match every pair to be returned) — same
/// semantics as a k8s label selector. Keys may contain `/` (e.g.
/// `ginger-gitter/branch=main`); since this is a query param rather than
/// a path segment, no escaping is needed for that.
///
/// Queries live PipelineRuns (k8s API) and archived ones (tekton-results
/// Postgres) concurrently, merges, and dedupes by name (live wins on
/// conflict). Returns a lightweight `RunSummary` per match — name +
/// created_time only, no task/step detail — since this is meant as a
/// fast discovery/listing call, not a full run-detail fetch.
#[openapi(tag = "runs")]
#[get("/<namespace>/runs-by-label?<labels>")]
pub async fn runs_by_label(
    namespace: String,
    labels: String,
    pg_pool: &State<Pool>,
) -> Result<Json<Vec<RunSummary>>, ApiError> {
    let pairs = parse_label_selector(&labels).map_err(|e| ApiError::bad_request(e.to_string()))?;

    if pairs.is_empty() {
        return Err(ApiError::bad_request(
            "labels query param must contain at least one key=value pair",
        ));
    }

    match fetch_runs_by_label(pg_pool.inner(), &namespace, &pairs).await {
        Ok(runs) => Ok(Json(runs)),
        Err(RunsByLabelError::Kube(e)) => Err(ApiError::bad_gateway(format!(
            "error reading PipelineRuns from cluster: {e}"
        ))),
        Err(RunsByLabelError::Db(e)) => {
            Err(ApiError::bad_gateway(format!("postgres error: {e}")))
        }
    }
}