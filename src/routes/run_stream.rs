// src/routes/run_stream.rs

use rocket::response::stream::EventStream;
use rocket::Shutdown;

use crate::handlers::run_event_stream::run_event_stream;

/// `GET /runs/<run_name>/stream`
///
/// Immediately returns metadata for every task/step in the run (live from
/// Tekton, or reconstructed from tekton-results + Loki if the run no
/// longer exists in the cluster), then streams labeled log lines and
/// status-change events as the run progresses — or, for an already
/// finished/archived run, replays everything it has.
///
/// Kept outside `openapi_get_routes!` (mounted under "/" via `routes![]`
/// in main.rs) the same way `stream_counter` is: SSE responses aren't
/// representable in OpenAPI/JSON-schema terms, so there's nothing useful
/// for `rocket_okapi` to document here.
#[get("/runs/<run_name>/stream")]
pub fn stream_pipeline_run(run_name: String, shutdown: Shutdown) -> EventStream![] {
    run_event_stream(run_name, shutdown)
}