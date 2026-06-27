// src/routes/run_stream.rs

use deadpool_postgres::Pool;
use rocket::response::stream::EventStream;
use rocket::Shutdown;
use rocket::State;

use crate::handlers::run_event_stream::run_event_stream;

#[get("/runs/<namespace>/<run_name>/stream")]
pub fn stream_pipeline_run(
    namespace: String,
    run_name: String,
    shutdown: Shutdown,
    pg_pool: &State<Pool>,
) -> EventStream![] {
    run_event_stream(namespace, run_name, shutdown, pg_pool.inner().clone())
}