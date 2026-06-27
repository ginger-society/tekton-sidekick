// src/routes/run_stream.rs

use deadpool_postgres::Pool;
use rocket::response::stream::EventStream;
use rocket::Shutdown;
use rocket::State;

use crate::handlers::run_event_stream::run_event_stream;

#[get("/runs/<run_name>/stream")]
pub fn stream_pipeline_run(
    run_name: String,
    shutdown: Shutdown,
    pg_pool: &State<Pool>,
) -> EventStream![] {
    // Clone the pool (cheap — it's Arc-backed) before entering the
    // EventStream! generator, which requires owned values.
    run_event_stream(run_name, shutdown, pg_pool.inner().clone())
}