// src/db/pg.rs
//
// Pooled Postgres client for tekton-results, mirroring the Redis pool
// pattern: create once at startup, manage via Rocket's state, inject
// wherever needed.
//
// Uses `deadpool-postgres` — async, no `spawn_blocking`, works natively
// with `tokio-postgres`. Add to Cargo.toml:
//   deadpool-postgres = { version = "0.14", features = [] }
//   tokio-postgres    = { version = "0.7", features = ["with-serde_json-1"] }

use deadpool_postgres::{Config, Pool, PoolError, Runtime};
use tokio_postgres::NoTls;

#[derive(Debug)]
pub enum PgPoolError {
    Build(String),
}

impl std::fmt::Display for PgPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgPoolError::Build(e) => write!(f, "failed to build pg pool: {e}"),
        }
    }
}

/// Build the pool at startup. Mirrors `create_redis_pool` — called once
/// from `main`, the returned `Pool` is passed to `.manage()`.
pub fn create_pg_pool() -> Result<Pool, PgPoolError> {
    let mut cfg = Config::new();
    cfg.host = Some(
        std::env::var("TEKTON_RESULTS_PG_HOST")
            .unwrap_or_else(|_| "tekton-results-postgres-service.tekton-pipelines.svc.cluster.local".into()),
    );
    cfg.port = Some(
        std::env::var("TEKTON_RESULTS_PG_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5432),
    );
    cfg.user = Some(
        std::env::var("TEKTON_RESULTS_PG_USER")
            .unwrap_or_else(|_| "tekton".into()),
    );
    cfg.password = Some(
        std::env::var("TEKTON_RESULTS_PG_PASSWORD")
            .unwrap_or_else(|_| {
                eprintln!("Warning: TEKTON_RESULTS_PG_PASSWORD not set");
                String::new()
            }),
    );
    cfg.dbname = Some(
        std::env::var("TEKTON_RESULTS_PG_DB")
            .unwrap_or_else(|_| "tekton-results".into()),
    );

    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .map_err(|e| PgPoolError::Build(e.to_string()))
}

/// Convenience type alias used in handler signatures.
pub type PgPool = Pool;

/// Error wrapper so `PoolError` maps cleanly into `ArchiveError`.
impl From<PoolError> for crate::handlers::run_archive::ArchiveError {
    fn from(e: PoolError) -> Self {
        crate::handlers::run_archive::ArchiveError::Db(e.to_string())
    }
}

impl From<tokio_postgres::Error> for crate::handlers::run_archive::ArchiveError {
    fn from(e: tokio_postgres::Error) -> Self {
        crate::handlers::run_archive::ArchiveError::Db(e.to_string())
    }
}