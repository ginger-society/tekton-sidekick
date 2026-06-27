// src/main.rs

#[macro_use]
extern crate rocket;

use dotenv::dotenv;
use rocket::{Build, Rocket};
use rocket_okapi::openapi_get_routes;
use rocket_okapi::swagger_ui::{make_swagger_ui, SwaggerUIConfig};
use rocket_prometheus::PrometheusMetrics;
use std::env;

use db::redis::create_redis_pool;
use db::pg::create_pg_pool;

mod db;
mod fairings;
mod handlers;
mod middlewares;
mod models;
mod routes;

const SERVICE_PREFIX: &str = "sidekick";

#[tokio::main]
async fn main() {
    dotenv().ok();

    println!("Starting server...");

    let prometheus = PrometheusMetrics::new();

    let mut server = rocket::build()
        .attach(fairings::cors::CORS)
        .attach(prometheus.clone())
        .mount(
            format!("/{}/", SERVICE_PREFIX),
            openapi_get_routes![routes::index],
        )
        .mount(
            format!("/{}/api-docs", SERVICE_PREFIX),
            make_swagger_ui(&SwaggerUIConfig {
                url: "../openapi.json".to_owned(),
                ..Default::default()
            }),
        )
        .mount(format!("/{}/metrics", SERVICE_PREFIX), prometheus)
        .mount(
            format!("/{}/", SERVICE_PREFIX),
            routes![
                routes::stream_counter,
                routes::run_stream::stream_pipeline_run,
            ],
        );

    match env::var("MONGO_URI") {
        Ok(mongo_uri) => match env::var("MONGO_DB_NAME") {
            Ok(mongo_db_name) => {
                println!("Attempting to connect to mongo");
                server = server.manage(db::connect_mongo(mongo_uri, mongo_db_name))
            }
            Err(_) => println!("Not connecting to mongo, missing MONGO_DB_NAME"),
        },
        Err(_) => println!("Not connecting to mongo, missing MONGO_URI"),
    };

    match env::var("REDIS_URI") {
        Ok(redis_uri) => {
            println!("Attempting to connect to redis");
            let redis_pool = create_redis_pool(redis_uri.clone()).await;
            server = server.manage(redis_pool);
        }
        Err(_) => println!("Not connecting to redis"),
    }

    // Postgres pool for tekton-results archive fallback.
    // Non-fatal if the password is missing — the live Tekton path works
    // without it; the archive path will error at request time.
    match create_pg_pool() {
        Ok(pg_pool) => {
            println!("Postgres pool created (tekton-results)");
            server = server.manage(pg_pool);
        }
        Err(e) => {
            println!("Warning: could not create Postgres pool: {e} — archive fallback disabled");
        }
    }

    server.launch().await.expect("Failed to launch Rocket");
}

#[cfg(test)]
mod tests;