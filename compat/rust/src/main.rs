use std::{
    env,
    time::{Duration, Instant},
};

use serde_json::json;

const MARKER: &str = "compatibility report complete";

fn target() -> String {
    env::var("PG_KINETIC_COMPAT_TARGET").unwrap_or_else(|_| "direct-postgres".into())
}

fn selected_libraries() -> Vec<&'static str> {
    match env::var("PG_KINETIC_COMPAT_LIBRARY").as_deref() {
        Ok("tokio-postgres") => vec!["tokio-postgres"],
        Ok("sqlx") => vec!["sqlx"],
        Ok("diesel") => vec!["diesel"],
        _ => vec!["tokio-postgres", "sqlx"],
    }
}

fn report(results: Vec<serde_json::Value>) {
    let failed = results.iter().any(|result| result["outcome"] == "fail");
    let passed = results
        .iter()
        .filter(|result| result["outcome"] == "pass")
        .count();
    let skipped = results
        .iter()
        .filter(|result| result["outcome"] == "skip")
        .count();
    println!(
        "{}",
        json!({
            "ok": !failed,
            "success_marker": MARKER,
            "summary": { "pass": passed, "fail": usize::from(failed), "skip": skipped },
            "results": results
        })
    );
}

fn result(
    library: &str,
    version: &str,
    outcome: &str,
    started: Instant,
    reason: Option<&str>,
    error: Option<&str>,
    cases: Vec<serde_json::Value>,
) -> serde_json::Value {
    json!({
        "suite_id": format!("rust-{library}"), "language": "rust", "library": library,
        "version": version, "target": target(), "outcome": outcome,
        "duration_ms": started.elapsed().as_millis(), "skip_reason": reason,
        "error_summary": error, "cases": cases
    })
}

fn observed(case_id: &str, outcome: &str) -> serde_json::Value {
    json!({ "case_id": case_id, "outcome": outcome })
}

async fn run_tokio_postgres(url: &str) -> Result<(), String> {
    let (mut client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connect: {error}"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query_one("SELECT $1::int", &[&1])
        .await
        .map_err(|error| format!("parameterized query: {error}"))?;
    let statement = client
        .prepare("SELECT $1::int")
        .await
        .map_err(|error| format!("prepare statement: {error}"))?;
    client
        .query_one(&statement, &[&2])
        .await
        .map_err(|error| format!("prepared query: {error}"))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| format!("begin transaction: {error}"))?;
    transaction
        .execute("CREATE TEMP TABLE IF NOT EXISTS compat_probe (id int)", &[])
        .await
        .map_err(|error| format!("create temp table: {error}"))?;
    transaction
        .rollback()
        .await
        .map_err(|error| format!("rollback transaction: {error}"))?;
    let error = client
        .query_one("SELECT * FROM compat_missing_relation", &[])
        .await;
    if error.is_ok() {
        return Err("expected missing-relation error".into());
    }
    Ok(())
}

async fn run_sqlx(url: &str) -> Result<(), String> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .map_err(|error| error.to_string())?;
    sqlx::query("SELECT $1::int")
        .bind(1_i32)
        .fetch_one(&pool)
        .await
        .map_err(|error| error.to_string())?;
    sqlx::query("SELECT $1::int")
        .bind(2_i32)
        .fetch_one(&pool)
        .await
        .map_err(|error| error.to_string())?;
    let mut transaction = pool.begin().await.map_err(|error| error.to_string())?;
    sqlx::query("CREATE TEMP TABLE IF NOT EXISTS compat_probe (id int)")
        .execute(&mut *transaction)
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .rollback()
        .await
        .map_err(|error| error.to_string())?;
    pool.close().await;
    Ok(())
}

async fn run() {
    let libraries = selected_libraries();
    let url_name = if target() == "pg-kinetic" {
        "DATABASE_URL_PROXY"
    } else {
        "DATABASE_URL_DIRECT"
    };
    let Some(url) = env::var(url_name).ok().filter(|value| !value.is_empty()) else {
        report(
            libraries
                .iter()
                .map(|library| {
                    result(
                        library,
                        "configured",
                        "skip",
                        Instant::now(),
                        Some("database-url-unavailable"),
                        Some("set the target database URL for live execution"),
                        Vec::new(),
                    )
                })
                .collect(),
        );
        return;
    };
    if env::var("PG_KINETIC_COMPAT_LIVE").as_deref() != Ok("1") {
        report(
            libraries
                .iter()
                .map(|library| {
                    result(
                        library,
                        "configured",
                        "skip",
                        Instant::now(),
                        Some("live-disabled"),
                        Some("set PG_KINETIC_COMPAT_LIVE=1"),
                        Vec::new(),
                    )
                })
                .collect(),
        );
        return;
    }
    let timeout = Duration::from_secs(
        env::var("PG_KINETIC_COMPAT_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(30),
    );
    let mut results = Vec::new();
    for library in libraries {
        if library == "diesel" {
            results.push(result(
                "diesel",
                "2",
                "skip",
                Instant::now(),
                Some("feature-unsupported"),
                Some("Diesel ORM mapping is optional for this protocol smoke"),
                Vec::new(),
            ));
            continue;
        }
        let started = Instant::now();
        let execution = if library == "tokio-postgres" {
            tokio::time::timeout(timeout, run_tokio_postgres(url.as_str())).await
        } else {
            tokio::time::timeout(timeout, run_sqlx(url.as_str())).await
        };
        let outcome = match execution {
            Ok(Ok(())) => {
                let mut cases = vec![
                    observed("startup-connect", "connected"),
                    observed("parameterized-query", "one-row"),
                    observed("prepared-statement", "one-row"),
                    observed("transaction-rollback", "rolled-back"),
                ];
                if library == "tokio-postgres" {
                    cases.push(observed("error-propagation", "sqlstate"));
                }
                result(library, "configured", "pass", started, None, None, cases)
            }
            Ok(Err(error)) => result(
                library,
                "configured",
                "fail",
                started,
                None,
                Some(&error),
                Vec::new(),
            ),
            Err(_) => result(
                library,
                "configured",
                "fail",
                started,
                None,
                Some("bounded execution timeout"),
                Vec::new(),
            ),
        };
        results.push(outcome);
    }
    report(results);
}

#[tokio::main]
async fn main() {
    run().await;
}
