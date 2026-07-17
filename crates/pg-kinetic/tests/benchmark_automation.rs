use std::{fs, path::PathBuf, process::Command};

use pg_kinetic_core::benchmark::{
    BenchmarkComparison, BenchmarkDriver, BenchmarkMetric, BenchmarkResult, BenchmarkTarget,
};
use pg_kinetic_proxy::benchmark::validate_benchmark_scenario;

fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_pg-kinetic")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates dir")
        .parent()
        .expect("repo root")
        .to_path_buf()
}

fn scenario_path() -> PathBuf {
    repo_root()
        .join("bench")
        .join("scenarios")
        .join("benchmark-basic.toml")
}

fn run_benchmark(args: &[&str]) -> std::process::Output {
    Command::new(binary_path())
        .args(args)
        .output()
        .expect("run benchmark command")
}

#[test]
fn benchmark_scenario_schema_parses() {
    let scenario = validate_benchmark_scenario(&scenario_path()).expect("scenario parses");

    assert_eq!(scenario.name(), "benchmark-basic");
    assert_eq!(scenario.driver(), BenchmarkDriver::PgBench);
    assert_eq!(scenario.duration_ms(), 60_000);
    assert_eq!(scenario.warmup_ms(), 5_000);
    assert_eq!(scenario.targets().len(), 4);
    assert_eq!(
        scenario.targets()[0].comparison(),
        BenchmarkComparison::DirectPostgreSQL
    );
    assert!(scenario.targets()[0].redacted_dsn().contains("<redacted>"));
}

#[test]
fn benchmark_result_schema_records_target_scenario_driver_and_metrics() {
    let target = BenchmarkTarget::new(
        "pg-kinetic",
        BenchmarkComparison::PgKinetic,
        "postgres://bench:bench-secret@127.0.0.1:58432/bench",
    )
    .expect("valid benchmark target");
    let metric = BenchmarkMetric::new(
        4.5,
        9.0,
        12.5,
        1_234.5,
        "x86_64",
        "resident_set_bytes",
        0.01,
    )
    .expect("valid benchmark metric");
    let result = BenchmarkResult::new(
        "benchmark-basic",
        target.clone(),
        BenchmarkDriver::PgBench,
        60_000,
        metric,
    )
    .expect("valid benchmark result");

    assert_eq!(result.scenario(), "benchmark-basic");
    assert_eq!(result.target(), &target);
    assert_eq!(result.driver(), BenchmarkDriver::PgBench);
    assert_eq!(result.duration_ms(), 60_000);
    assert_eq!(result.metrics().p50_ms(), 4.5);
    assert_eq!(result.metrics().p95_ms(), 9.0);
    assert_eq!(result.metrics().p99_ms(), 12.5);
    assert_eq!(result.metrics().throughput_qps(), 1_234.5);
    assert_eq!(result.metrics().cpu_label(), "x86_64");
    assert_eq!(result.metrics().memory_label(), "resident_set_bytes");
    assert_eq!(result.metrics().error_rate(), 0.01);
}

#[test]
fn benchmark_comparison_labels_are_supported() {
    let labels = [
        (BenchmarkComparison::DirectPostgreSQL, "direct_postgresql"),
        (BenchmarkComparison::PgBouncer, "pgbouncer"),
        (BenchmarkComparison::PgDog, "pgdog"),
        (BenchmarkComparison::PgKinetic, "pg_kinetic"),
    ];

    for (comparison, expected) in labels {
        assert_eq!(comparison.as_str(), expected);
        assert_eq!(
            expected
                .parse::<BenchmarkComparison>()
                .expect("label parses"),
            comparison
        );
    }
}

#[test]
fn benchmark_validate_command_redacts_credentials_without_running_load() {
    let output = run_benchmark(&[
        "benchmark",
        "validate",
        "--scenario",
        scenario_path().to_str().expect("scenario path"),
    ]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":true"));
    assert!(stdout.contains("<redacted>"));
    assert!(!stdout.contains("bench-secret"));
}

#[test]
fn benchmark_run_command_supports_json_output() {
    let output = run_benchmark(&[
        "benchmark",
        "run",
        "--scenario",
        scenario_path().to_str().expect("scenario path"),
        "--format",
        "json",
    ]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":true"));
    assert!(stdout.contains("\"results\""));
    assert!(stdout.contains("\"metrics\""));
    assert!(stdout.contains("\"p50_ms\""));
    assert!(!stdout.contains("bench-secret"));
}

#[test]
fn benchmark_scenario_file_exists() {
    let scenario = scenario_path();
    assert!(scenario.exists());
    let contents = fs::read_to_string(scenario).expect("read scenario file");
    assert!(contents.contains("benchmark-basic"));
}
