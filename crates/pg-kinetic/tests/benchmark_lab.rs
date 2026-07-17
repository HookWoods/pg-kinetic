use std::{fs, path::PathBuf, process::Command};

use pg_kinetic_core::benchmark::{
    BenchmarkComparison, BenchmarkDriver, BenchmarkScenario, BenchmarkTarget, BenchmarkWorkloadKind,
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

fn scenario_path(name: &str) -> PathBuf {
    repo_root()
        .join("bench")
        .join("scenarios")
        .join(format!("benchmark-{name}.toml"))
}

fn write_temporary_scenario(contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pg-kinetic-benchmark-lab-{}-{}.toml",
        std::process::id(),
        contents.len()
    ));
    fs::write(&path, contents).expect("write temporary benchmark scenario");
    path
}

fn target_matrix() -> &'static str {
    r#"
[target_matrix]
[[target_matrix.targets]]
label = "pg-kinetic"
comparison = "pg_kinetic"
dsn = "postgres://bench:benchmark-secret@127.0.0.1:8432/bench"
"#
}

#[test]
fn benchmark_scenarios_parse_with_complete_workload_matrix() {
    let scenarios = [
        ("simple-query", BenchmarkWorkloadKind::SimpleQuery),
        ("extended-query", BenchmarkWorkloadKind::ExtendedQuery),
        ("prepared", BenchmarkWorkloadKind::PreparedStatementReuse),
        ("transaction-pool", BenchmarkWorkloadKind::TransactionPool),
        ("idle-clients", BenchmarkWorkloadKind::IdleClients),
        (
            "routing-sharding-policy",
            BenchmarkWorkloadKind::RoutingShardingPolicy,
        ),
    ];

    for (name, workload) in scenarios {
        let scenario = validate_benchmark_scenario(&scenario_path(name)).expect("scenario parses");

        assert_eq!(scenario.workload(), workload);
        assert!(scenario.duration_ms() > 0);
        assert!(scenario.warmup().duration_ms() > 0);
        assert!(scenario.connections().concurrency() > 0);
        assert!(scenario.connections().connection_count() > 0);
        assert!(!scenario.matrix().targets().is_empty());
        assert!(scenario.expected_metrics().any_enabled());
    }
}

#[test]
fn scenario_validation_rejects_zero_duration_concurrency_and_target_matrix() {
    let zero_duration = write_temporary_scenario(&format!(
        "name = \"zero-duration\"\nduration_ms = 0\n{}",
        target_matrix()
    ));
    assert!(validate_benchmark_scenario(&zero_duration).is_err());
    fs::remove_file(zero_duration).expect("remove zero duration scenario");

    let zero_concurrency = write_temporary_scenario(&format!(
        "name = \"zero-concurrency\"\n[connections]\nconcurrency = 0\nconnection_count = 1\n{}",
        target_matrix()
    ));
    assert!(validate_benchmark_scenario(&zero_concurrency).is_err());
    fs::remove_file(zero_concurrency).expect("remove zero concurrency scenario");

    let missing_matrix = write_temporary_scenario("name = \"missing-target-matrix\"\n");
    assert!(validate_benchmark_scenario(&missing_matrix).is_err());
    fs::remove_file(missing_matrix).expect("remove missing matrix scenario");
}

#[test]
fn advanced_scenario_explicitly_measures_feature_overhead() {
    let scenario = validate_benchmark_scenario(&scenario_path("routing-sharding-policy"))
        .expect("advanced scenario parses");

    assert!(scenario.features().read_routing());
    assert!(scenario.features().sharding());
    assert!(scenario.features().policy_overhead());
}

#[test]
fn scenario_debug_and_report_output_redact_credentials() {
    let scenario = validate_benchmark_scenario(&scenario_path("simple-query"))
        .expect("simple query scenario parses");
    let debug = format!("{scenario:?}");
    assert!(!debug.contains("benchmark-secret"));
    assert!(debug.contains("<redacted>"));

    let path = scenario_path("simple-query");
    let output = Command::new(binary_path())
        .args([
            "benchmark",
            "validate",
            "--scenario",
            path.to_str().expect("scenario path"),
        ])
        .output()
        .expect("run benchmark validation");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 benchmark output");
    assert!(!stdout.contains("benchmark-secret"));
    assert!(stdout.contains("<redacted>"));
}

#[test]
fn scenario_output_is_stable_json_compatible_data() {
    let path = scenario_path("prepared");
    let arguments = [
        "benchmark",
        "run",
        "--scenario",
        path.to_str().expect("scenario path"),
        "--format",
        "json",
    ];

    let first = Command::new(binary_path())
        .args(arguments)
        .output()
        .expect("run first benchmark report");
    let second = Command::new(binary_path())
        .args(arguments)
        .output()
        .expect("run second benchmark report");

    assert!(first.status.success());
    assert!(second.status.success());
    assert_eq!(first.stdout, second.stdout);

    let stdout = String::from_utf8(first.stdout).expect("utf-8 benchmark output");
    assert!(stdout.trim_start().starts_with('{'));
    assert!(stdout.trim_end().ends_with('}'));
    assert!(stdout.contains("\"ok\":true"));
    assert!(stdout.contains("\"scenario\""));
    assert!(stdout.contains("\"results\""));
    assert!(!stdout.contains("benchmark-secret"));
}

#[test]
fn benchmark_scenario_constructor_remains_compatible_with_existing_callers() {
    let target = BenchmarkTarget::new(
        "pg-kinetic",
        BenchmarkComparison::PgKinetic,
        "postgres://bench:benchmark-secret@127.0.0.1:8432/bench",
    )
    .expect("target is valid");
    let scenario = BenchmarkScenario::new(
        "compatibility",
        BenchmarkDriver::PgBench,
        1_000,
        100,
        vec![target],
    )
    .expect("legacy scenario constructor remains valid");

    assert_eq!(scenario.workload(), BenchmarkWorkloadKind::SimpleQuery);
    assert_eq!(scenario.connections().concurrency(), 16);
}
