use std::{fs, path::PathBuf, process::Command};

use pg_kinetic_core::benchmark::{
    BenchmarkComparison, BenchmarkDriver, BenchmarkScenario, BenchmarkTarget,
    BenchmarkValidationError, BenchmarkWorkloadKind,
};
use pg_kinetic_proxy::benchmark::{
    benchmark_target_is_required, benchmark_target_label, validate_benchmark_scenario,
    validate_benchmark_targets_with, BenchmarkTargetAvailability, BenchmarkTargetOutcome,
    BenchmarkTargetReportOutcome,
};

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

fn temporary_report_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "pg-kinetic-benchmark-report-{name}-{}-{}.json",
        std::process::id(),
        name.len()
    ))
}

fn report_fixture(
    scenario: &str,
    targets: &[&str],
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    throughput_qps: f64,
) -> String {
    let results = targets
        .iter()
        .map(|comparison| {
            format!(
                r#"{{"target":{{"comparison":"{comparison}"}},"metrics":{{"p50_ms":{p50_ms},"p95_ms":{p95_ms},"p99_ms":{p99_ms},"throughput_qps":{throughput_qps},"error_rate":0.0}}}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"scenario":{{"name":"{scenario}"}},"results":[{results}]}}"#)
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

fn benchmark_target(label: &str, comparison: BenchmarkComparison, port: u16) -> BenchmarkTarget {
    BenchmarkTarget::new(
        label,
        comparison,
        format!("postgres://bench:target-secret@127.0.0.1:{port}/bench"),
    )
    .expect("target is valid")
}

fn orchestration_scenario(targets: Vec<BenchmarkTarget>) -> BenchmarkScenario {
    BenchmarkScenario::new(
        "target-orchestration",
        BenchmarkDriver::PgBench,
        1_000,
        100,
        targets,
    )
    .expect("scenario is valid")
}

#[test]
fn target_orchestration_requires_direct_postgresql_and_pg_kinetic() {
    for comparison in [
        BenchmarkComparison::DirectPostgreSQL,
        BenchmarkComparison::PgKinetic,
    ] {
        let scenario = orchestration_scenario(vec![benchmark_target(
            "configured-label",
            comparison,
            54_321,
        )]);
        let report = validate_benchmark_targets_with(&scenario, |_| false);
        let target = &report.targets()[0];

        assert!(benchmark_target_is_required(comparison));
        assert_eq!(
            report.outcome(),
            BenchmarkTargetReportOutcome::FailedRequired
        );
        assert!(!report.can_run());
        assert_eq!(
            target.availability(),
            BenchmarkTargetAvailability::Unavailable
        );
        assert_eq!(target.outcome(), BenchmarkTargetOutcome::FailedRequired);
    }
}

#[test]
fn target_orchestration_supports_optional_competitors() {
    let scenario = orchestration_scenario(vec![
        benchmark_target(
            "configured-pgbouncer",
            BenchmarkComparison::PgBouncer,
            64_320,
        ),
        benchmark_target("configured-pgdog", BenchmarkComparison::PgDog, 64_319),
    ]);
    let report = validate_benchmark_targets_with(&scenario, |_| true);

    assert_eq!(report.outcome(), BenchmarkTargetReportOutcome::Ready);
    assert!(report.can_run());
    assert!(!benchmark_target_is_required(
        BenchmarkComparison::PgBouncer
    ));
    assert!(!benchmark_target_is_required(BenchmarkComparison::PgDog));
    assert!(report.targets().iter().all(|target| {
        target.availability() == BenchmarkTargetAvailability::Ready
            && target.outcome() == BenchmarkTargetOutcome::Ready
    }));
}

#[test]
fn unavailable_optional_target_is_partial_and_skipped() {
    let scenario = orchestration_scenario(vec![
        benchmark_target(
            "configured-direct",
            BenchmarkComparison::DirectPostgreSQL,
            54_321,
        ),
        benchmark_target(
            "configured-pgbouncer",
            BenchmarkComparison::PgBouncer,
            64_320,
        ),
        benchmark_target("configured-kinetic", BenchmarkComparison::PgKinetic, 64_318),
    ]);
    let report = validate_benchmark_targets_with(&scenario, |target| {
        target.comparison() != BenchmarkComparison::PgBouncer
    });
    let optional = report
        .targets()
        .iter()
        .find(|target| target.comparison() == BenchmarkComparison::PgBouncer)
        .expect("PgBouncer target report");

    assert_eq!(report.outcome(), BenchmarkTargetReportOutcome::Partial);
    assert!(report.can_run());
    assert_eq!(
        optional.availability(),
        BenchmarkTargetAvailability::Unavailable
    );
    assert_eq!(optional.outcome(), BenchmarkTargetOutcome::SkippedOptional);
}

#[test]
fn target_reports_use_stable_labels_and_redacted_connection_strings() {
    let scenario = orchestration_scenario(vec![
        benchmark_target(
            "direct-custom",
            BenchmarkComparison::DirectPostgreSQL,
            54_321,
        ),
        benchmark_target("bouncer-custom", BenchmarkComparison::PgBouncer, 64_320),
        benchmark_target("dog-custom", BenchmarkComparison::PgDog, 64_319),
        benchmark_target("kinetic-custom", BenchmarkComparison::PgKinetic, 64_318),
    ]);
    let report = validate_benchmark_targets_with(&scenario, |_| true);
    let labels = report
        .targets()
        .iter()
        .map(|target| target.label())
        .collect::<Vec<_>>();

    assert_eq!(
        labels,
        vec!["direct-postgresql", "pgbouncer", "pgdog", "pg-kinetic"]
    );
    assert_eq!(
        benchmark_target_label(BenchmarkComparison::DirectPostgreSQL),
        "direct-postgresql"
    );
    assert!(report
        .targets()
        .iter()
        .all(|target| !target.dsn().contains("target-secret")));
    assert!(report
        .targets()
        .iter()
        .all(|target| target.dsn().contains("<redacted>@")));
}

#[test]
fn all_tracked_benchmark_scenarios_parse_with_complete_workload_matrix() {
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

    let missing_matrix = write_temporary_scenario(
        "name = \"missing-target-matrix\"\n[[targets]]\nlabel = \"legacy-target\"\ncomparison = \"pg_kinetic\"\ndsn = \"postgres://bench:benchmark-secret@127.0.0.1:8432/bench\"\n",
    );
    assert_eq!(
        validate_benchmark_scenario(&missing_matrix)
            .expect_err("legacy targets require target_matrix"),
        BenchmarkValidationError::MissingTargetMatrix
    );
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

    let query_credential_target = BenchmarkTarget::new(
        "query-credentials",
        BenchmarkComparison::PgKinetic,
        "postgres://bench:userinfo-secret@127.0.0.1:8432/bench?password=query-secret&application_name=benchmark-lab",
    )
    .expect("target with query credentials is valid");
    let redacted = query_credential_target.redacted_dsn();
    assert!(!redacted.contains("userinfo-secret"));
    assert!(!redacted.contains("query-secret"));
    assert!(redacted.contains("<redacted>@"));
    assert!(redacted.contains("password=<redacted>"));
    assert!(redacted.contains("application_name=benchmark-lab"));

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

    let stdout = String::from_utf8(first.stdout).expect("utf-8 benchmark output");
    assert!(stdout.trim_start().starts_with('{'));
    assert!(stdout.trim_end().ends_with('}'));
    assert!(stdout.contains("\"ok\":true"));
    assert!(stdout.contains("\"scenario\""));
    assert!(stdout.contains("\"results\""));
    assert!(!stdout.contains("benchmark-secret"));
}

#[test]
fn benchmark_dry_run_writes_a_redacted_json_report() {
    let scenario = scenario_path("simple-query");
    let report_path = temporary_report_path("dry-run");
    let output = Command::new(binary_path())
        .args([
            "benchmark",
            "run",
            "--scenario",
            scenario.to_str().expect("scenario path"),
            "--format",
            "json",
            "--dry-run",
            "--output",
            report_path.to_str().expect("report path"),
        ])
        .output()
        .expect("run benchmark dry-run");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 benchmark output");
    let report = fs::read_to_string(&report_path).expect("read benchmark report");
    fs::remove_file(&report_path).expect("remove benchmark report");

    assert_eq!(stdout.trim(), report);
    assert!(report.contains("\"dry_run\":true"));
    assert!(report.contains("\"workload\":\"simple_query\""));
    assert!(report.contains("\"metrics\""));
    assert!(report.contains("\"environment\""));
    assert!(report.contains("\"git\""));
    assert!(report.contains("<redacted>@"));
    assert!(!report.contains("benchmark-secret"));
}

#[test]
fn benchmark_comparison_reports_pass_warning_and_failure_from_budgets() {
    let baseline_path = temporary_report_path("baseline");
    let current_path = temporary_report_path("current");
    fs::write(
        &baseline_path,
        report_fixture("fixture", &["pg_kinetic"], 100.0, 100.0, 100.0, 100.0),
    )
    .expect("write baseline report");
    fs::write(
        &current_path,
        report_fixture("fixture", &["pg_kinetic"], 100.0, 107.0, 112.0, 100.0),
    )
    .expect("write current report");

    let output = Command::new(binary_path())
        .args([
            "benchmark",
            "compare",
            "--baseline",
            baseline_path.to_str().expect("baseline path"),
            "--current",
            current_path.to_str().expect("current path"),
        ])
        .output()
        .expect("compare benchmark reports");
    fs::remove_file(&baseline_path).expect("remove baseline report");
    fs::remove_file(&current_path).expect("remove current report");

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 comparison output");
    assert!(stdout.contains("\"outcome\":\"failed\""));
    assert!(stdout.contains("\"outcome\":\"passed\""));
    assert!(stdout.contains("\"outcome\":\"warning\""));
}

#[test]
fn benchmark_comparison_rejects_different_scenario_names() {
    let baseline_path = temporary_report_path("scenario-baseline");
    let current_path = temporary_report_path("scenario-current");
    fs::write(
        &baseline_path,
        report_fixture(
            "baseline-scenario",
            &["pg_kinetic"],
            100.0,
            100.0,
            100.0,
            100.0,
        ),
    )
    .expect("write baseline report");
    fs::write(
        &current_path,
        report_fixture(
            "current-scenario",
            &["pg_kinetic"],
            100.0,
            100.0,
            100.0,
            100.0,
        ),
    )
    .expect("write current report");

    let output = Command::new(binary_path())
        .args([
            "benchmark",
            "compare",
            "--baseline",
            baseline_path.to_str().expect("baseline path"),
            "--current",
            current_path.to_str().expect("current path"),
        ])
        .output()
        .expect("compare benchmark reports");
    fs::remove_file(&baseline_path).expect("remove baseline report");
    fs::remove_file(&current_path).expect("remove current report");

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 comparison output");
    assert!(stdout.contains("\"outcome\":\"failed\""));
    assert!(stdout.contains("scenario names differ"));
}

#[test]
fn benchmark_comparison_rejects_different_target_sets() {
    let baseline_path = temporary_report_path("targets-baseline");
    let current_path = temporary_report_path("targets-current");
    fs::write(
        &baseline_path,
        report_fixture(
            "fixture",
            &["direct_postgresql", "pg_kinetic"],
            100.0,
            100.0,
            100.0,
            100.0,
        ),
    )
    .expect("write baseline report");
    fs::write(
        &current_path,
        report_fixture("fixture", &["pg_kinetic"], 100.0, 100.0, 100.0, 100.0),
    )
    .expect("write current report");

    let output = Command::new(binary_path())
        .args([
            "benchmark",
            "compare",
            "--baseline",
            baseline_path.to_str().expect("baseline path"),
            "--current",
            current_path.to_str().expect("current path"),
        ])
        .output()
        .expect("compare benchmark reports");
    fs::remove_file(&baseline_path).expect("remove baseline report");
    fs::remove_file(&current_path).expect("remove current report");

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 comparison output");
    assert!(stdout.contains("\"outcome\":\"failed\""));
    assert!(stdout.contains("target sets differ"));
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
