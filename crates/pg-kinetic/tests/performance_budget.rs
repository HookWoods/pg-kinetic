use pg_kinetic::core::performance::{
    BenchmarkTarget, PerformanceBudget, PerformanceBudgetOutcome, PerformanceBudgetSet,
    PerformanceMetric, PerformanceRegressionThreshold,
};

#[test]
fn performance_metric_labels_are_stable() {
    let labels = [
        (PerformanceMetric::LatencyP50, "latency_p50"),
        (PerformanceMetric::LatencyP95, "latency_p95"),
        (PerformanceMetric::LatencyP99, "latency_p99"),
        (PerformanceMetric::LatencyP999, "latency_p999"),
        (PerformanceMetric::Throughput, "throughput"),
        (PerformanceMetric::CpuPerQuery, "cpu_per_query"),
        (PerformanceMetric::MemoryPerClient, "memory_per_client"),
        (PerformanceMetric::ErrorRate, "error_rate"),
    ];

    for (metric, expected) in labels {
        assert_eq!(metric.as_str(), expected);
    }
}

#[test]
fn benchmark_target_labels_are_stable() {
    let labels = [
        (BenchmarkTarget::DirectPostgres, "direct_postgresql"),
        (BenchmarkTarget::PgBouncer, "pgbouncer"),
        (BenchmarkTarget::PgDog, "pgdog"),
        (BenchmarkTarget::PgKinetic, "pg_kinetic"),
    ];

    for (target, expected) in labels {
        assert_eq!(target.as_str(), expected);
    }
}

#[test]
fn budget_comparison_classifies_pass_warning_and_failure() {
    let budget = PerformanceBudget::new(
        PerformanceMetric::LatencyP99,
        PerformanceRegressionThreshold::Percentage(10.0),
        PerformanceRegressionThreshold::Percentage(20.0),
    );

    assert_eq!(
        budget
            .evaluate("read-heavy", BenchmarkTarget::PgKinetic, 105.0, Some(100.0))
            .outcome(),
        PerformanceBudgetOutcome::Passed
    );
    assert_eq!(
        budget
            .evaluate("read-heavy", BenchmarkTarget::PgKinetic, 115.0, Some(100.0))
            .outcome(),
        PerformanceBudgetOutcome::Warning
    );
    assert_eq!(
        budget
            .evaluate("read-heavy", BenchmarkTarget::PgKinetic, 125.0, Some(100.0))
            .outcome(),
        PerformanceBudgetOutcome::Failed
    );
}

#[test]
fn regression_thresholds_support_percentage_and_absolute_deltas() {
    let percentage = PerformanceRegressionThreshold::Percentage(12.5);
    let absolute = PerformanceRegressionThreshold::Absolute(3.5);

    assert_eq!(percentage.allowed_delta(80.0), Some(10.0));
    assert_eq!(absolute.allowed_delta(80.0), Some(3.5));

    let throughput_budget = PerformanceBudget::new(
        PerformanceMetric::Throughput,
        PerformanceRegressionThreshold::Absolute(50.0),
        PerformanceRegressionThreshold::Absolute(100.0),
    );
    assert_eq!(
        throughput_budget
            .evaluate(
                "read-heavy",
                BenchmarkTarget::PgKinetic,
                925.0,
                Some(1_000.0)
            )
            .outcome(),
        PerformanceBudgetOutcome::Warning
    );
}

#[test]
fn budget_result_records_measurement_context_and_outcome() {
    let budget = PerformanceBudget::new(
        PerformanceMetric::CpuPerQuery,
        PerformanceRegressionThreshold::Absolute(0.5),
        PerformanceRegressionThreshold::Absolute(1.0),
    );

    let result = budget.evaluate("write-heavy", BenchmarkTarget::PgKinetic, 2.3, Some(1.5));

    assert_eq!(result.scenario(), "write-heavy");
    assert_eq!(result.target(), BenchmarkTarget::PgKinetic);
    assert_eq!(result.metric(), PerformanceMetric::CpuPerQuery);
    assert_eq!(result.observed_value(), 2.3);
    assert_eq!(result.baseline_value(), Some(1.5));
    assert_eq!(result.outcome(), PerformanceBudgetOutcome::Warning);
}

#[test]
fn missing_or_unknown_baselines_warn_instead_of_passing() {
    let budget_set = PerformanceBudgetSet::new([PerformanceBudget::new(
        PerformanceMetric::ErrorRate,
        PerformanceRegressionThreshold::Absolute(0.01),
        PerformanceRegressionThreshold::Absolute(0.02),
    )]);

    let missing = budget_set.evaluate(
        "write-heavy",
        BenchmarkTarget::PgKinetic,
        PerformanceMetric::ErrorRate,
        0.01,
        None,
    );
    let unknown = budget_set.evaluate(
        "write-heavy",
        BenchmarkTarget::PgKinetic,
        PerformanceMetric::ErrorRate,
        0.01,
        Some(f64::NAN),
    );

    assert_eq!(missing.outcome(), PerformanceBudgetOutcome::Warning);
    assert_eq!(unknown.outcome(), PerformanceBudgetOutcome::Warning);
}
