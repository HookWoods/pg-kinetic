use pg_kinetic::{
    core::{
        admin::{parse_admin_command, AdminCommand, AdminView},
        observability::metric_catalog,
        performance::{
            PerformanceBudget, PerformanceMetric, PerformanceRegressionThreshold,
            ProfileCaptureStatus,
        },
    },
    proxy_runtime::snapshot::{PerformanceSnapshot, SnapshotStore},
};

#[test]
fn performance_admin_view_and_metrics_follow_the_public_contract() {
    assert_eq!(
        parse_admin_command(" SHOW PERFORMANCE; "),
        AdminCommand::Show(AdminView::Performance)
    );

    let budget = PerformanceBudget::new(
        PerformanceMetric::LatencyP95,
        PerformanceRegressionThreshold::Percentage(5.0),
        PerformanceRegressionThreshold::Percentage(10.0),
    );
    let snapshot = PerformanceSnapshot {
        budgets: vec![budget],
        profile_status: ProfileCaptureStatus::Unavailable,
        protocol_buffer_copies: 2,
        prepared_cache_hits: 7,
        prepared_cache_misses: 1,
        observability_hot_path_allocations: 3,
        idle_clients: 4,
        ..Default::default()
    };
    let store = SnapshotStore::new();
    store.set_performance_snapshot(snapshot);

    let stored = store.performance_snapshot();
    assert_eq!(stored.budgets.len(), 1);
    assert_eq!(stored.profile_status, ProfileCaptureStatus::Unavailable);
    assert_eq!(stored.protocol_buffer_copies, 2);
    assert_eq!(stored.prepared_cache_hits, 7);
    assert_eq!(stored.prepared_cache_misses, 1);

    let expected = [
        (
            "pg_kinetic_benchmark_latency_ms",
            &["scenario", "target", "workload", "driver", "metric"][..],
        ),
        (
            "pg_kinetic_benchmark_throughput_qps",
            &["scenario", "target", "workload", "driver"][..],
        ),
        (
            "pg_kinetic_benchmark_errors_total",
            &["scenario", "target", "workload", "driver", "outcome"][..],
        ),
        (
            "pg_kinetic_performance_budget_status",
            &["metric", "outcome"][..],
        ),
        ("pg_kinetic_process_cpu_seconds", &[][..]),
        ("pg_kinetic_process_resident_memory_bytes", &[][..]),
        ("pg_kinetic_cpu_per_query", &[][..]),
        ("pg_kinetic_memory_per_client_bytes", &[][..]),
        ("pg_kinetic_protocol_buffer_copies_total", &["feature"][..]),
        ("pg_kinetic_pool_checkout_lock_wait_ms", &["outcome"][..]),
        ("pg_kinetic_prepared_cache_hits_total", &[][..]),
        ("pg_kinetic_prepared_cache_misses_total", &[][..]),
        (
            "pg_kinetic_observability_hot_path_allocations_total",
            &["feature"][..],
        ),
        ("pg_kinetic_idle_clients", &[][..]),
    ];

    for (name, labels) in expected {
        let descriptor = metric_catalog()
            .iter()
            .find(|descriptor| descriptor.name == name)
            .unwrap_or_else(|| panic!("missing performance metric descriptor for {name}"));
        let actual_labels = descriptor
            .labels
            .iter()
            .map(|label| label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(actual_labels, labels, "unexpected labels for {name}");
    }
}
