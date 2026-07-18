use std::collections::BTreeSet;

use pg_kinetic::{
    core::observability::{metric_catalog, MetricKind},
    route::{QueryClass, RouteKey},
};

#[test]
fn metric_catalog_is_complete_and_stable() {
    let catalog = metric_catalog();
    let mut seen_names = BTreeSet::new();

    let expected = [
        (
            "pg_kinetic_client_connections_total",
            MetricKind::Counter,
            "1",
            &[][..],
            "Single counter without labels.",
        ),
        (
            "pg_kinetic_runtime_lifecycle_state",
            MetricKind::Gauge,
            "1",
            &["state"][..],
            "Runtime lifecycle state series.",
        ),
        (
            "pg_kinetic_runtime_readiness_state",
            MetricKind::Gauge,
            "1",
            &["state"][..],
            "Runtime readiness state series.",
        ),
        (
            "pg_kinetic_runtime_shutdown_total",
            MetricKind::Counter,
            "1",
            &["reason"][..],
            "Runtime shutdown counts by reason.",
        ),
        (
            "pg_kinetic_node_heartbeat_age_ms",
            MetricKind::Gauge,
            "ms",
            &["node"][..],
            "Node heartbeat age in milliseconds.",
        ),
        (
            "pg_kinetic_prepared_events_total",
            MetricKind::Counter,
            "1",
            &["event"][..],
            "Prepared statement virtualization events",
        ),
        (
            "pg_kinetic_pool_checkout_wait_ms",
            MetricKind::Histogram,
            "ms",
            &["stage", "outcome"][..],
            "Stage is bounded to request, route-gate registry lock lookup, or checkout; outcome splits successful, timeout, canceled, and error waits.",
        ),
        (
            "pg_kinetic_backend_pin_total",
            MetricKind::Counter,
            "1",
            &["reason"][..],
            "Reason values stay aligned with pinning causes.",
        ),
        (
            "pg_kinetic_backend_cleanup_total",
            MetricKind::Counter,
            "1",
            &["action"][..],
            "Action values stay aligned with cleanup outcomes.",
        ),
        (
            "pg_kinetic_backend_recovery_total",
            MetricKind::Counter,
            "1",
            &["trigger", "action", "outcome"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_backend_sqlstate_total",
            MetricKind::Counter,
            "1",
            &["sqlstate"][..],
            "SQLSTATE is a normalized error code",
        ),
        (
            "pg_kinetic_read_after_write_total",
            MetricKind::Counter,
            "1",
            &["outcome"][..],
            "freshness states",
        ),
        (
            "pg_kinetic_read_after_write_wait_ms",
            MetricKind::Histogram,
            "ms",
            &["route", "outcome"][..],
            "freshness states",
        ),
        (
            "pg_kinetic_read_after_write_rejections_total",
            MetricKind::Counter,
            "1",
            &["route", "outcome"][..],
            "freshness states",
        ),
        (
            "pg_kinetic_route_decisions_total",
            MetricKind::Counter,
            "1",
            &["route", "target_role", "query_class"][..],
            "routing enums",
        ),
        (
            "pg_kinetic_route_fallbacks_total",
            MetricKind::Counter,
            "1",
            &["route", "reason", "fallback_policy"][..],
            "routing enums",
        ),
        (
            "pg_kinetic_shard_route_decisions_total",
            MetricKind::Counter,
            "1",
            &["route", "shard", "strategy", "reason", "outcome"][..],
            "bucketed shard labels",
        ),
        (
            "pg_kinetic_shard_multi_shard_rejections_total",
            MetricKind::Counter,
            "1",
            &["route", "shard", "policy", "reason", "outcome"][..],
            "bucketed shard labels",
        ),
        (
            "pg_kinetic_shard_primary_fallbacks_total",
            MetricKind::Counter,
            "1",
            &["route", "shard", "policy", "outcome"][..],
            "bucketed shard labels",
        ),
        (
            "pg_kinetic_route_map_reload_total",
            MetricKind::Counter,
            "1",
            &["outcome", "error_code"][..],
            "reload outcomes",
        ),
        (
            "pg_kinetic_policy_decisions_total",
            MetricKind::Counter,
            "1",
            &["policy", "mode", "hook", "action", "outcome"][..],
            "policy decisions",
        ),
        (
            "pg_kinetic_policy_eval_duration_ms",
            MetricKind::Histogram,
            "ms",
            &["policy", "mode", "hook", "outcome"][..],
            "policy evaluation",
        ),
        (
            "pg_kinetic_policy_denies_total",
            MetricKind::Counter,
            "1",
            &["policy", "reason"][..],
            "policy denies",
        ),
        (
            "pg_kinetic_policy_dry_run_total",
            MetricKind::Counter,
            "1",
            &["policy", "mode", "hook", "action"][..],
            "dry run",
        ),
        (
            "pg_kinetic_policy_reload_total",
            MetricKind::Counter,
            "1",
            &["source", "mode", "outcome", "error_code"][..],
            "reload outcomes",
        ),
        (
            "pg_kinetic_policy_active",
            MetricKind::Gauge,
            "1",
            &["source", "mode"][..],
            "active policy",
        ),
        (
            "pg_kinetic_policy_audit_events_total",
            MetricKind::Counter,
            "1",
            &["policy", "mode", "hook", "action", "outcome", "reason"][..],
            "audit events",
        ),
        (
            "pg_kinetic_policy_wasm_eval_total",
            MetricKind::Counter,
            "1",
            &["source", "mode", "hook", "outcome", "error_code"][..],
            "wasm eval",
        ),
        (
            "pg_kinetic_policy_wasm_eval_duration_ms",
            MetricKind::Histogram,
            "ms",
            &["source", "mode", "hook", "outcome"][..],
            "wasm eval",
        ),
        (
            "pg_kinetic_route_map_generation",
            MetricKind::Gauge,
            "1",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_mirror_decisions_total",
            MetricKind::Counter,
            "1",
            &["mode", "target", "outcome"][..],
            "Mirror decisions by mode, target, and outcome.",
        ),
        (
            "pg_kinetic_mirror_in_flight",
            MetricKind::Gauge,
            "1",
            &["mode", "target"][..],
            "Current mirror in-flight count by mode and target.",
        ),
        (
            "pg_kinetic_mirror_duration_ms",
            MetricKind::Histogram,
            "ms",
            &["mode", "target", "outcome"][..],
            "Mirror task duration in milliseconds.",
        ),
        (
            "pg_kinetic_mirror_dropped_total",
            MetricKind::Counter,
            "1",
            &["mode", "reason"][..],
            "Dropped mirror tasks by mode and reason.",
        ),
        (
            "pg_kinetic_adaptive_recommendations_total",
            MetricKind::Counter,
            "1",
            &["mode", "target", "outcome"][..],
            "Adaptive recommendations by mode, target, and outcome.",
        ),
        (
            "pg_kinetic_adaptive_apply_total",
            MetricKind::Counter,
            "1",
            &["mode", "target", "outcome"][..],
            "Adaptive apply outcomes by mode, target, and outcome.",
        ),
        (
            "pg_kinetic_benchmark_runs_total",
            MetricKind::Counter,
            "1",
            &["engine", "target", "outcome"][..],
            "Benchmark runs by engine, target, and outcome.",
        ),
        (
            "pg_kinetic_preflight_findings_total",
            MetricKind::Counter,
            "1",
            &["check", "severity"][..],
            "Preflight findings by check and severity.",
        ),
        (
            "pg_kinetic_shard_lifecycle_state",
            MetricKind::Gauge,
            "1",
            &["shard", "lifecycle_state"][..],
            "bucketed shard labels",
        ),
        (
            "pg_kinetic_shard_active_transactions",
            MetricKind::Gauge,
            "1",
            &["shard"][..],
            "bucketed shard labels",
        ),
        (
            "pg_kinetic_shard_prepared_statements",
            MetricKind::Gauge,
            "1",
            &["shard"][..],
            "bucketed shard labels",
        ),
        (
            "pg_kinetic_replica_health",
            MetricKind::Gauge,
            "1",
            &["endpoint", "health"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_replica_lag_ms",
            MetricKind::Gauge,
            "ms",
            &["endpoint", "lag_state"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_replica_replay_lsn",
            MetricKind::Gauge,
            "lsn",
            &["endpoint", "target_role"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_split_brain_warnings_total",
            MetricKind::Counter,
            "1",
            &["endpoint", "target_role", "reason"][..],
            "role mismatch",
        ),
        (
            "pg_kinetic_backpressure_events_total",
            MetricKind::Counter,
            "1",
            &["route", "outcome"][..],
            "client addresses",
        ),
        (
            "pg_kinetic_route_checkout_wait_ms",
            MetricKind::Histogram,
            "ms",
            &["route", "outcome"][..],
            "client addresses",
        ),
        (
            "pg_kinetic_route_in_flight",
            MetricKind::Gauge,
            "1",
            &["route", "scope"][..],
            "client addresses",
        ),
        (
            "pg_kinetic_route_waiting",
            MetricKind::Gauge,
            "1",
            &["route", "scope"][..],
            "client addresses",
        ),
        (
            "pg_kinetic_timeout_total",
            MetricKind::Counter,
            "1",
            &["kind"][..],
            "Timeouts by kind",
        ),
        (
            "pg_kinetic_buffer_limit_total",
            MetricKind::Counter,
            "1",
            &["kind"][..],
            "Buffer limit breaches by kind",
        ),
        (
            "pg_kinetic_tls_handshakes_total",
            MetricKind::Counter,
            "1",
            &["scope", "mode"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_tls_failures_total",
            MetricKind::Counter,
            "1",
            &["scope", "mode", "reason"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_auth_attempts_total",
            MetricKind::Counter,
            "1",
            &["mode"][..],
            "Authentication attempts",
        ),
        (
            "pg_kinetic_auth_failures_total",
            MetricKind::Counter,
            "1",
            &["mode", "reason"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_config_reload_total",
            MetricKind::Counter,
            "1",
            &["outcome"][..],
            "reload decisions",
        ),
        (
            "pg_kinetic_drain_state",
            MetricKind::Gauge,
            "1",
            &["state"][..],
            "drain lifecycle states",
        ),
        (
            "pg_kinetic_health_status",
            MetricKind::Gauge,
            "1",
            &["kind", "status"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_socket_option_total",
            MetricKind::Counter,
            "1",
            &["socket", "option", "outcome"][..],
            "bounded enums",
        ),
        (
            "pg_kinetic_benchmark_latency_ms",
            MetricKind::Histogram,
            "ms",
            &["scenario", "target", "workload", "driver", "metric"][..],
            "benchmark configuration enums",
        ),
        (
            "pg_kinetic_benchmark_throughput_qps",
            MetricKind::Histogram,
            "qps",
            &["scenario", "target", "workload", "driver"][..],
            "benchmark configuration enums",
        ),
        (
            "pg_kinetic_benchmark_errors_total",
            MetricKind::Counter,
            "1",
            &["scenario", "target", "workload", "driver", "outcome"][..],
            "benchmark configuration enums",
        ),
        (
            "pg_kinetic_performance_budget_status",
            MetricKind::Gauge,
            "1",
            &["metric", "outcome"][..],
            "performance enums",
        ),
        (
            "pg_kinetic_process_cpu_seconds",
            MetricKind::Gauge,
            "s",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_process_resident_memory_bytes",
            MetricKind::Gauge,
            "By",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_cpu_per_query",
            MetricKind::Gauge,
            "s",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_memory_per_client_bytes",
            MetricKind::Gauge,
            "By",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_protocol_buffer_copies_total",
            MetricKind::Gauge,
            "1",
            &["feature"][..],
            "fixed implementation paths",
        ),
        (
            "pg_kinetic_pool_checkout_lock_wait_ms",
            MetricKind::Histogram,
            "ms",
            &["outcome"][..],
            "checkout completion states",
        ),
        (
            "pg_kinetic_prepared_cache_hits_total",
            MetricKind::Gauge,
            "1",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_prepared_cache_misses_total",
            MetricKind::Gauge,
            "1",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_observability_hot_path_allocations_total",
            MetricKind::Gauge,
            "1",
            &["feature"][..],
            "fixed implementation paths",
        ),
        (
            "pg_kinetic_idle_clients",
            MetricKind::Gauge,
            "1",
            &[][..],
            "Single gauge without labels.",
        ),
        (
            "pg_kinetic_protocol_phase_duration_ms",
            MetricKind::Histogram,
            "ms",
            &["phase", "outcome"][..],
            "protocol enums",
        ),
    ];

    assert_eq!(catalog.len(), expected.len());

    for (name, kind, unit, expected_labels, _note_fragment) in expected {
        let descriptor = catalog
            .iter()
            .find(|candidate| candidate.name == name)
            .unwrap_or_else(|| panic!("missing metric descriptor for {name}"));

        assert!(
            seen_names.insert(descriptor.name),
            "duplicate metric descriptor for {name}"
        );
        assert_eq!(descriptor.kind, kind, "wrong kind for {name}");
        assert_eq!(descriptor.unit, unit, "wrong unit for {name}");
        assert_eq!(
            label_names(descriptor),
            expected_labels,
            "wrong labels for {name}"
        );
        assert!(
            !descriptor.cardinality_note.is_empty(),
            "missing cardinality note for {name}"
        );
        assert!(
            !descriptor.description.is_empty(),
            "missing description for {name}"
        );
    }
}

#[test]
fn route_labels_do_not_include_raw_client_addresses() {
    let route = RouteKey::new(
        "postgres",
        "pgkinetic",
        Some("api-a"),
        Some("127.0.0.1:5432".parse().expect("socket address")),
        QueryClass::Default,
    );

    assert_eq!(route.metric_label(), "postgres/pgkinetic/api-a/default");
    assert!(!route.metric_label().contains("127.0.0.1"));
    assert!(!route.metric_label().contains("5432"));
}

#[test]
fn catalog_never_exposes_sql_text_labels() {
    let forbidden_labels = [
        "client_addr",
        "client_address",
        "password",
        "bind",
        "certificate",
        "tenant",
        "policy_expression",
        "query",
        "query_text",
        "sql",
        "sql_text",
        "statement",
    ];

    for descriptor in metric_catalog() {
        for label in descriptor.labels {
            let label_name = label.as_str();
            assert!(
                !forbidden_labels.contains(&label_name),
                "unexpected SQL-text-like label {label_name} on {}",
                descriptor.name
            );
        }
    }
}

fn label_names(
    descriptor: &pg_kinetic::core::observability::MetricDescriptor,
) -> Vec<&'static str> {
    descriptor
        .labels
        .iter()
        .map(|label| label.as_str())
        .collect()
}
