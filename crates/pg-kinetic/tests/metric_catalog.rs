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
            &["outcome"][..],
            "Outcome splits successful, timeout, and canceled waits.",
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
