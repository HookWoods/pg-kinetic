use pg_kinetic_core::constants::{
    BufferDefaults, MetricName, PreparedEvent, QosDefaults, TimeoutDefaults,
};

#[test]
fn qos_defaults_are_named_constants() {
    assert_eq!(QosDefaults::MAX_ROUTE_IN_FLIGHT, 100);
    assert_eq!(QosDefaults::MAX_ROUTE_WAITERS, 1_000);
    assert_eq!(TimeoutDefaults::QUERY_TIMEOUT_MS, 30_000);
    assert_eq!(BufferDefaults::MAX_BACKEND_BUFFER_BYTES, 4_194_304);
}

#[test]
fn metric_and_event_labels_are_named() {
    assert_eq!(
        MetricName::BackpressureEvents.as_str(),
        "pg_kinetic_backpressure_events_total"
    );
    assert_eq!(PreparedEvent::Materialize.as_str(), "materialize");
}
