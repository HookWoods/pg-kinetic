use pg_kinetic_core::{
    admin::{AdminColumn, AdminColumnType, AdminCommand, AdminView},
    observability::{MetricName, ProtocolPhase, TraceEvent},
};

#[test]
fn admin_view_labels_are_stable() {
    assert_eq!(AdminView::Clients.as_str(), "clients");
    assert_eq!(AdminView::Pools.as_str(), "pools");
    assert_eq!(AdminView::Backpressure.as_str(), "backpressure");
    assert_eq!(AdminCommand::Show(AdminView::Prepared).view(), Some(AdminView::Prepared));
}

#[test]
fn admin_column_schema_is_typed() {
    let column = AdminColumn::new("client_id", AdminColumnType::Int8);
    assert_eq!(column.name(), "client_id");
    assert_eq!(column.column_type(), AdminColumnType::Int8);
}

#[test]
fn observability_labels_are_stable() {
    assert_eq!(ProtocolPhase::Startup.as_str(), "startup");
    assert_eq!(ProtocolPhase::BackendCheckout.as_str(), "backend_checkout");
    assert_eq!(TraceEvent::ClientAccepted.as_str(), "client_accepted");
    assert_eq!(
        MetricName::ProtocolPhaseDuration.as_str(),
        "pg_kinetic_protocol_phase_duration_ms"
    );
}
