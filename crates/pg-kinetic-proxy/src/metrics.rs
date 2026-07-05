use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
use pg_kinetic_core::{
    cleanup::CleanupAction,
    constants::{MetricName, PreparedEvent},
};
use pg_kinetic_core::{
    recovery::{RecoveryAction, RecoveryTrigger},
    virtual_session::PinReason,
};
use pg_kinetic_wire::sqlstate::SqlState;

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub listen_addr: Option<SocketAddr>,
}

pub fn install(config: MetricsConfig) -> anyhow::Result<()> {
    if let Some(addr) = config.listen_addr {
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()
            .map_err(|error| anyhow::anyhow!("install prometheus exporter: {error}"))?;
        tracing::info!(%addr, "metrics listener enabled");
    }

    describe_metrics();
    Ok(())
}

pub fn record_pool_checkout(wait_ms: f64, outcome: &'static str) {
    metrics_crate::histogram!(
        MetricName::PoolCheckoutWaitMs.as_str(),
        "outcome" => outcome
    )
    .record(wait_ms);
}

pub fn increment_client_connections() {
    metrics_crate::counter!(MetricName::ClientConnectionsTotal.as_str()).increment(1);
}

pub fn increment_prepared_event(event: PreparedEvent) {
    metrics_crate::counter!(
        MetricName::PreparedEventsTotal.as_str(),
        "event" => event.as_str()
    )
    .increment(1);
}

pub fn increment_pin(reason: PinReason) {
    metrics_crate::counter!(
        MetricName::BackendPinTotal.as_str(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

pub fn increment_cleanup(action: CleanupAction) {
    metrics_crate::counter!(
        MetricName::BackendCleanupTotal.as_str(),
        "action" => action.metric_label()
    )
    .increment(1);
}

pub fn increment_recovery(trigger: RecoveryTrigger, action: RecoveryAction, outcome: &'static str) {
    metrics_crate::counter!(
        MetricName::BackendRecoveryTotal.as_str(),
        "trigger" => trigger.metric_label(),
        "action" => action.metric_label(),
        "outcome" => outcome
    )
    .increment(1);
}

pub fn increment_sqlstate(sqlstate: SqlState) {
    metrics_crate::counter!(
        MetricName::BackendSqlstateTotal.as_str(),
        "sqlstate" => sqlstate.as_str().to_string()
    )
    .increment(1);
}

fn describe_metrics() {
    metrics_crate::describe_counter!(
        MetricName::ClientConnectionsTotal.as_str(),
        "Total accepted client connections"
    );
    metrics_crate::describe_histogram!(
        MetricName::PoolCheckoutWaitMs.as_str(),
        "Backend checkout wait time in milliseconds"
    );
    metrics_crate::describe_counter!(
        MetricName::PreparedEventsTotal.as_str(),
        "Prepared statement virtualization events"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendPinTotal.as_str(),
        "Backend pin decisions by reason"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendCleanupTotal.as_str(),
        "Backend cleanup decisions by action"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendRecoveryTotal.as_str(),
        "Backend recovery attempts by trigger, action, and outcome"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendSqlstateTotal.as_str(),
        "Backend ErrorResponse counts by SQLSTATE"
    );
}
