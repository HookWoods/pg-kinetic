use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;

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
    metrics::histogram!("pg_kinetic_pool_checkout_wait_ms", "outcome" => outcome).record(wait_ms);
}

pub fn increment_client_connections() {
    metrics::counter!("pg_kinetic_client_connections_total").increment(1);
}

pub fn increment_prepared_event(event: &'static str) {
    metrics::counter!("pg_kinetic_prepared_events_total", "event" => event).increment(1);
}

fn describe_metrics() {
    metrics::describe_counter!(
        "pg_kinetic_client_connections_total",
        "Total accepted client connections"
    );
    metrics::describe_histogram!(
        "pg_kinetic_pool_checkout_wait_ms",
        "Backend checkout wait time in milliseconds"
    );
    metrics::describe_counter!(
        "pg_kinetic_prepared_events_total",
        "Prepared statement virtualization events"
    );
}
