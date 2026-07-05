pub mod backend;
pub mod backpressure;
pub mod cleanup;
pub mod config;
pub mod metrics;
pub mod pool;
pub mod prepare;
pub mod proxy;
pub mod recovery;
pub mod session;
pub mod sql;
pub mod virtual_session;
pub mod wire;

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    metrics::install(metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    proxy::Proxy::new(config).run().await
}
