pub mod backend;
pub mod config;
pub mod metrics;
pub mod pool;
pub mod proxy;

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    metrics::install(metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    proxy::Proxy::new(config).run().await
}
