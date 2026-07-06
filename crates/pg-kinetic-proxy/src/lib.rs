pub mod auth;
pub mod backend;
pub mod config;
pub mod drain;
pub mod health;
pub mod metrics;
pub mod pool;
pub mod proxy;
pub mod reload;
pub mod socket;
pub mod tls;

pub use reload::{ReloadDecision, ReloadableConfig};

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    metrics::install(metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    proxy::Proxy::new(config).run().await
}
