pub mod auth;
pub mod cancel;
pub mod config;
pub mod engine;
pub mod net;
pub mod observe;
pub mod ops;
pub mod pool;
pub mod proxy;
pub mod query_stats;
pub mod routing;

pub use observe::health::{EndpointHealthProbe, EndpointHealthSnapshot};
pub use ops::reload::ReloadDecision;

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    config.validate().map_err(anyhow::Error::msg)?;
    observe::metrics::install(observe::metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    proxy::Proxy::new(config).run().await
}

pub fn run_thread_per_core(config: config::Config) -> anyhow::Result<()> {
    config.validate().map_err(anyhow::Error::msg)?;
    observe::metrics::install(observe::metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    proxy::Proxy::new(config).run_thread_per_core()
}

pub fn run_io_uring(config: config::Config) -> anyhow::Result<()> {
    engine::io_uring::run(config)
}
