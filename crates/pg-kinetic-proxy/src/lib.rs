pub mod adaptive;
pub mod admin;
pub mod auth;
pub mod backend;
pub mod benchmark;
pub mod buffers;
pub mod cancel;
pub mod compatibility;
pub mod config;
pub mod control;
pub mod drain;
pub mod health;
pub mod io_uring;
pub mod lifecycle;
pub mod metrics;
pub mod mirror;
pub mod pause;
pub mod policy;
#[cfg(feature = "policy-wasm")]
pub mod policy_wasm;
pub mod pool;
pub mod preflight;
pub mod profile;
pub mod proxy;
pub mod regression;
pub mod reload;
pub mod routing;
pub mod runtime_engine;
pub mod sharding;
pub mod snapshot;
pub mod socket;
pub mod telemetry;
pub mod tls;

pub use health::{EndpointHealthProbe, EndpointHealthSnapshot};
pub use reload::ReloadDecision;

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    metrics::install(metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    proxy::Proxy::new(config).run().await
}

#[cfg(feature = "runtime-experiments")]
pub fn run_thread_per_core(config: config::Config) -> anyhow::Result<()> {
    metrics::install(metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    proxy::Proxy::new(config).run_thread_per_core()
}

pub fn run_io_uring(config: config::Config) -> anyhow::Result<()> {
    io_uring::run(config)
}
