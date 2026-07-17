pub mod admin;
pub mod auth;
pub mod backend;
pub mod config;
pub mod drain;
pub mod health;
pub mod lifecycle;
pub mod metrics;
pub mod mirror;
pub mod policy;
#[cfg(feature = "policy-wasm")]
pub mod policy_wasm;
pub mod pool;
pub mod proxy;
pub mod reload;
pub mod routing;
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
