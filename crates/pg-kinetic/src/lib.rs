pub mod backpressure;
pub mod config;
pub mod prepare;
pub mod proxy;
pub mod session;
pub mod wire;

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    proxy::Proxy::new(config).run().await
}
