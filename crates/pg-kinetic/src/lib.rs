pub mod config;
pub mod wire;

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    tracing::info!(
        listen_addr = %config.listen_addr,
        backend_addr = %config.backend_addr,
        "pg-kinetic configured"
    );
    Ok(())
}
