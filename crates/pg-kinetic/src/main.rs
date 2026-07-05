use anyhow::Context;
use pg_kinetic::config::Config;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let config = Config::parse_args();
    pg_kinetic::run(config)
        .await
        .context("pg-kinetic runtime failed")
}
