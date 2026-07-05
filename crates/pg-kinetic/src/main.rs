use anyhow::Context;
use pg_kinetic::config::Config;
use tracing_subscriber::{fmt, EnvFilter};

fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let config = Config::parse_args();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?
        .block_on(pg_kinetic::run(config))
        .context("pg-kinetic runtime failed")
}
