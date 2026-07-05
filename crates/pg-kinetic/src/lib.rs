pub use pg_kinetic_core as core;
pub use pg_kinetic_proxy as proxy_runtime;
pub use pg_kinetic_wire as wire;

pub use pg_kinetic_core::route;
pub use pg_kinetic_core::{
    backpressure, cleanup, pin, prepare, recovery, session, sql, virtual_session,
};
pub use pg_kinetic_proxy::{backend, config, metrics, pool, proxy};

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    pg_kinetic_proxy::run(config).await
}
