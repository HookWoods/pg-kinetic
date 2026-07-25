pub use pg_kinetic_core as core;
pub use pg_kinetic_lab as lab;
pub use pg_kinetic_proxy as proxy_runtime;
pub use pg_kinetic_wire as wire;

pub use pg_kinetic_core::traffic::route;
pub use pg_kinetic_core::{
    cluster::cleanup, cluster::recovery, protocol::pin, protocol::prepare, protocol::session,
    protocol::sql, protocol::virtual_session, traffic::backpressure,
};
pub use pg_kinetic_proxy::{config, observe::metrics, pool, pool::backend, proxy};

pub async fn run(config: config::Config) -> anyhow::Result<()> {
    pg_kinetic_proxy::run(config).await
}

pub fn run_thread_per_core(config: config::Config) -> anyhow::Result<()> {
    pg_kinetic_proxy::run_thread_per_core(config)
}

pub fn run_io_uring(config: config::Config) -> anyhow::Result<()> {
    pg_kinetic_proxy::run_io_uring(config)
}
