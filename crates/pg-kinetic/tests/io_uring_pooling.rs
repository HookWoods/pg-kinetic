#![cfg(all(target_os = "linux", feature = "io-uring"))]

use pg_kinetic::{config::Config, core::runtime::RuntimeEngine, proxy_runtime::io_uring};

fn io_uring_config() -> Config {
    let mut config = Config::default();
    config.runtime.engine.runtime_engine = RuntimeEngine::ExperimentalIoUring;
    config.runtime.engine.experimental_runtime_enabled = true;
    config
}

#[test]
fn io_uring_accepts_pool_configs_after_shared_pool_checkout() {
    let mut config = io_uring_config();
    config.pools = vec![pg_kinetic::config::PoolConfig {
        database: "app".to_string(),
        user: "app".to_string(),
        backend_addr: "127.0.0.1:6544".parse().expect("pool addr"),
        max_backends: Some(2),
    }];

    io_uring::validate_supported_config_for_test(&config)
        .expect("io_uring supports pool configs after shared checkout");
}
