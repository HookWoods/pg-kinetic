#![cfg(all(target_os = "linux", feature = "io-uring"))]

use pg_kinetic::{config::Config, core::runtime::RuntimeEngine, proxy_runtime::io_uring};

fn io_uring_config() -> Config {
    let mut config = Config::default();
    config.runtime.engine.runtime_engine = RuntimeEngine::ExperimentalIoUring;
    config.runtime.engine.experimental_runtime_enabled = true;
    config
}

#[test]
fn io_uring_session_lifecycle_uses_shared_runtime_path() {
    let summary = io_uring::session_lifecycle_summary_for_test(io_uring_config())
        .expect("io_uring session lifecycle summary");

    assert_eq!(summary.client_transport, "monoio");
    assert_eq!(summary.backend_checkout, "shared_pool");
    assert_eq!(summary.session_lifecycle, "shared_proxy");
}
