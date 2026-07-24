#![cfg(all(target_os = "linux", feature = "io-uring"))]

use pg_kinetic::{
    config::{Config, ReadRoutingConfig, RouteConfig},
    core::runtime::RuntimeEngine,
    proxy_runtime::io_uring,
};
use pg_kinetic_core::routing::ReadRoutingMode;

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

#[test]
fn io_uring_route_shapes_use_shared_runtime_path() {
    let mut config = io_uring_config();
    let mut route = RouteConfig::from_backend_addr("127.0.0.1:6544".parse().expect("route addr"));
    route.replicas.push(Default::default());
    route.read_routing = ReadRoutingConfig {
        read_routing_mode: ReadRoutingMode::PreferReplica,
        ..ReadRoutingConfig::default()
    };
    config.routes = vec![
        route,
        RouteConfig::from_backend_addr("127.0.0.1:6545".parse().expect("route addr")),
    ];

    let summary = io_uring::session_lifecycle_summary_for_test(config)
        .expect("route shapes use shared runtime path");

    assert_eq!(summary.backend_checkout, "shared_pool");
    assert_eq!(summary.session_lifecycle, "shared_proxy");
}
