use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        AuthMode, BackendTlsMode, ClientTlsMode, Config, PoolConfig, ReadRoutingConfig, RouteConfig,
    },
    core::runtime::RuntimeEngine,
    proxy_runtime::io_uring,
    wire::protocol::ProtocolVersion,
};
use pg_kinetic_core::routing::ReadRoutingMode;

fn io_uring_config() -> Config {
    let mut config = Config::default();
    config.runtime.engine.runtime_engine = RuntimeEngine::ExperimentalIoUring;
    config.runtime.engine.experimental_runtime_enabled = true;
    config
}

#[test]
fn io_uring_accepts_current_plain_pass_through_boundary() {
    let config = io_uring_config();

    io_uring::validate_supported_config_for_test(&config)
        .expect("plain pass-through io_uring boundary remains supported");
}

#[test]
fn io_uring_accepts_single_primary_route_boundary() {
    let mut config = io_uring_config();
    config.routes = vec![RouteConfig::from_backend_addr(
        "127.0.0.1:6544".parse().expect("route addr"),
    )];

    let resolved = io_uring::direct_backend_addr_for_test(&config).expect("route is supported");

    assert_eq!(resolved.to_string(), "127.0.0.1:6544");
}

#[test]
fn io_uring_resolves_startup_backend_through_shared_runtime_state() {
    let mut config = io_uring_config();
    config.routes = vec![RouteConfig::from_backend_addr(
        "127.0.0.1:6544".parse().expect("route addr"),
    )];
    let startup_packet = startup_packet("postgres", "pgkinetic");

    let resolved = io_uring::startup_backend_addr_for_test(
        config,
        &startup_packet,
        "127.0.0.1:54321".parse().expect("client addr"),
    )
    .expect("startup route resolves through proxy runtime state");

    assert_eq!(resolved.to_string(), "127.0.0.1:6544");
}

#[test]
fn io_uring_prepares_backend_startup_from_shared_runtime_plan() {
    let mut config = io_uring_config();
    config.routes = vec![RouteConfig::from_backend_addr(
        "127.0.0.1:6544".parse().expect("route addr"),
    )];
    let startup_packet = startup_packet("postgres", "pgkinetic");

    let plan = io_uring::startup_backend_plan_for_test(
        config,
        &startup_packet,
        "127.0.0.1:54321".parse().expect("client addr"),
    )
    .expect("startup plan resolves through shared runtime state");

    assert_eq!(plan.backend_addr.to_string(), "127.0.0.1:6544");
    assert_eq!(plan.backend_startup_packet, startup_packet);
}

#[test]
fn io_uring_prepares_proxy_capacity_slots_from_shared_runtime_state() {
    let mut config = io_uring_config();
    config.capacity.max_clients = 17;
    config.capacity.max_backends = 19;

    let limits = io_uring::shared_capacity_limits_for_test(config)
        .expect("io_uring prepares shared proxy capacity slots");

    assert_eq!(limits, (17, 19));
}

#[test]
fn io_uring_rejects_tls_until_tls_stream_adapter_exists() {
    let mut config = io_uring_config();
    config.tls.client_tls_mode = ClientTlsMode::Require;

    let error = io_uring::validate_supported_config_for_test(&config)
        .expect_err("TLS requires a monoio-compatible TLS adapter");

    assert!(error.to_string().contains("client_tls_mode=disable"));
}

#[test]
fn io_uring_rejects_backend_tls_until_semantic_runtime_exists() {
    let mut config = io_uring_config();
    config.tls.backend_tls_mode = BackendTlsMode::Require;

    let error = io_uring::validate_supported_config_for_test(&config)
        .expect_err("backend TLS is not supported yet");

    assert!(error.to_string().contains("backend_tls_mode=disable"));
}

#[test]
fn io_uring_accepts_auth_modes_through_shared_session_lifecycle() {
    let mut config = io_uring_config();
    config.auth.auth_mode = AuthMode::Trust;

    io_uring::validate_supported_config_for_test(&config)
        .expect("auth modes use shared session lifecycle");
}

#[test]
fn io_uring_accepts_multiple_routes_through_shared_route_selection() {
    let mut config = io_uring_config();
    config.routes = vec![
        RouteConfig::from_backend_addr("127.0.0.1:6544".parse().expect("route addr")),
        RouteConfig::from_backend_addr("127.0.0.1:6545".parse().expect("route addr")),
    ];

    io_uring::validate_supported_config_for_test(&config)
        .expect("multiple routes use shared route selection");
}

#[test]
fn io_uring_accepts_replicas_through_shared_route_selection() {
    let mut config = io_uring_config();
    let mut route = RouteConfig::from_backend_addr("127.0.0.1:6544".parse().expect("route addr"));
    route.replicas.push(Default::default());
    config.routes = vec![route];

    io_uring::validate_supported_config_for_test(&config)
        .expect("replicas use shared route selection");
}

#[test]
fn io_uring_accepts_read_routing_through_shared_planner() {
    let mut config = io_uring_config();
    let mut route = RouteConfig::from_backend_addr("127.0.0.1:6544".parse().expect("route addr"));
    route.read_routing = ReadRoutingConfig {
        read_routing_mode: ReadRoutingMode::PreferReplica,
        ..ReadRoutingConfig::default()
    };
    config.routes = vec![route];

    io_uring::validate_supported_config_for_test(&config)
        .expect("read routing uses shared planner");
}

#[test]
fn io_uring_accepts_pool_configs_through_shared_checkout() {
    let mut config = io_uring_config();
    config.pools = vec![PoolConfig {
        database: "app".to_string(),
        user: "app".to_string(),
        backend_addr: "127.0.0.1:6544".parse().expect("pool addr"),
        max_backends: Some(2),
    }];

    io_uring::validate_supported_config_for_test(&config)
        .expect("pool configs use shared checkout");
}

fn startup_packet(user: &str, database: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.put_u8(0);
    body.put_u8(0);

    let mut packet = BytesMut::with_capacity(body.len() + 4);
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet
}
