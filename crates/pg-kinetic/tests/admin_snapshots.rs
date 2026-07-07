use std::{path::PathBuf, time::Duration};

use pg_kinetic::{
    config::Config,
    core::observability::MetricOutcome,
    core::session::PinReason,
    proxy_runtime::snapshot::{
        BackpressureSnapshot, LimitsSnapshot, PinningSnapshot, PoolSnapshot, PreparedSnapshot,
        RecoverySnapshot, RouteSnapshot, ServerSnapshot, SettingsSnapshot, SnapshotStore,
    },
    recovery::{RecoveryAction, RecoveryTrigger},
    route::{QueryClass, RouteKey},
};

fn route_key() -> RouteKey {
    RouteKey::new(
        "billing",
        "reporter",
        Some("dashboard"),
        Some("127.0.0.1:6100".parse().expect("client addr")),
        QueryClass::Default,
    )
}

#[test]
fn clients_can_be_registered_and_removed() {
    let store = SnapshotStore::new();
    let handle = store.client_handle();

    handle.register(7);

    let client = store.client_snapshots();
    assert_eq!(client.len(), 1);
    assert_eq!(client[0].client_id, 7);
    assert_eq!(handle.remove(7).expect("client removed").client_id, 7);
    assert!(store.client_snapshots().is_empty());
}

#[test]
fn snapshots_cover_pool_server_prepared_pinning_recovery_and_backpressure() {
    let store = SnapshotStore::new();
    let route_key = route_key();

    let pool = PoolSnapshot {
        configured_backends: 12,
        active_backends: 5,
        idle_backends: 7,
        waiting_clients: 2,
    };
    store.pool_handle().set(pool.clone());

    let mut server = ServerSnapshot::new(42, "active", Duration::from_secs(9));
    server.route_key = Some(route_key.clone());
    server.in_transaction = true;
    store.set_server_snapshot(server.clone());

    let prepared_handle = store.prepared_handle();
    prepared_handle.set(PreparedSnapshot::new(3, 1));
    prepared_handle.increment_statement_count();
    prepared_handle.increment_materialization_count();

    let pinning = PinningSnapshot::new(
        7,
        Some(42),
        Some(route_key.clone()),
        PinReason::OpenTransaction,
        Duration::from_secs(4),
    );
    store.set_pinning_snapshot(pinning.clone());

    let recovery_handle = store.recovery_handle();
    recovery_handle.record(
        RecoveryTrigger::AbandonedResponse,
        RecoveryAction::DrainAndSync,
        MetricOutcome::Timeout,
    );
    recovery_handle.record(
        RecoveryTrigger::AbandonedResponse,
        RecoveryAction::DrainAndSync,
        MetricOutcome::Timeout,
    );
    recovery_handle.set_last_error(
        RecoveryTrigger::AbandonedResponse,
        RecoveryAction::DrainAndSync,
        MetricOutcome::Timeout,
        "backend closed unexpectedly",
    );

    let backpressure_handle = store.backpressure_handle();
    backpressure_handle.set_route(route_key.clone(), 3, 2);
    backpressure_handle.increment_rejected(route_key.clone());
    backpressure_handle.increment_timed_out(route_key.clone());
    backpressure_handle.increment_canceled(route_key.clone());

    store.set_route_snapshot(RouteSnapshot {
        route_key: route_key.clone(),
        client_count: 4,
        backend_count: 2,
    });

    assert_eq!(store.pool_snapshot(), pool);
    assert_eq!(store.server_snapshots(), vec![server]);
    assert_eq!(store.prepared_snapshot(), PreparedSnapshot::new(4, 2));
    assert_eq!(store.pinning_snapshots(), vec![pinning]);
    assert_eq!(
        store.recovery_snapshots(),
        vec![RecoverySnapshot {
            trigger: RecoveryTrigger::AbandonedResponse,
            action: RecoveryAction::DrainAndSync,
            outcome: MetricOutcome::Timeout,
            count: 2,
            last_error: Some(String::from("backend closed unexpectedly")),
        }]
    );
    assert_eq!(
        store.backpressure_snapshots(),
        vec![BackpressureSnapshot {
            route_key: route_key.clone(),
            waiting: 3,
            in_flight: 2,
            rejected: 1,
            timed_out: 1,
            canceled: 1,
        }]
    );
    assert_eq!(
        store.route_snapshots(),
        vec![RouteSnapshot {
            route_key,
            client_count: 4,
            backend_count: 2,
        }]
    );
}

#[test]
fn settings_and_limits_snapshots_do_not_expose_secrets() {
    let mut config = Config::default();
    config.connection.listen_addr = "127.0.0.1:7000".parse().expect("listen addr");
    config.connection.backend_addr = "127.0.0.1:7001".parse().expect("backend addr");
    config.tls.client_tls_mode = pg_kinetic::config::ClientTlsMode::VerifyClient;
    config.tls.backend_tls_mode = pg_kinetic::config::BackendTlsMode::VerifyFull;
    config.tls.client_cert_path = Some(PathBuf::from("client-cert.pem"));
    config.tls.client_key_path = Some(PathBuf::from("client-key.pem"));
    config.tls.backend_ca_path = Some(PathBuf::from("backend-ca.pem"));
    config.auth.auth_mode = pg_kinetic::config::AuthMode::Trust;
    config.auth.auth_failure_message_mode =
        pg_kinetic::config::AuthFailureMessageMode::Detailed;
    config.auth.auth_users_file = Some(PathBuf::from("auth-users.toml"));
    config.auth.backend_user = Some(String::from("proxy_user"));
    config.auth.backend_password_env_var_name = Some(String::from("PG_KINETIC_BACKEND_PASSWORD"));
    config.tls.backend_server_name = Some(String::from("db.example.internal"));
    config.performance.backend_reset_query = String::from("DISCARD TEMP");
    config.reload.reload_enabled = true;
    config.reload.config_reload_interval_ms = 1_500;
    config.drain.drain_timeout_ms = 9_000;
    config.drain.reject_new_clients_during_drain = true;
    config.health.health_addr = Some("127.0.0.1:9191".parse().expect("health addr"));
    config.health.readiness_backend_check_interval_ms = 333;
    config.health.readiness_timeout_ms = 444;
    config.observability.metrics_addr = Some("127.0.0.1:9292".parse().expect("metrics addr"));
    config.socket.tcp_nodelay = false;
    config.socket.tcp_keepalive = true;
    config.socket.tcp_keepalive_idle_ms = Some(1_111);
    config.socket.tcp_keepalive_interval_ms = Some(2_222);
    config.socket.tcp_keepalive_retries = Some(3);
    config.socket.tcp_user_timeout_ms = Some(4_444);
    config.socket.tcp_send_buffer_bytes = Some(5_555);
    config.socket.tcp_recv_buffer_bytes = Some(6_666);
    config.socket.strict_socket_option_mode = true;

    let settings = SettingsSnapshot::from_config(&config);
    let limits = LimitsSnapshot::from_config(&config);
    let debug = format!("{settings:?} {limits:?}");

    assert_eq!(settings.backend_user.as_deref(), Some("proxy_user"));
    assert_eq!(limits.max_clients, config.capacity.max_clients);
    assert_eq!(limits.max_backends, config.capacity.max_backends);
    assert!(!debug.contains("PG_KINETIC_BACKEND_PASSWORD"));
    assert!(!debug.contains("client-key.pem"));
    assert!(!debug.contains("auth-users.toml"));
    assert!(!debug.contains("client_cert_path"));
    assert!(!debug.contains("backend_password_env_var_name"));
}
