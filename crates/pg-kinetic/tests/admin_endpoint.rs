use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, BackendTlsMode, CapacityConfig, Config,
        ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig, PerformanceConfig,
        QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    core::{observability::MetricOutcome, prepare::PreparedStatementSnapshot, session::PinReason},
    proxy::Proxy,
    proxy_runtime::snapshot::{
        ClientSnapshot, PinningSnapshot, PoolSnapshot, PreparedSnapshot, RouteSnapshot,
        ServerSnapshot, SnapshotStore,
    },
    recovery::{RecoveryAction, RecoveryTrigger},
    route::{QueryClass, RouteKey},
    wire::{
        backend::{parse_backend_frame, BackendFrame, ReadyStatus},
        protocol::ProtocolVersion,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[tokio::test]
async fn admin_listener_is_disabled_by_default() {
    let admin_addr = free_port().await;
    let backend_addr = free_port().await;
    let (run_handle, _, _) = spawn_proxy(test_config(None, None, backend_addr)).await;

    let connect = time::timeout(Duration::from_millis(200), TcpStream::connect(admin_addr)).await;
    match connect {
        Err(_) => {}
        Ok(result) => assert!(result.is_err(), "expected no admin listener"),
    }

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn admin_listener_accepts_startup_without_backend_connection() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, _) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let mut stream = TcpStream::connect(admin_addr).await.expect("connect admin");
    stream
        .write_all(&startup_packet("admin"))
        .await
        .expect("startup");

    let frames = read_until_ready(&mut stream).await;
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn admin_listener_rejects_non_admin_users() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, _) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let mut stream = TcpStream::connect(admin_addr).await.expect("connect admin");
    stream
        .write_all(&startup_packet("postgres"))
        .await
        .expect("startup");

    let frames = read_until_ready(&mut stream).await;
    let error = frames
        .iter()
        .find(|frame| frame.tag == b'E')
        .expect("error response");
    assert!(error_message(error)
        .expect("error message")
        .contains("admin access restricted"));
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn unknown_command_returns_error_response() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, _) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let mut stream = TcpStream::connect(admin_addr).await.expect("connect admin");
    stream
        .write_all(&startup_packet("admin"))
        .await
        .expect("startup");
    let _ = read_until_ready(&mut stream).await;

    stream
        .write_all(&query_packet("SELECT 1"))
        .await
        .expect("query");

    let frames = read_until_ready(&mut stream).await;
    let error = frames
        .iter()
        .find(|frame| frame.tag == b'E')
        .expect("error response");
    assert_eq!(error.sqlstate().map(|state| state.as_str()), Some("0A000"));
    assert!(error_message(error)
        .expect("error message")
        .contains("unsupported admin command"));
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn show_performance_refreshes_runtime_data_with_numeric_float_defaults() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, snapshot_store) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let frames = admin_query(admin_addr, "SHOW PERFORMANCE").await;
    let data_rows = frames
        .iter()
        .filter(|frame| frame.tag == b'D')
        .map(data_row_values)
        .collect::<Vec<_>>();
    assert_eq!(data_rows.len(), 1);

    for index in [3, 4, 8, 9, 10, 11, 13] {
        assert_ne!(data_rows[0][index], "unknown");
    }
    assert!(snapshot_store
        .performance_snapshot()
        .process_sample
        .is_some());
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn show_clients_pools_and_servers_return_stable_columns() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, snapshot_store) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let mut client = ClientSnapshot::new(7);
    client.user = Some(String::from("reporter"));
    client.database = Some(String::from("billing"));
    client.application_name = Some(String::from("dashboard"));
    client.route_key = Some(route_key());
    client.state = String::from("active");
    client.connected_duration = Duration::from_secs(3);
    snapshot_store.register_client(client);

    snapshot_store.set_pool_snapshot(PoolSnapshot {
        configured_backends: 12,
        active_backends: 5,
        idle_backends: 7,
        waiting_clients: 2,
    });

    let mut server = ServerSnapshot::new(42, "active", Duration::from_secs(9));
    server.route_key = Some(route_key());
    server.in_transaction = true;
    snapshot_store.set_server_snapshot(server);

    let client_frames = admin_query(admin_addr, "SHOW CLIENTS").await;
    assert_admin_table_response(
        &client_frames,
        &[
            "client_id",
            "user",
            "database",
            "application_name",
            "route_key",
            "state",
            "connected_duration_ms",
            "current_target_role",
            "required_session_write_lsn",
        ],
        &[vec![
            "7",
            "reporter",
            "billing",
            "dashboard",
            "billing/reporter/dashboard/default",
            "active",
            "3000",
            "<none>",
            "<none>",
        ]],
    );

    let pool_frames = admin_query(admin_addr, "SHOW POOLS").await;
    assert_admin_table_response(
        &pool_frames,
        &[
            "route_key",
            "max_backends",
            "active_backends",
            "idle_backends",
            "waiting_clients",
            "checkout_lock_wait_ms",
        ],
        &[vec!["global", "12", "5", "7", "2", "0.000"]],
    );

    let server_frames = admin_query(admin_addr, "SHOW SERVERS").await;
    assert_admin_table_response(
        &server_frames,
        &[
            "backend_id",
            "route_key",
            "state",
            "last_checkout_age_ms",
            "in_transaction",
            "endpoint_role",
            "detected_role",
            "health",
            "lag_ms",
            "replay_lsn",
            "last_probe_age_ms",
        ],
        &[vec![
            "42",
            "billing/reporter/dashboard/default",
            "active",
            "9000",
            "true",
            "<none>",
            "<none>",
            "<none>",
            "<none>",
            "<none>",
            "<none>",
        ]],
    );

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn show_prepared_pinning_recovery_backpressure_and_routes_return_stable_columns() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, snapshot_store) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let route_key = route_key();
    let route_key_text = "billing/reporter/dashboard/default";

    let prepared_handle = snapshot_store.prepared_handle();
    prepared_handle.set(PreparedSnapshot::new(3, 1).with_statements(vec![
        PreparedStatementSnapshot {
            session_id: 11,
            client_statement_name: String::from("stmt_a"),
            backend_statement_name: String::from("pgk_11_1"),
            materialized_backend_count: 2,
            invalidation_count: 1,
        },
    ]));
    prepared_handle.increment_statement_count();
    prepared_handle.increment_materialization_count();

    snapshot_store.set_pinning_snapshot(PinningSnapshot::new(
        7,
        Some(42),
        Some(route_key.clone()),
        PinReason::OpenTransaction,
        Duration::from_secs(4),
    ));

    let recovery_handle = snapshot_store.recovery_handle();
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

    let backpressure_handle = snapshot_store.backpressure_handle();
    backpressure_handle.set_route(route_key.clone(), 3, 2);
    backpressure_handle.increment_rejected(route_key.clone());
    backpressure_handle.increment_timed_out(route_key.clone());
    backpressure_handle.increment_canceled(route_key.clone());

    snapshot_store.set_route_snapshot(RouteSnapshot {
        route_key: route_key.clone(),
        client_count: 4,
        backend_count: 2,
    });

    let prepared_frames = admin_query(admin_addr, "SHOW PREPARED").await;
    assert_admin_table_response(
        &prepared_frames,
        &[
            "session_id",
            "client_statement_name",
            "backend_statement_name",
            "materialized_backend_count",
            "invalidation_count",
            "prepared_cache_hits",
            "prepared_cache_misses",
        ],
        &[vec!["11", "stmt_a", "pgk_11_1", "2", "1", "0", "0"]],
    );

    let pinning_frames = admin_query(admin_addr, "SHOW PINNING").await;
    assert_admin_table_response(
        &pinning_frames,
        &[
            "client_id",
            "backend_id",
            "route_key",
            "reason",
            "duration_ms",
        ],
        &[vec!["7", "42", route_key_text, "open_transaction", "4000"]],
    );

    let recovery_frames = admin_query(admin_addr, "SHOW RECOVERY").await;
    assert_admin_table_response(
        &recovery_frames,
        &["trigger", "action", "outcome", "count", "last_error"],
        &[vec![
            "abandoned_response",
            "drain_and_sync",
            "timeout",
            "2",
            "backend closed unexpectedly",
        ]],
    );

    let backpressure_frames = admin_query(admin_addr, "SHOW BACKPRESSURE").await;
    assert_admin_table_response(
        &backpressure_frames,
        &[
            "route_key",
            "waiting",
            "in_flight",
            "rejected",
            "timed_out",
            "canceled",
        ],
        &[vec![route_key_text, "3", "2", "1", "1", "1"]],
    );

    let routes_frames = admin_query(admin_addr, "SHOW ROUTES").await;
    assert_admin_table_response(
        &routes_frames,
        &[
            "database",
            "user",
            "application_name",
            "query_class",
            "client_count",
            "backend_count",
            "primary_count",
            "replica_count",
            "read_routing_mode",
            "fallback_policy",
            "freshness_policy",
            "read_after_write_timeout_ms",
            "route_map_generation_id",
            "sharding_enabled",
        ],
        &[vec![
            "billing",
            "reporter",
            "dashboard",
            "default",
            "4",
            "2",
            "0",
            "0",
            "off",
            "primary",
            "session_write_lsn",
            "0",
            "0",
            "false",
        ]],
    );

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn show_settings_and_limits_keep_secrets_out() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let mut config = test_config(Some(admin_addr), Some("admin"), backend_addr);
    config.tls.client_cert_path = Some(PathBuf::from("client-cert.pem"));
    config.tls.client_key_path = Some(PathBuf::from("client-key.pem"));
    config.tls.client_ca_path = Some(PathBuf::from("client-ca.pem"));
    config.tls.backend_ca_path = Some(PathBuf::from("backend-ca.pem"));
    config.tls.backend_server_name = Some(String::from("db.example.internal"));
    config.performance.backend_reset_query = String::from("DISCARD TEMP");
    config.admin.admin_query_timeout_ms = 75;
    config.admin.admin_max_clients = 9;

    let backend_addr_text = backend_addr.to_string();
    let (run_handle, listen_addr, _) = spawn_proxy(config).await;
    let listen_addr_text = listen_addr.to_string();

    let settings_frames = admin_query(admin_addr, "SHOW SETTINGS").await;
    assert_admin_table_response(
        &settings_frames,
        &[
            "listen_addr",
            "backend_addr",
            "client_tls_mode",
            "backend_tls_mode",
            "auth_mode",
            "auth_failure_message_mode",
            "backend_user",
            "backend_reset_query",
            "recovery_mode",
            "reload_enabled",
            "config_reload_interval_ms",
            "drain_timeout_ms",
            "reject_new_clients_during_drain",
            "health_addr",
            "readiness_backend_check_interval_ms",
            "readiness_timeout_ms",
            "metrics_addr",
            "tcp_nodelay",
            "tcp_keepalive",
            "tcp_keepalive_idle_ms",
            "tcp_keepalive_interval_ms",
            "tcp_keepalive_retries",
            "tcp_user_timeout_ms",
            "tcp_send_buffer_bytes",
            "tcp_recv_buffer_bytes",
            "strict_socket_option_mode",
        ],
        &[vec![
            listen_addr_text.as_str(),
            backend_addr_text.as_str(),
            "disable",
            "disable",
            "pass_through",
            "generic",
            "<none>",
            "DISCARD TEMP",
            "recover",
            "false",
            "5000",
            "30000",
            "false",
            "<none>",
            "1000",
            "5000",
            "<none>",
            "true",
            "false",
            "<none>",
            "<none>",
            "<none>",
            "<none>",
            "<none>",
            "<none>",
            "false",
        ]],
    );

    let settings_text = table_text(&settings_frames);
    for secret in [
        "client-cert.pem",
        "client-key.pem",
        "client-ca.pem",
        "backend-ca.pem",
        "db.example.internal",
    ] {
        assert!(
            !settings_text.contains(secret),
            "unexpected secret value in settings output: {secret}"
        );
    }

    let limits_frames = admin_query(admin_addr, "SHOW LIMITS").await;
    assert_admin_table_response(
        &limits_frames,
        &[
            "max_clients",
            "max_backends",
            "max_checkout_waiters",
            "max_route_in_flight",
            "max_route_waiters",
            "checkout_timeout_ms",
            "query_timeout_ms",
            "idle_client_timeout_ms",
            "idle_transaction_timeout_ms",
            "max_client_buffer_bytes",
            "max_backend_buffer_bytes",
            "recovery_timeout_ms",
            "drain_timeout_ms",
            "readiness_backend_check_interval_ms",
            "readiness_timeout_ms",
            "config_reload_interval_ms",
            "admin_query_timeout_ms",
            "admin_max_clients",
            "overload_error_code",
        ],
        &[vec![
            "10", "1", "4", "100", "1000", "250", "30000", "300000", "60000", "1048576", "4194304",
            "1000", "30000", "1000", "5000", "5000", "75", "9", "53300",
        ]],
    );

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

async fn spawn_proxy(config: Config) -> (tokio::task::JoinHandle<()>, SocketAddr, SnapshotStore) {
    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let mut config = config;
    config.connection.listen_addr = listen_addr;

    let proxy = Proxy::new(config);
    let snapshot_store = proxy.snapshot_store();
    let handle = tokio::spawn(async move {
        proxy.run().await.expect("proxy run");
    });
    time::sleep(Duration::from_millis(50)).await;

    (handle, listen_addr, snapshot_store)
}

fn test_config(
    admin_addr: Option<SocketAddr>,
    admin_allowed_user: Option<&str>,
    backend_addr: SocketAddr,
) -> Config {
    Config {
        connection: ConnectionConfig {
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            backend_addr,
        },
        routes: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 1,
            max_checkout_waiters: 4,
        },
        performance: PerformanceConfig {
            checkout_timeout_ms: 250,
            recovery_mode: pg_kinetic::recovery::RecoveryMode::Recover,
            recovery_timeout_ms: 1_000,
            backend_reset_query: String::from("DISCARD ALL"),
        },
        qos: QosConfig {
            max_route_in_flight: 100,
            max_route_waiters: 1_000,
            query_timeout_ms: 30_000,
            idle_client_timeout_ms: 300_000,
            idle_transaction_timeout_ms: 60_000,
            max_client_buffer_bytes: 1_048_576,
            max_backend_buffer_bytes: 4_194_304,
            overload_error_code: String::from("53300"),
        },
        admin: pg_kinetic::config::AdminConfig {
            admin_addr,
            admin_require_tls: false,
            admin_allowed_user: admin_allowed_user.map(str::to_owned),
            admin_query_timeout_ms: 100,
            admin_max_clients: 4,
        },
        observability: ObservabilityConfig {
            metrics_addr: None,
            ..Default::default()
        },
        tls: TlsConfig {
            client_tls_mode: pg_kinetic::config::ClientTlsMode::Disable,
            client_cert_path: None,
            client_key_path: None,
            client_ca_path: None,
            backend_tls_mode: BackendTlsMode::Disable,
            backend_ca_path: None,
            backend_server_name: None,
        },
        auth: AuthConfig {
            auth_mode: pg_kinetic::config::AuthMode::PassThrough,
            auth_users_file: None,
            backend_user: None,
            backend_password_env_var_name: None,
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        },
        reload: ReloadConfig::default(),
        drain: DrainConfig::default(),
        health: HealthConfig::default(),
        socket: SocketConfig::default(),
    }
}

fn route_key() -> RouteKey {
    RouteKey::new(
        "billing",
        "reporter",
        Some("dashboard"),
        Some("127.0.0.1:6100".parse().expect("client addr")),
        QueryClass::Default,
    )
}

async fn spawn_backend_monitor(hits: Arc<AtomicUsize>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = listener.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept backend");
            let hits = Arc::clone(&hits);
            tokio::spawn(async move {
                hits.fetch_add(1, Ordering::SeqCst);
                let mut startup = [0_u8; 1024];
                let _ = stream.read(&mut startup).await.expect("read startup");
                let mut sink = [0_u8; 256];
                let _ = stream.read(&mut sink).await.expect("read follow-up");
            });
        }
    });

    backend_addr
}

async fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port");
    let addr = listener.local_addr().expect("free addr");
    drop(listener);
    addr
}

async fn read_until_ready(stream: &mut TcpStream) -> Vec<BackendFrame> {
    let mut buffer = BytesMut::new();
    let mut frames = Vec::new();

    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            let ready = frame.ready_status();
            frames.push(frame);
            if ready == Some(ReadyStatus::Idle) {
                return frames;
            }
        }

        let mut chunk = [0_u8; 4096];
        match time::timeout(Duration::from_millis(250), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(read)) => buffer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => panic!("read admin response: {error}"),
        }
    }

    frames
}

fn startup_packet(user: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.extend_from_slice(b"\0database\0pgkinetic\0\0");

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet.to_vec()
}

fn query_packet(sql: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.put_u8(0);

    let mut packet = BytesMut::new();
    packet.put_u8(b'Q');
    packet.put_i32((payload.len() + 4) as i32);
    packet.extend_from_slice(&payload);
    packet.to_vec()
}

fn error_message(frame: &BackendFrame) -> Option<&str> {
    if frame.tag != b'E' {
        return None;
    }

    let mut offset = 0;
    while offset < frame.payload.len() {
        let field_kind = frame.payload[offset];
        offset += 1;
        if field_kind == 0 {
            return None;
        }

        let remaining = frame.payload.get(offset..)?;
        let terminator = remaining.iter().position(|byte| *byte == 0)?;
        let value = std::str::from_utf8(&remaining[..terminator]).ok()?;
        if field_kind == b'M' {
            return Some(value);
        }
        offset += terminator + 1;
    }

    None
}

async fn admin_query(admin_addr: SocketAddr, sql: &str) -> Vec<BackendFrame> {
    let mut stream = TcpStream::connect(admin_addr).await.expect("connect admin");
    stream
        .write_all(&startup_packet("admin"))
        .await
        .expect("startup");
    let _ = read_until_ready(&mut stream).await;
    stream.write_all(&query_packet(sql)).await.expect("query");
    read_until_ready(&mut stream).await
}

fn assert_admin_table_response(
    frames: &[BackendFrame],
    expected_columns: &[&str],
    expected_rows: &[Vec<&str>],
) {
    let row_description_frames = frames
        .iter()
        .filter(|frame| frame.tag == b'T')
        .collect::<Vec<_>>();
    assert_eq!(
        row_description_frames.len(),
        1,
        "expected one row description"
    );
    assert_eq!(
        row_description_columns(row_description_frames[0]),
        expected_columns
    );

    let data_rows = frames
        .iter()
        .filter(|frame| frame.tag == b'D')
        .map(data_row_values)
        .collect::<Vec<_>>();
    let expected_rows = expected_rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(data_rows, expected_rows);
    assert!(
        frames.iter().any(|frame| frame.tag == b'C'),
        "expected command complete"
    );
    assert!(
        frames.iter().any(|frame| frame.tag == b'Z'),
        "expected ready for query"
    );
}

fn row_description_columns(frame: &BackendFrame) -> Vec<String> {
    assert_eq!(frame.tag, b'T');
    let payload = frame.payload.as_ref();
    assert!(payload.len() >= 2, "row description payload too short");

    let column_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut columns = Vec::with_capacity(column_count);

    for _ in 0..column_count {
        let name_end = payload[offset..]
            .iter()
            .position(|byte| *byte == 0)
            .expect("column name terminator");
        columns.push(
            std::str::from_utf8(&payload[offset..offset + name_end])
                .expect("column name utf8")
                .to_owned(),
        );
        offset += name_end + 1;
        offset += 4 + 2 + 4 + 2 + 4 + 2;
    }

    columns
}

fn data_row_values(frame: &BackendFrame) -> Vec<String> {
    assert_eq!(frame.tag, b'D');
    let payload = frame.payload.as_ref();
    assert!(payload.len() >= 2, "data row payload too short");

    let column_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut values = Vec::with_capacity(column_count);

    for _ in 0..column_count {
        let length = i32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        offset += 4;
        if length < 0 {
            values.push(String::from("<null>"));
            continue;
        }

        let length = length as usize;
        values.push(
            std::str::from_utf8(&payload[offset..offset + length])
                .expect("data value utf8")
                .to_owned(),
        );
        offset += length;
    }

    values
}

fn table_text(frames: &[BackendFrame]) -> String {
    let mut text = String::new();
    for frame in frames {
        if frame.tag == b'T' {
            text.push_str(&row_description_columns(frame).join("|"));
        }
        if frame.tag == b'D' {
            text.push_str(&data_row_values(frame).join("|"));
        }
    }
    text
}
