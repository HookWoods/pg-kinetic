use std::{
    collections::HashSet,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use ::metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, CapacityConfig,
        ClientTlsMode, Config, ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig,
        PerformanceConfig, QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    core::{
        ha::{
            EndpointHealth, EndpointRoleState, HealthProbeOutcome, ReplicaLagState,
            RoleProbeOutcome,
        },
        lsn::{FreshnessStatus, PgLsn},
        routing::{BackendRole, FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
    },
    proxy::Proxy,
    proxy_runtime::{
        metrics as proxy_metrics,
        routing::{ReplicaCandidate, RoutingReason, RoutingTarget},
        snapshot::{
            BackpressureSnapshot, ClientSnapshot, ReplicaHealthSnapshot, RouteCheckoutSnapshot,
            RoutePolicySnapshot, RouteSnapshot, ServerSnapshot, SnapshotStore,
        },
    },
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

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();

#[test]
fn route_decision_and_fallback_metrics_use_stable_labels() {
    let recorder = install_metrics_recorder();
    recorder.clear();

    let route = route_key("api-a");
    let store = SnapshotStore::new();

    store.set_route_checkout_snapshot(RouteCheckoutSnapshot::new(
        route.clone(),
        RoutingTarget::Replica {
            candidate: ReplicaCandidate::new(7, true, None, None),
            reason: RoutingReason::ReplicaHint,
        },
        Some(FreshnessStatus::Satisfied),
    ));
    store.set_route_checkout_snapshot(RouteCheckoutSnapshot::new(
        route.clone(),
        RoutingTarget::Primary {
            reason: RoutingReason::FallbackPrimary,
        },
        Some(FreshnessStatus::Stale),
    ));
    proxy_metrics::record_read_after_write_wait(&route, 42.5, FreshnessStatus::Waiting);
    proxy_metrics::increment_read_after_write_rejection(&route, FreshnessStatus::Unavailable);

    let route_label = route.metric_label();
    assert!(recorder.has_metric(
        "pg_kinetic_route_decisions_total",
        &[
            ("route", route_label.as_str()),
            ("target_role", "replica"),
            ("query_class", "default"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_route_fallbacks_total",
        &[
            ("route", route_label.as_str()),
            ("reason", "fallback_primary"),
            ("fallback_policy", "primary"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_read_after_write_wait_ms",
        &[("route", route_label.as_str()), ("outcome", "waiting")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_read_after_write_rejections_total",
        &[("route", route_label.as_str()), ("outcome", "unavailable")],
    ));

    assert_no_sensitive_labels(&recorder);
}

#[test]
fn replica_health_lag_and_split_brain_metrics_use_stable_labels() {
    let recorder = install_metrics_recorder();
    recorder.clear();

    let store = SnapshotStore::new();
    let endpoint_addr: SocketAddr = "10.0.0.5:5432".parse().expect("socket address");
    let mut snapshot = ReplicaHealthSnapshot::new(7, endpoint_addr, BackendRole::Replica);
    snapshot.health = HealthProbeOutcome::new(EndpointHealth::Healthy, false, 0);
    snapshot.role = RoleProbeOutcome::new(EndpointRoleState::Replica, None);
    snapshot.replay_lsn = Some(PgLsn::from_parts(2, 16));
    snapshot.replay_timestamp = Some(std::time::SystemTime::now());
    snapshot.lag_duration = Some(Duration::from_millis(125));
    snapshot.lag_state = ReplicaLagState::Fresh;
    store.set_replica_health_snapshot(snapshot.clone());

    proxy_metrics::record_split_brain_warning(snapshot.endpoint_id, snapshot.expected_role);

    assert!(recorder.has_metric(
        "pg_kinetic_replica_health",
        &[("endpoint", "7"), ("health", "healthy")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_replica_lag_ms",
        &[("endpoint", "7"), ("lag_state", "fresh")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_replica_replay_lsn",
        &[("endpoint", "7"), ("target_role", "replica")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_split_brain_warnings_total",
        &[
            ("endpoint", "7"),
            ("target_role", "replica"),
            ("reason", "role_mismatch"),
        ],
    ));

    assert_no_sensitive_labels(&recorder);
}

#[tokio::test]
async fn show_routes_servers_clients_and_backpressure_include_phase_7_columns() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, snapshot_store) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let route = route_key("dashboard");

    snapshot_store.register_client({
        let mut client = ClientSnapshot::new(7);
        client.user = Some(String::from("reporter"));
        client.database = Some(String::from("billing"));
        client.application_name = Some(String::from("dashboard"));
        client.route_key = Some(route.clone());
        client.state = String::from("active");
        client.connected_duration = Duration::from_secs(3);
        client
    });

    snapshot_store.set_route_snapshot(RouteSnapshot {
        route_key: route.clone(),
        client_count: 4,
        backend_count: 2,
    });
    snapshot_store.set_route_policy_snapshot(RoutePolicySnapshot {
        route_key: route.clone(),
        primary_count: 1,
        replica_count: 1,
        read_routing_mode: ReadRoutingMode::PreferReplica,
        fallback_policy: FallbackPolicy::Wait,
        freshness_policy: FreshnessPolicy::SessionWriteLsnAndMaxLag,
        read_after_write_timeout_ms: 750,
    });

    let mut checkout = RouteCheckoutSnapshot::new(
        route.clone(),
        RoutingTarget::Primary {
            reason: RoutingReason::FallbackPrimary,
        },
        Some(FreshnessStatus::Stale),
    );
    checkout.required_session_write_lsn = Some(PgLsn::from_parts(2, 16));
    snapshot_store.set_route_checkout_snapshot(checkout);

    snapshot_store.set_backpressure_snapshot(BackpressureSnapshot {
        route_key: route.clone(),
        waiting: 3,
        in_flight: 2,
        rejected: 1,
        timed_out: 1,
        canceled: 1,
    });

    let mut server = ServerSnapshot::new(42, "active", Duration::from_secs(9));
    server.route_key = Some(route.clone());
    server.in_transaction = true;
    snapshot_store.set_server_snapshot(server);

    let endpoint_addr: SocketAddr = "10.0.0.5:5432".parse().expect("socket address");
    let mut health = ReplicaHealthSnapshot::new(42, endpoint_addr, BackendRole::Replica);
    health.health = HealthProbeOutcome::new(EndpointHealth::Healthy, false, 0);
    health.role = RoleProbeOutcome::new(EndpointRoleState::Replica, None);
    health.replay_lsn = Some(PgLsn::from_parts(2, 16));
    health.replay_timestamp = Some(std::time::SystemTime::now());
    health.lag_duration = Some(Duration::from_millis(125));
    health.lag_state = ReplicaLagState::Fresh;
    health.last_successful_probe_at = Some(std::time::SystemTime::now() - Duration::from_secs(4));
    snapshot_store.set_replica_health_snapshot(health);

    let clients_frames = admin_query(admin_addr, "SHOW CLIENTS").await;
    assert_admin_table_response(
        &clients_frames,
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
            "postgres/pgkinetic/dashboard/default",
            "active",
            "3000",
            "primary",
            "2/10",
        ]],
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
            "postgres",
            "pgkinetic",
            "dashboard",
            "default",
            "4",
            "2",
            "1",
            "1",
            "prefer_replica",
            "wait",
            "session_write_lsn_and_max_lag",
            "750",
            "0",
            "false",
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
        &[vec![
            "postgres/pgkinetic/dashboard/default",
            "3",
            "2",
            "1",
            "1",
            "1",
        ]],
    );

    let server_frames = admin_query(admin_addr, "SHOW SERVERS").await;
    assert_admin_table_columns(
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
    );
    let server_row = first_data_row(&server_frames);
    assert_eq!(server_row[0], "42");
    assert_eq!(server_row[1], "postgres/pgkinetic/dashboard/default");
    assert_eq!(server_row[2], "active");
    assert_eq!(server_row[3], "9000");
    assert_eq!(server_row[4], "true");
    assert_eq!(server_row[5], "replica");
    assert_eq!(server_row[6], "replica");
    assert_eq!(server_row[7], "healthy");
    assert_eq!(server_row[8], "125");
    assert_eq!(server_row[9], "2/10");
    let last_probe_age_ms = server_row[10]
        .parse::<u64>()
        .expect("last probe age is numeric");
    assert!(
        last_probe_age_ms <= 10_000,
        "unexpected probe age: {last_probe_age_ms}"
    );

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn show_settings_redacts_secret_config() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let mut config = test_config(Some(admin_addr), Some("admin"), backend_addr);
    config.tls.client_cert_path = Some(PathBuf::from("client-cert.pem"));
    config.tls.client_key_path = Some(PathBuf::from("client-key.pem"));
    config.tls.client_ca_path = Some(PathBuf::from("client-ca.pem"));
    config.tls.backend_ca_path = Some(PathBuf::from("backend-ca.pem"));
    config.tls.backend_server_name = Some(String::from("db.example.internal"));
    config.auth.backend_password_env_var_name = Some(String::from("PG_KINETIC_BACKEND_PASSWORD"));
    config.auth.backend_user = Some(String::from("proxy_user"));
    let (run_handle, _, _) = spawn_proxy(config).await;

    let settings_frames = admin_query(admin_addr, "SHOW SETTINGS").await;
    let settings_text = table_text(&settings_frames);
    for secret in [
        "client-cert.pem",
        "client-key.pem",
        "client-ca.pem",
        "backend-ca.pem",
        "db.example.internal",
        "PG_KINETIC_BACKEND_PASSWORD",
    ] {
        assert!(
            !settings_text.contains(secret),
            "secret value leaked into SHOW SETTINGS: {secret}"
        );
    }
    assert!(settings_text.contains("proxy_user"));
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

fn route_key(application_name: &str) -> RouteKey {
    RouteKey::new(
        "postgres",
        "pgkinetic",
        Some(application_name),
        Some("127.0.0.1:5432".parse().expect("socket address")),
        QueryClass::Default,
    )
}

fn install_metrics_recorder() -> Arc<TestRecorder> {
    METRICS_RECORDER
        .get_or_init(|| {
            let recorder = Arc::new(TestRecorder::default());
            ::metrics::set_global_recorder(recorder.clone()).expect("install metrics recorder");
            recorder
        })
        .clone()
}

fn assert_no_sensitive_labels(recorder: &TestRecorder) {
    let forbidden = [
        "select",
        "bind",
        "password",
        "127.0.0.1",
        "BEGIN CERTIFICATE",
        "client_addr",
    ];

    let signatures = recorder.signatures();
    for signature in signatures {
        let lowered = signature.to_ascii_lowercase();
        for needle in forbidden {
            assert!(
                !lowered.contains(&needle.to_ascii_lowercase()),
                "unexpected sensitive label content in {signature}"
            );
        }
    }
}

#[derive(Debug, Default)]
struct TestRecorder {
    registrations: Mutex<HashSet<String>>,
}

impl TestRecorder {
    fn clear(&self) {
        self.registrations.lock().expect("lock recorder").clear();
    }

    fn has_metric(&self, name: &str, labels: &[(&str, &str)]) -> bool {
        self.registrations
            .lock()
            .expect("lock recorder")
            .contains(&metric_signature(name, labels))
    }

    fn signatures(&self) -> Vec<String> {
        self.registrations
            .lock()
            .expect("lock recorder")
            .iter()
            .cloned()
            .collect()
    }
}

impl Recorder for TestRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.registrations
            .lock()
            .expect("lock recorder")
            .insert(metric_signature_from_key(key));
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.registrations
            .lock()
            .expect("lock recorder")
            .insert(metric_signature_from_key(key));
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.registrations
            .lock()
            .expect("lock recorder")
            .insert(metric_signature_from_key(key));
        Histogram::noop()
    }
}

fn metric_signature_from_key(key: &Key) -> String {
    let labels = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}|{}", key.name(), labels)
}

fn metric_signature(name: &str, labels: &[(&str, &str)]) -> String {
    let labels = labels
        .iter()
        .map(|(label_key, label_value)| format!("{label_key}={label_value}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}|{labels}")
}

async fn spawn_proxy(config: Config) -> (tokio::task::JoinHandle<()>, SocketAddr, SnapshotStore) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listener.local_addr().expect("listen addr");
    drop(listener);

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
            client_tls_mode: ClientTlsMode::Disable,
            client_cert_path: None,
            client_key_path: None,
            client_ca_path: None,
            backend_tls_mode: BackendTlsMode::Disable,
            backend_ca_path: None,
            backend_server_name: None,
        },
        auth: AuthConfig {
            auth_mode: AuthMode::PassThrough,
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
    let mut body = BytesMut::new();
    body.extend_from_slice(sql.as_bytes());
    body.put_u8(0);

    let mut packet = BytesMut::new();
    packet.put_u8(b'Q');
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet.to_vec()
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

fn assert_admin_table_columns(frames: &[BackendFrame], expected_columns: &[&str]) {
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
}

fn first_data_row(frames: &[BackendFrame]) -> Vec<String> {
    frames
        .iter()
        .find(|frame| frame.tag == b'D')
        .map(data_row_values)
        .expect("data row")
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
