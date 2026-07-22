use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::{Duration, Instant},
};

use bytes::{BufMut, BytesMut};
use metrics::{Counter, Gauge, Histogram, Key, Metadata, Recorder};
use pg_kinetic::{
    config::{
        BackendEndpointConfig, CapacityConfig, Config, ConnectionConfig, FreshnessConfig, HaConfig,
        ObservabilityConfig, PerformanceConfig, QosConfig, ReadRoutingConfig, ReplicaConfig,
        RouteConfig,
    },
    proxy::Proxy,
    proxy_runtime::snapshot::{ReplicaHealthSnapshot, SnapshotStore},
    wire::{
        admin::{build_admin_table_response, AdminWireColumn, AdminWireType},
        backend::parse_backend_frame,
        frame::parse_frontend_frame,
        message::parse_simple_query,
        protocol::{FrontendTag, ProtocolVersion},
    },
};
use pg_kinetic_core::{
    ha::{
        EndpointHealth, EndpointRoleState, HealthProbeOutcome, ReplicaLagState, RoleProbeOutcome,
        SplitBrainWarning,
    },
    lsn::PgLsn,
    routing::{FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::sleep,
};

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();

fn install_metrics_recorder() -> Arc<TestRecorder> {
    METRICS_RECORDER
        .get_or_init(|| {
            let recorder = Arc::new(TestRecorder::default());
            metrics::set_global_recorder(recorder.clone()).expect("install metrics recorder");
            recorder
        })
        .clone()
}

#[derive(Debug, Default)]
struct TestRecorder {
    registrations: StdMutex<HashSet<String>>,
}

impl TestRecorder {
    fn has_metric(&self, name: &str, labels: &[(&str, &str)]) -> bool {
        self.registrations
            .lock()
            .expect("lock recorder")
            .contains(&metric_signature(name, labels))
    }
}

impl Recorder for TestRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

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

fn query_packet(sql: &str) -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_u8(u8::from(FrontendTag::Query));
    packet.put_i32((sql.len() + 5) as i32);
    packet.extend_from_slice(sql.as_bytes());
    packet.put_u8(0);
    packet.to_vec()
}

fn startup_packet() -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0postgres\0database\0pgkinetic\0\0");

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet.to_vec()
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

async fn spawn_backend(
    role: &'static str,
    wal_lsn: Option<&'static str>,
) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = listener.local_addr().expect("backend addr");

    tokio::spawn({
        let events = Arc::clone(&events);
        async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept backend");
                let events = Arc::clone(&events);
                tokio::spawn(async move {
                    handle_backend_connection(role, wal_lsn, stream, events).await;
                });
            }
        }
    });

    (backend_addr, events)
}

async fn handle_backend_connection(
    role: &'static str,
    wal_lsn: Option<&'static str>,
    mut stream: TcpStream,
    events: Arc<Mutex<Vec<String>>>,
) {
    let mut startup = [0_u8; 2048];
    let read = stream.read(&mut startup).await.expect("read startup");
    if read == 0 {
        return;
    }

    events.lock().await.push(format!("{role}:connect"));
    stream
        .write_all(&auth_ok_ready())
        .await
        .expect("auth ready");

    let mut buffer = BytesMut::with_capacity(4096);
    let mut in_transaction = false;

    loop {
        let read = stream.read_buf(&mut buffer).await.expect("read frontend");
        if read == 0 {
            return;
        }

        while let Some(frame) = parse_frontend_frame(&mut buffer).expect("parse frontend frame") {
            if let Some(query) = parse_simple_query(&frame).expect("simple query") {
                let normalized = normalize_sql(query);
                events
                    .lock()
                    .await
                    .push(format!("{role}:query:{normalized}"));

                if normalized == "select pg_current_wal_lsn()" {
                    if let Some(wal_lsn) = wal_lsn {
                        stream
                            .write_all(&lsn_response(wal_lsn))
                            .await
                            .expect("wal lsn response");
                    } else {
                        stream
                            .write_all(&ready_idle())
                            .await
                            .expect("fallback response");
                    }
                    continue;
                }

                if normalized.starts_with("begin") || normalized.starts_with("start transaction") {
                    in_transaction = true;
                    stream
                        .write_all(&ready_in_transaction())
                        .await
                        .expect("begin response");
                } else if normalized.starts_with("commit") || normalized.starts_with("rollback") {
                    in_transaction = false;
                    stream
                        .write_all(&ready_idle())
                        .await
                        .expect("commit response");
                } else if in_transaction {
                    stream
                        .write_all(&ready_in_transaction())
                        .await
                        .expect("transaction response");
                } else {
                    stream
                        .write_all(&ready_idle())
                        .await
                        .expect("query response");
                }
            }
        }
    }
}

fn lsn_response(value: &str) -> Vec<u8> {
    build_admin_table_response(
        &[AdminWireColumn::new(
            "pg_current_wal_lsn",
            AdminWireType::Text,
        )],
        &[vec![value.to_string()]],
    )
    .to_vec()
}

fn auth_ok_ready() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'R');
    bytes.put_i32(8);
    bytes.put_i32(0);
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}

fn ready_idle() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'C');
    bytes.put_i32(13);
    bytes.extend_from_slice(b"SELECT 1\0");
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}

fn ready_in_transaction() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'C');
    bytes.put_i32(10);
    bytes.extend_from_slice(b"BEGIN\0");
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'T');
    bytes.to_vec()
}

fn error_sqlstate(response: &[u8]) -> Option<&'static str> {
    let mut buffer = BytesMut::from(response);
    while let Some(frame) = parse_backend_frame(&mut buffer).ok().flatten() {
        if let Some(sqlstate) = frame.sqlstate() {
            return Some(sqlstate.as_str());
        }
    }

    None
}

async fn spawn_proxy(
    primary_addr: SocketAddr,
    replica_addr: SocketAddr,
    fallback_policy: FallbackPolicy,
    freshness_policy: FreshnessPolicy,
    max_replica_lag_ms: u64,
    read_after_write_timeout_ms: u64,
) -> (SocketAddr, SnapshotStore) {
    let _metrics_recorder = install_metrics_recorder();
    let route = RouteConfig {
        primary: BackendEndpointConfig {
            address: primary_addr,
            connect_timeout_ms: 100,
            tls_mode: pg_kinetic::config::BackendTlsMode::Disable,
        },
        replicas: vec![ReplicaConfig {
            address: replica_addr,
            connect_timeout_ms: 100,
            tls_mode: pg_kinetic::config::BackendTlsMode::Disable,
            weight: 1,
        }],
        read_routing: ReadRoutingConfig {
            read_routing_mode: ReadRoutingMode::PreferReplica,
            fallback_policy,
        },
        freshness: FreshnessConfig {
            freshness_policy,
            max_replica_lag_ms,
            read_after_write_timeout_ms,
        },
        ha: HaConfig::default(),
    };

    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr,
            backend_addr: primary_addr,
        },
        routes: vec![route],
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 8,
            max_checkout_waiters: 4,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            pool_mode: Default::default(),
            recovery_mode: pg_kinetic::recovery::RecoveryMode::Recover,
            recovery_timeout_ms: 1_000,
            backend_reset_query: String::from("DISCARD ALL"),
        },
        qos: QosConfig {
            max_route_in_flight: 100,
            max_route_waiters: 100,
            query_timeout_ms: 5_000,
            idle_client_timeout_ms: 5_000,
            idle_transaction_timeout_ms: 5_000,
            max_client_buffer_bytes: 1_048_576,
            max_backend_buffer_bytes: 4_194_304,
            overload_error_code: String::from("53300"),
        },
        admin: Default::default(),
        observability: ObservabilityConfig::default(),
        tls: Default::default(),
        auth: Default::default(),
        reload: Default::default(),
        drain: Default::default(),
        health: Default::default(),
        socket: Default::default(),
    };

    let proxy = Proxy::new(config);
    let snapshot_store = proxy.snapshot_store();
    tokio::spawn(async move {
        let _ = proxy.run().await;
    });
    sleep(Duration::from_millis(100)).await;

    (listen_addr, snapshot_store)
}

fn publish_replica_health(
    snapshot_store: &SnapshotStore,
    replica_id: u64,
    replica_addr: SocketAddr,
    replay_lsn: PgLsn,
    lag_ms: u64,
) {
    let mut snapshot = ReplicaHealthSnapshot::new(
        replica_id,
        replica_addr,
        pg_kinetic_core::routing::BackendRole::Replica,
    );
    snapshot.health = HealthProbeOutcome::new(EndpointHealth::Healthy, false, 0);
    snapshot.role = RoleProbeOutcome::new(EndpointRoleState::Replica, None);
    snapshot.replay_lsn = Some(replay_lsn);
    snapshot.lag_duration = Some(Duration::from_millis(lag_ms));
    snapshot.lag_state = if lag_ms == 0 {
        ReplicaLagState::Fresh
    } else {
        ReplicaLagState::Lagging
    };
    snapshot.last_successful_probe_at = Some(std::time::SystemTime::now());
    snapshot_store.set_replica_health_snapshot(snapshot);
}

fn publish_replica_split_brain_health(
    snapshot_store: &SnapshotStore,
    replica_id: u64,
    replica_addr: SocketAddr,
    replay_lsn: PgLsn,
    lag_ms: u64,
    expected_role: pg_kinetic_core::routing::BackendRole,
    observed_role: pg_kinetic_core::routing::BackendRole,
) {
    let mut snapshot = ReplicaHealthSnapshot::new(
        replica_id,
        replica_addr,
        pg_kinetic_core::routing::BackendRole::Replica,
    );
    snapshot.health = HealthProbeOutcome::new(EndpointHealth::Healthy, false, 0);
    snapshot.role = RoleProbeOutcome::new(
        EndpointRoleState::Warning,
        Some(SplitBrainWarning::new(expected_role, observed_role)),
    );
    snapshot.replay_lsn = Some(replay_lsn);
    snapshot.lag_duration = Some(Duration::from_millis(lag_ms));
    snapshot.lag_state = if lag_ms == 0 {
        ReplicaLagState::Fresh
    } else {
        ReplicaLagState::Lagging
    };
    snapshot.last_successful_probe_at = Some(std::time::SystemTime::now());
    snapshot_store.set_replica_health_snapshot(snapshot);
}

async fn open_client(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");
    read_until_ready_for_query(&mut stream, "startup response").await;
    stream
}

async fn run_transaction(stream: &mut TcpStream, queries: &[&str]) {
    for query in queries {
        stream.write_all(&query_packet(query)).await.expect("query");
        read_response_bytes(stream, "transaction response").await;
    }
}

async fn run_simple_query(stream: &mut TcpStream, sql: &str) -> Vec<u8> {
    stream.write_all(&query_packet(sql)).await.expect("query");
    read_response_bytes(stream, "query response").await
}

async fn read_until_ready_for_query(stream: &mut TcpStream, context: &str) {
    let mut buffer = BytesMut::with_capacity(1024);
    loop {
        if parse_backend_frame(&mut buffer)
            .expect(context)
            .and_then(|frame| frame.ready_status())
            .is_some()
        {
            return;
        }

        let read = stream.read_buf(&mut buffer).await.expect(context);
        assert!(read > 0, "{context}: client disconnected");
    }
}

async fn read_response_bytes(stream: &mut TcpStream, context: &str) -> Vec<u8> {
    let mut response = Vec::new();
    let mut buffer = BytesMut::with_capacity(1024);
    loop {
        let before = buffer.len();
        let read = stream.read_buf(&mut buffer).await.expect(context);
        assert!(read > 0, "{context}: client disconnected");
        response.extend_from_slice(&buffer[before..]);

        while let Some(frame) = parse_backend_frame(&mut buffer).expect(context) {
            if frame.ready_status().is_some() {
                return response;
            }
        }
    }
}

async fn collect_events(events: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    events.lock().await.clone()
}

#[tokio::test]
async fn write_transaction_records_required_session_lsn_and_blocks_stale_replica() {
    let (primary_addr, primary_events) = spawn_backend("primary", Some("0/20")).await;
    let (replica_addr, replica_events) = spawn_backend("replica", None).await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        replica_addr,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
        200,
    )
    .await;

    publish_replica_health(
        &snapshot_store,
        1,
        replica_addr,
        PgLsn::from_parts(0, 10),
        10,
    );

    let mut client = open_client(proxy_addr).await;
    run_transaction(
        &mut client,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    let primary_events_snapshot = collect_events(&primary_events).await;
    assert!(
        primary_events_snapshot
            .iter()
            .any(|event| event == "primary:query:select pg_current_wal_lsn()"),
        "events: {primary_events_snapshot:?}"
    );

    let stale_read = run_simple_query(&mut client, "select 1").await;
    assert_ne!(stale_read.first().copied(), Some(b'E'));
    sleep(Duration::from_millis(50)).await;

    let primary_events_after = collect_events(&primary_events).await;
    assert!(
        primary_events_after
            .iter()
            .any(|event| event == "primary:query:select 1"),
        "events: {primary_events_after:?}"
    );
    assert!(!collect_events(&replica_events)
        .await
        .iter()
        .any(|event| event == "replica:query:select 1"));

    publish_replica_health(
        &snapshot_store,
        1,
        replica_addr,
        PgLsn::from_parts(0, 32),
        2,
    );

    run_simple_query(&mut client, "select 1").await;
    sleep(Duration::from_millis(50)).await;

    let replica_events_snapshot = collect_events(&replica_events).await;
    assert!(
        replica_events_snapshot
            .iter()
            .any(|event| event == "replica:query:select 1"),
        "events: {replica_events_snapshot:?}"
    );
}

#[tokio::test]
async fn fallback_primary_uses_primary_while_replica_catches_up() {
    let (primary_addr, primary_events) = spawn_backend("primary", Some("0/20")).await;
    let (replica_addr, replica_events) = spawn_backend("replica", None).await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        replica_addr,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
        200,
    )
    .await;

    publish_replica_health(
        &snapshot_store,
        1,
        replica_addr,
        PgLsn::from_parts(0, 10),
        10,
    );

    let mut client = open_client(proxy_addr).await;
    run_transaction(
        &mut client,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    run_simple_query(&mut client, "select 1").await;
    sleep(Duration::from_millis(50)).await;

    let primary_events_snapshot = collect_events(&primary_events).await;
    assert!(
        primary_events_snapshot
            .iter()
            .any(|event| event == "primary:query:select 1"),
        "events: {primary_events_snapshot:?}"
    );
    assert!(!collect_events(&replica_events)
        .await
        .iter()
        .any(|event| event == "replica:query:select 1"));
}

#[tokio::test]
async fn fallback_wait_waits_until_replica_is_fresh_enough() {
    let (primary_addr, _primary_events) = spawn_backend("primary", Some("0/20")).await;
    let (replica_addr, replica_events) = spawn_backend("replica", None).await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        replica_addr,
        FallbackPolicy::Wait,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
        200,
    )
    .await;

    publish_replica_health(
        &snapshot_store,
        1,
        replica_addr,
        PgLsn::from_parts(0, 10),
        10,
    );

    let mut client = open_client(proxy_addr).await;
    run_transaction(
        &mut client,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    let snapshot_store = snapshot_store.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(60)).await;
        publish_replica_health(
            &snapshot_store,
            1,
            replica_addr,
            PgLsn::from_parts(0, 32),
            2,
        );
    });

    let started = Instant::now();
    run_simple_query(&mut client, "select 1").await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(50),
        "wait path returned too early: {elapsed:?}"
    );

    sleep(Duration::from_millis(50)).await;
    let replica_events_snapshot = collect_events(&replica_events).await;
    assert!(
        replica_events_snapshot
            .iter()
            .any(|event| event == "replica:query:select 1"),
        "events: {replica_events_snapshot:?}"
    );
}

#[tokio::test]
async fn fallback_reject_returns_postgresql_error_when_freshness_is_impossible() {
    let recorder = install_metrics_recorder();
    let (primary_addr, primary_events) = spawn_backend("primary", Some("0/20")).await;
    let (replica_addr, _replica_events) = spawn_backend("replica", None).await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        replica_addr,
        FallbackPolicy::Reject,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
        200,
    )
    .await;

    publish_replica_health(
        &snapshot_store,
        1,
        replica_addr,
        PgLsn::from_parts(0, 10),
        10,
    );

    let mut client = open_client(proxy_addr).await;
    run_transaction(
        &mut client,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    let response = run_simple_query(&mut client, "select 1").await;
    assert_eq!(response.first().copied(), Some(b'E'));
    assert_eq!(error_sqlstate(&response), Some("57P03"));

    let primary_events_snapshot = collect_events(&primary_events).await;
    assert!(
        !primary_events_snapshot
            .iter()
            .any(|event| event == "primary:query:select 1"),
        "events: {primary_events_snapshot:?}"
    );

    let checkout = snapshot_store
        .route_checkout_snapshots()
        .into_iter()
        .next()
        .expect("checkout snapshot");
    assert_eq!(checkout.decision.reason().as_str(), "replica_stale");
    assert!(recorder.has_metric("pg_kinetic_read_after_write_total", &[("outcome", "stale")]));
}

#[tokio::test]
async fn wait_fallback_times_out_then_routes_to_primary() {
    let recorder = install_metrics_recorder();
    let (primary_addr, primary_events) = spawn_backend("primary", Some("0/20")).await;
    let (replica_addr, replica_events) = spawn_backend("replica", None).await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        replica_addr,
        FallbackPolicy::Wait,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
        150,
    )
    .await;

    let mut client = open_client(proxy_addr).await;
    run_transaction(
        &mut client,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    let response = run_simple_query(&mut client, "select 1").await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(120),
        "wait path returned too early: {elapsed:?}"
    );
    assert_ne!(response.first().copied(), Some(b'E'));

    let primary_events_snapshot = collect_events(&primary_events).await;
    assert!(
        primary_events_snapshot
            .iter()
            .any(|event| event == "primary:query:select 1"),
        "events: {primary_events_snapshot:?}"
    );
    assert!(!collect_events(&replica_events)
        .await
        .iter()
        .any(|event| event == "replica:query:select 1"));

    let checkout = snapshot_store
        .route_checkout_snapshots()
        .into_iter()
        .next()
        .expect("checkout snapshot");
    assert_eq!(checkout.decision.reason().as_str(), "fallback_primary");
    assert!(recorder.has_metric("pg_kinetic_read_after_write_total", &[("outcome", "stale")]));
}

#[tokio::test]
async fn split_brain_role_warning_follows_fallback_policy() {
    let recorder = install_metrics_recorder();
    let (primary_addr, primary_events) = spawn_backend("primary", Some("0/20")).await;
    let (replica_addr, replica_events) = spawn_backend("replica", None).await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        replica_addr,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
        200,
    )
    .await;

    publish_replica_split_brain_health(
        &snapshot_store,
        1,
        replica_addr,
        PgLsn::from_parts(0, 40),
        5,
        pg_kinetic_core::routing::BackendRole::Replica,
        pg_kinetic_core::routing::BackendRole::Primary,
    );

    let mut client = open_client(proxy_addr).await;
    run_transaction(
        &mut client,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    let response = run_simple_query(&mut client, "select 1").await;
    assert_ne!(response.first().copied(), Some(b'E'));

    assert!(collect_events(&primary_events)
        .await
        .iter()
        .any(|event| event == "primary:query:select 1"));
    assert!(!collect_events(&replica_events)
        .await
        .iter()
        .any(|event| event == "replica:query:select 1"));

    let checkout = snapshot_store
        .route_checkout_snapshots()
        .into_iter()
        .next()
        .expect("checkout snapshot");
    assert_eq!(checkout.decision.reason().as_str(), "fallback_primary");
    assert!(snapshot_store
        .replica_health_snapshots()
        .into_iter()
        .next()
        .expect("replica health snapshot")
        .role
        .warning
        .is_some());
    assert!(recorder.has_metric("pg_kinetic_read_after_write_total", &[("outcome", "stale")]));
}

#[tokio::test]
async fn stale_ok_hint_bypasses_session_lsn_only_when_route_config_allows_it() {
    let (primary_addr_a, primary_events_a) = spawn_backend("primary-a", Some("0/20")).await;
    let (replica_addr_a, replica_events_a) = spawn_backend("replica-a", None).await;
    let (proxy_addr_a, snapshot_store_a) = spawn_proxy(
        primary_addr_a,
        replica_addr_a,
        FallbackPolicy::Primary,
        FreshnessPolicy::MaxReplicaLag,
        50,
        200,
    )
    .await;

    publish_replica_health(
        &snapshot_store_a,
        1,
        replica_addr_a,
        PgLsn::from_parts(0, 10),
        10,
    );

    let mut client_a = open_client(proxy_addr_a).await;
    run_transaction(
        &mut client_a,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    run_simple_query(&mut client_a, "/* pg-kinetic: stale-ok */ select 1").await;
    sleep(Duration::from_millis(50)).await;

    assert!(
        collect_events(&replica_events_a)
            .await
            .iter()
            .any(|event| event.starts_with("replica-a:query:") && event.contains("select 1")),
        "replica should be selected when stale reads are allowed"
    );
    assert!(
        !collect_events(&primary_events_a)
            .await
            .iter()
            .any(|event| event.starts_with("primary-a:query:") && event.contains("select 1")),
        "primary should not be used when stale reads are allowed"
    );

    let (primary_addr_b, primary_events_b) = spawn_backend("primary-b", Some("0/20")).await;
    let (replica_addr_b, replica_events_b) = spawn_backend("replica-b", None).await;
    let (proxy_addr_b, snapshot_store_b) = spawn_proxy(
        primary_addr_b,
        replica_addr_b,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        50,
        200,
    )
    .await;

    publish_replica_health(
        &snapshot_store_b,
        1,
        replica_addr_b,
        PgLsn::from_parts(0, 10),
        10,
    );

    let mut client_b = open_client(proxy_addr_b).await;
    run_transaction(
        &mut client_b,
        &[
            "begin read write",
            "insert into accounts values (1)",
            "commit",
        ],
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    run_simple_query(&mut client_b, "/* pg-kinetic: stale-ok */ select 1").await;
    sleep(Duration::from_millis(50)).await;

    assert!(
        collect_events(&primary_events_b)
            .await
            .iter()
            .any(|event| event.starts_with("primary-b:query:") && event.contains("select 1")),
        "primary should be used when stale explicit reads are not allowed"
    );
    assert!(
        !collect_events(&replica_events_b)
            .await
            .iter()
            .any(|event| event.starts_with("replica-b:query:") && event.contains("select 1")),
        "replica should not be used when stale explicit reads are not allowed"
    );
}
