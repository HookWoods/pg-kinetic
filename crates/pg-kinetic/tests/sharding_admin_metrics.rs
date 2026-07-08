use std::{
    collections::HashSet,
    net::SocketAddr,
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
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, ClientTlsMode, Config,
        ConnectionConfig, DrainConfig, HealthConfig, MultiShardPolicyConfig, ObservabilityConfig,
        ReloadConfig, RouteMapConfig, RouteMapPriority, ShardScopeConfig, ShardStrategyConfig,
        ShardTargetConfig, ShardingConfig, SocketConfig, TlsConfig,
    },
    core::{
        lsn::PgLsn,
        route::{QueryClass, RouteKey},
        routing::{FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
        sharding::{
            ShardDrainPolicy, ShardId, ShardLifecycleState, ShardMigrationSafetyReport,
            ShardMigrationState, ShardRebalancePlan,
        },
    },
    proxy::Proxy,
    proxy_runtime::{
        metrics as proxy_metrics,
        sharding::RouteMapReloadErrorCode,
        snapshot::{
            RouteMapReloadSnapshot, RoutePolicySnapshot, RouteSnapshot, ShardLifecycleSnapshot,
            ShardMigrationSafetySnapshot, SnapshotStore,
        },
    },
    wire::{
        backend::{parse_backend_frame, BackendFrame, ReadyStatus},
        protocol::ProtocolVersion,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex as AsyncMutex,
    time,
};

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();
static TEST_MUTEX: OnceLock<AsyncMutex<()>> = OnceLock::new();

#[tokio::test]
async fn sharding_admin_views_expose_routes_maps_shards_and_migrations() {
    let _test_guard = TEST_MUTEX
        .get_or_init(|| AsyncMutex::new(()))
        .lock()
        .await;
    let recorder = install_metrics_recorder();
    recorder.clear();

    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, snapshot_store) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    let route = route_key("dashboard");
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

    snapshot_store.set_sharding_snapshot(sharding_snapshot());

    snapshot_store.set_route_map_reload_snapshot(RouteMapReloadSnapshot {
        route_map_generation_id: 7,
        success: true,
        error_code: None,
        draining_shard_ids: vec![],
    });
    snapshot_store.set_route_map_reload_snapshot(RouteMapReloadSnapshot {
        route_map_generation_id: 8,
        success: false,
        error_code: Some(RouteMapReloadErrorCode::ConflictingRouteScopes),
        draining_shard_ids: vec![],
    });

    snapshot_store.set_shard_lifecycle_snapshot(ShardLifecycleSnapshot::new(
        shard_id("tenant-b"),
        ShardLifecycleState::Draining,
        ShardDrainPolicy::default(),
    ));

    snapshot_store.set_shard_migration_safety_snapshot(ShardMigrationSafetySnapshot::new(
        migration_plan(),
    ));

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
            "1",
            "1",
            "prefer_replica",
            "wait",
            "session_write_lsn_and_max_lag",
            "750",
            "8",
            "true",
        ]],
    );

    let route_maps_frames = admin_query(admin_addr, "SHOW ROUTE MAPS").await;
    assert_admin_table_response(
        &route_maps_frames,
        &["scope", "strategy", "priority", "multi_shard_policy"],
        &[vec!["billing/reporter", "hash", "7", "fan_out"]],
    );

    let shards_frames = admin_query(admin_addr, "SHOW SHARDS").await;
    assert_admin_table_response(
        &shards_frames,
        &[
            "shard_id",
            "route_key",
            "lifecycle_state",
            "primary_backend_count",
            "replica_backend_count",
            "health_summary",
        ],
        &[vec![
            "tenant-a",
            "billing/reporter",
            "active",
            "1",
            "0",
            "healthy",
        ], vec![
            "tenant-b",
            "billing/reporter",
            "draining",
            "0",
            "1",
            "draining",
        ]],
    );

    let migrations_frames = admin_query(admin_addr, "SHOW MIGRATIONS").await;
    assert_admin_table_response(
        &migrations_frames,
        &[
            "migration_state",
            "migration_override_explicit",
            "source_shard_ids",
            "target_shard_ids",
            "active_client_count",
            "prepared_statement_count",
            "open_transaction_count",
            "last_required_lsn",
        ],
        &[vec![
            "assessing",
            "true",
            "tenant-a",
            "tenant-b",
            "2",
            "2",
            "1",
            "2/10",
        ]],
    );

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);
    assert!(!recorder.signatures().is_empty(), "expected sharding metrics to be recorded");
    assert_no_sensitive_labels(&recorder);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn sharding_metrics_use_bucketed_labels_and_reject_sensitive_data() {
    let _test_guard = TEST_MUTEX
        .get_or_init(|| AsyncMutex::new(()))
        .lock()
        .await;
    let recorder = install_metrics_recorder();
    recorder.clear();

    let route = route_key("dashboard");
    let route_label = route.metric_label();

    proxy_metrics::record_shard_route_decision(
        &route,
        Some("tenant-a"),
        pg_kinetic::core::sharding::ShardStrategy::Hash,
        pg_kinetic::core::sharding::ShardRouteReason::HashMatch,
        "selected",
    );
    proxy_metrics::record_shard_multi_shard_rejection(
        &route,
        Some("tenant-b"),
        pg_kinetic::core::sharding::MultiShardPolicy::FanOut,
        pg_kinetic::core::sharding::ShardRouteReason::MultiShardRejected,
        "rejected",
    );
    proxy_metrics::record_shard_primary_fallback(
        &route,
        Some("tenant-c"),
        pg_kinetic::core::sharding::MultiShardPolicy::Reject,
        "fallback",
    );

    let snapshot_store = SnapshotStore::new();
    snapshot_store.set_route_map_reload_snapshot(RouteMapReloadSnapshot {
        route_map_generation_id: 9,
        success: true,
        error_code: None,
        draining_shard_ids: vec![],
    });
    snapshot_store.set_route_map_reload_snapshot(RouteMapReloadSnapshot {
        route_map_generation_id: 10,
        success: false,
        error_code: Some(RouteMapReloadErrorCode::ConflictingRouteScopes),
        draining_shard_ids: vec![],
    });
    snapshot_store.set_shard_lifecycle_snapshot(ShardLifecycleSnapshot::new(
        shard_id("tenant-a"),
        ShardLifecycleState::Readonly,
        ShardDrainPolicy::default(),
    ));
    snapshot_store.set_shard_migration_safety_snapshot(ShardMigrationSafetySnapshot::new(
        migration_plan(),
    ));

    let shard_bucket = bucket_label("tenant-a");
    assert!(recorder.has_metric(
        "pg_kinetic_shard_route_decisions_total",
        &[
            ("route", route_label.as_str()),
            ("shard", shard_bucket.as_str()),
            ("strategy", "hash"),
            ("reason", "hash_match"),
            ("outcome", "selected"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_shard_multi_shard_rejections_total",
        &[
            ("route", route_label.as_str()),
            ("shard", bucket_label("tenant-b").as_str()),
            ("policy", "fan_out"),
            ("reason", "multi_shard_rejected"),
            ("outcome", "rejected"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_shard_primary_fallbacks_total",
        &[
            ("route", route_label.as_str()),
            ("shard", bucket_label("tenant-c").as_str()),
            ("policy", "reject"),
            ("outcome", "fallback"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_route_map_reload_total",
        &[("outcome", "success"), ("error_code", "none")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_route_map_reload_total",
        &[
            ("outcome", "failure"),
            ("error_code", "conflicting_route_scopes"),
        ],
    ));
    assert!(recorder.has_metric("pg_kinetic_route_map_generation", &[]));
    assert!(recorder.has_metric(
        "pg_kinetic_shard_lifecycle_state",
        &[
            ("shard", shard_bucket.as_str()),
            ("lifecycle_state", "readonly"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_shard_active_transactions",
        &[("shard", shard_bucket.as_str())],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_shard_prepared_statements",
        &[("shard", shard_bucket.as_str())],
    ));

    assert_no_sensitive_labels(&recorder);
}

fn sharding_snapshot() -> ShardingConfig {
    ShardingConfig {
        sharding_enabled: true,
        multi_shard_policy: MultiShardPolicyConfig::FanOut,
        route_map_reload_strict: true,
        route_preview_enabled: false,
        route_maps: vec![RouteMapConfig {
            scope: ShardScopeConfig::DatabaseUser {
                database: String::from("billing"),
                user: String::from("reporter"),
            },
            strategy: ShardStrategyConfig::Hash,
            targets: vec![
                ShardTargetConfig::Primary {
                    shard_id: String::from("tenant-a"),
                },
                ShardTargetConfig::Replicas {
                    shard_id: String::from("tenant-b"),
                },
            ],
            priority: Some(RouteMapPriority(7)),
        }],
    }
}

fn migration_plan() -> ShardRebalancePlan {
    ShardRebalancePlan::new(
        vec![shard_id("tenant-a")],
        vec![shard_id("tenant-b")],
    )
    .with_migration_state(ShardMigrationState::Assessing)
    .with_migration_override_explicit(true)
    .with_safety_report(ShardMigrationSafetyReport::new(
        vec![11, 17],
        vec![String::from("stmt_a"), String::from("stmt_b")],
        vec![88],
        Some(PgLsn::from_parts(2, 16)),
    ))
}

fn shard_id(value: &str) -> ShardId {
    ShardId::new(value).expect("valid shard id")
}

fn bucket_label(value: &str) -> String {
    format!("bucket_{}", shard_bucket(value))
}

fn shard_bucket(value: &str) -> u8 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    (hash % 8) as u8
}

fn route_key(application_name: &str) -> RouteKey {
    RouteKey::new(
        "billing",
        "reporter",
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
        "bind",
        "password",
        "127.0.0.1",
        "BEGIN CERTIFICATE",
        "client_addr",
        "tenant-a",
        "tenant-b",
        "tenant-c",
    ];

    for signature in recorder.signatures() {
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
    let mut config = Config::default();
    config.connection = ConnectionConfig {
        listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        backend_addr,
    };
    config.admin = pg_kinetic::config::AdminConfig {
        admin_addr,
        admin_require_tls: false,
        admin_allowed_user: admin_allowed_user.map(str::to_owned),
        admin_query_timeout_ms: 100,
        admin_max_clients: 4,
    };
    config.observability = ObservabilityConfig {
        metrics_addr: None,
        ..Default::default()
    };
    config.tls = TlsConfig {
        client_tls_mode: ClientTlsMode::Disable,
        client_cert_path: None,
        client_key_path: None,
        client_ca_path: None,
        backend_tls_mode: BackendTlsMode::Disable,
        backend_ca_path: None,
        backend_server_name: None,
    };
    config.auth = AuthConfig {
        auth_mode: AuthMode::PassThrough,
        auth_users_file: None,
        backend_user: None,
        backend_password_env_var_name: None,
        auth_failure_message_mode: AuthFailureMessageMode::Generic,
    };
    config.reload = ReloadConfig::default();
    config.drain = DrainConfig::default();
    config.health = HealthConfig::default();
    config.socket = SocketConfig::default();
    config
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
        .map(|row| row.iter().map(|value| (*value).to_owned()).collect::<Vec<_>>())
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

fn row_description_columns(frame: &BackendFrame) -> Vec<&str> {
    let mut offset = 0;
    let mut columns = Vec::new();
    let field_count = read_i16(&frame.payload, &mut offset) as usize;
    for _ in 0..field_count {
        let name = read_cstring(&frame.payload, &mut offset);
        columns.push(name);
        offset += 18;
    }

    columns
}

fn data_row_values(frame: &BackendFrame) -> Vec<String> {
    let mut offset = 0;
    let field_count = read_i16(&frame.payload, &mut offset) as usize;
    let mut values = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let length = read_i32(&frame.payload, &mut offset);
        if length < 0 {
            values.push(String::from("<null>"));
            continue;
        }

        let length = length as usize;
        let value = std::str::from_utf8(&frame.payload[offset..offset + length])
            .expect("data row value")
            .to_owned();
        offset += length;
        values.push(value);
    }

    values
}

fn read_i16(bytes: &[u8], offset: &mut usize) -> i16 {
    let value = i16::from_be_bytes([bytes[*offset], bytes[*offset + 1]]);
    *offset += 2;
    value
}

fn read_i32(bytes: &[u8], offset: &mut usize) -> i32 {
    let value = i32::from_be_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    value
}

fn read_cstring<'a>(bytes: &'a [u8], offset: &mut usize) -> &'a str {
    let start = *offset;
    while bytes[*offset] != 0 {
        *offset += 1;
    }
    let value = std::str::from_utf8(&bytes[start..*offset]).expect("cstring");
    *offset += 1;
    value
}
