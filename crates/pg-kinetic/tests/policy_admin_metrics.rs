use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, CapacityConfig,
        ClientTlsMode, Config, ConnectionConfig, DrainConfig, HealthConfig,
        InlinePolicyActionConfig, InlinePolicyConfig, ObservabilityConfig, PerformanceConfig,
        PolicyConfig, QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    core::{
        lsn::FreshnessStatus,
        policy::{
            PolicyAction, PolicyAuditEvent, PolicyAuditKind, PolicyDecision, PolicyHookPoint,
            PolicyId, PolicyMode, PolicyOutcome, PolicyVersion,
        },
        routing::BackendRole,
        routing::QueryClass,
        session::TransactionAccessMode,
    },
    proxy::Proxy,
    proxy_runtime::{
        policy::{preview_policy, PolicyRuntime, PolicyStore},
        snapshot::{PolicyReloadSnapshot, PolicyStatusSnapshot, SnapshotStore},
    },
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
async fn show_policies_exposes_status_reload_details_and_bounded_history() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, snapshot_store) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    snapshot_store.set_policy_status_snapshot(PolicyStatusSnapshot::new(
        "route-fallback",
        7,
        PolicyMode::Enforce,
        "inline",
    ));
    snapshot_store.set_policy_reload_snapshot(PolicyReloadSnapshot {
        policy_generation_id: 6,
        success: true,
        error_code: None,
        error: None,
    });
    snapshot_store.set_policy_reload_snapshot(PolicyReloadSnapshot {
        policy_generation_id: 7,
        success: false,
        error_code: Some(
            pg_kinetic::proxy_runtime::policy::PolicyReloadErrorCode::RouteReferenceMissing,
        ),
        error: Some(String::from(
            "route override target 'route-9' does not reference an existing route",
        )),
    });

    let policies_frames = admin_query(admin_addr, "SHOW POLICIES").await;
    assert_admin_table_response(
        &policies_frames,
        &[
            "policy_id",
            "policy_version",
            "policy_mode",
            "source",
            "enabled",
            "last_reload_outcome",
            "error_code",
        ],
        &[vec![
            "route-fallback",
            "7",
            "enforce",
            "inline",
            "true",
            "failure",
            "route_reference_missing",
        ]],
    );

    let policy_runtime = PolicyRuntime::new(Duration::from_millis(5), 8_192);
    let policy_input = sample_policy_input();
    let policy_audit_handle = snapshot_store.policy_audit_handle();
    for policy_version in 1..=130 {
        policy_audit_handle.record(policy_event(
            &policy_runtime,
            PolicyAuditKind::Decision,
            policy_version,
            PolicyOutcome::Rejected,
            &policy_input,
        ));
    }
    policy_audit_handle.record(policy_event(
        &policy_runtime,
        PolicyAuditKind::Validation,
        999,
        PolicyOutcome::Rejected,
        &policy_input,
    ));

    let decision_frames = admin_query(admin_addr, "SHOW POLICY DECISIONS").await;
    assert_admin_table_columns(
        &decision_frames,
        &[
            "policy_id",
            "policy_version",
            "hook_point",
            "action",
            "outcome",
            "reason",
            "route",
            "shard",
            "target_role",
            "context",
        ],
    );
    let decision_rows = data_rows(&decision_frames);
    assert_eq!(decision_rows.len(), 128);
    assert_eq!(decision_rows.first().expect("first decision")[1], "3");
    assert_eq!(decision_rows.last().expect("last decision")[1], "130");
    for row in &decision_rows {
        assert_eq!(row[0], "route-fallback");
        assert_eq!(row[2], "before_routing");
        assert_eq!(row[3], "deny");
        assert_eq!(row[4], "rejected");
        assert_eq!(row[5], "policy_denied");
        assert!(row[9].contains("<redacted>"));
        assert!(!row[9].contains("SELECT * FROM users"));
        assert!(!row[9].contains("secret-bind-1"));
    }

    let audit_frames = admin_query(admin_addr, "SHOW POLICY AUDIT").await;
    assert_admin_table_columns(
        &audit_frames,
        &[
            "kind",
            "policy_id",
            "policy_version",
            "hook_point",
            "action",
            "outcome",
            "reason",
            "route",
            "shard",
            "target_role",
            "context",
        ],
    );
    let audit_rows = data_rows(&audit_frames);
    assert_eq!(audit_rows.len(), 128);
    assert_eq!(audit_rows.first().expect("first audit")[2], "4");
    assert_eq!(audit_rows.last().expect("last audit")[0], "validation");
    assert_eq!(audit_rows.last().expect("last audit")[2], "999");
    for row in &audit_rows {
        assert!(row[10].contains("<redacted>"));
        assert!(!row[10].contains("SELECT * FROM users"));
        assert!(!row[10].contains("secret-bind-1"));
    }

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn disabled_policy_mode_shows_no_active_evaluator() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let (run_handle, _, snapshot_store) =
        spawn_proxy(test_config(Some(admin_addr), Some("admin"), backend_addr)).await;

    snapshot_store.set_policy_status_snapshot(PolicyStatusSnapshot::new(
        "route-fallback",
        8,
        PolicyMode::Disabled,
        "inline",
    ));
    snapshot_store.set_policy_reload_snapshot(PolicyReloadSnapshot {
        policy_generation_id: 8,
        success: true,
        error_code: None,
        error: None,
    });

    let policies_frames = admin_query(admin_addr, "SHOW POLICIES").await;
    assert_admin_table_response(
        &policies_frames,
        &[
            "policy_id",
            "policy_version",
            "policy_mode",
            "source",
            "enabled",
            "last_reload_outcome",
            "error_code",
        ],
        &[vec![
            "route-fallback",
            "8",
            "disabled",
            "inline",
            "false",
            "success",
            "<none>",
        ]],
    );

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[test]
fn policy_metrics_use_bounded_labels() {
    let _guard = metrics_lock().lock().expect("metrics lock");
    let recorder = install_metrics_recorder();
    recorder.clear();

    let snapshot_store = SnapshotStore::default();
    let runtime = PolicyRuntime::new(Duration::from_millis(5), 8_192)
        .with_policy_mode(PolicyMode::Enforce)
        .with_policy_audit_enabled(true)
        .with_policy_audit_sample_rate(1.0);
    let input = sample_policy_input();

    let decision_event = policy_event(
        &runtime,
        PolicyAuditKind::Decision,
        17,
        PolicyOutcome::Rejected,
        &input,
    );
    assert!(runtime.record_audit_event(&snapshot_store, &decision_event));

    let validation_event = policy_event(
        &runtime,
        PolicyAuditKind::Validation,
        18,
        PolicyOutcome::Rejected,
        &input,
    );
    assert!(runtime.record_audit_event(&snapshot_store, &validation_event));

    let preview_config = PolicyConfig {
        policy_mode: PolicyMode::Enforce,
        inline_rules: vec![InlinePolicyConfig {
            policy_id: PolicyId::new("route-fallback").expect("policy id"),
            hook_point: PolicyHookPoint::BeforeRouting,
            action: InlinePolicyActionConfig::Deny {
                reason: String::from("policy denied"),
            },
        }],
        ..PolicyConfig::default()
    };
    let preview = preview_policy(&preview_config, false, &input).expect("preview policy");
    assert_eq!(preview.policy_mode, PolicyMode::Enforce);

    let reload_store = PolicyStore::new(PolicyConfig::default());
    let reload_result = reload_store.reload(
        &PolicyConfig {
            policy_mode: PolicyMode::Enforce,
            inline_rules: vec![InlinePolicyConfig {
                policy_id: PolicyId::new("route-fallback").expect("policy id"),
                hook_point: PolicyHookPoint::BeforeRouting,
                action: InlinePolicyActionConfig::RouteOverride {
                    target_id: pg_kinetic::core::policy::PolicyRouteTargetId::new("missing-route")
                        .expect("target id"),
                },
            }],
            ..PolicyConfig::default()
        },
        std::iter::empty::<&str>(),
        false,
        std::iter::empty::<&str>(),
    );
    assert!(!reload_result.success);

    #[cfg(feature = "policy-wasm")]
    {
        let module_path = wasm_module_path(
            "policy-metrics",
            r#"
            (module
              (memory (export "memory") 1)
              (func (export "pg_kinetic_policy_abi_version") (result i32)
                i32.const 1)
              (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
                i32.const 0)
            )
            "#,
        );
        let wasm_runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
            .with_policy_mode(PolicyMode::Enforce)
            .with_policy_wasm_enabled(true);
        let wasm_rule = InlinePolicyConfig {
            policy_id: PolicyId::new("wasm-rule").expect("policy id"),
            hook_point: PolicyHookPoint::BeforeCheckout,
            action: InlinePolicyActionConfig::Wasm { module_path },
        };
        let wasm_input = sample_policy_input();
        let wasm_decision = wasm_runtime
            .evaluate_wasm_policy(&wasm_rule, &wasm_input)
            .expect("wasm policy evaluation");
        assert_eq!(wasm_decision.outcome, PolicyOutcome::Applied);
    }

    assert!(recorder.has_metric(
        "pg_kinetic_policy_decisions_total",
        &[
            ("policy", "route-fallback"),
            ("mode", "enforce"),
            ("hook", "before_routing"),
            ("action", "deny"),
            ("outcome", "rejected"),
        ]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_policy_eval_duration_ms",
        &[
            ("policy", "route-fallback"),
            ("mode", "enforce"),
            ("hook", "before_routing"),
            ("outcome", "rejected"),
        ]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_policy_denies_total",
        &[("policy", "route-fallback"), ("reason", "policy_denied")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_policy_dry_run_total",
        &[
            ("policy", "route-fallback"),
            ("mode", "dry_run"),
            ("hook", "before_routing"),
            ("action", "deny"),
        ]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_policy_reload_total",
        &[
            ("source", "inline"),
            ("mode", "enforce"),
            ("outcome", "success"),
            ("error_code", "none"),
        ]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_policy_reload_total",
        &[
            ("source", "inline"),
            ("mode", "enforce"),
            ("outcome", "failure"),
            ("error_code", "route_reference_missing"),
        ]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_policy_active",
        &[("source", "inline"), ("mode", "enforce")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_policy_audit_events_total",
        &[
            ("policy", "route-fallback"),
            ("mode", "enforce"),
            ("hook", "before_routing"),
            ("action", "deny"),
            ("outcome", "rejected"),
            ("reason", "policy_denied"),
        ]
    ));

    #[cfg(feature = "policy-wasm")]
    {
        assert!(recorder.has_metric(
            "pg_kinetic_policy_wasm_eval_total",
            &[
                ("source", "wasm"),
                ("mode", "enforce"),
                ("hook", "before_checkout"),
                ("outcome", "applied"),
                ("error_code", "none"),
            ]
        ));
        assert!(recorder.has_metric(
            "pg_kinetic_policy_wasm_eval_duration_ms",
            &[
                ("source", "wasm"),
                ("mode", "enforce"),
                ("hook", "before_checkout"),
                ("outcome", "applied"),
            ]
        ));
    }

    assert_no_sensitive_metric_labels(&recorder);
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

fn sample_policy_input() -> pg_kinetic::proxy_runtime::policy::PolicyEvalInput {
    pg_kinetic::proxy_runtime::policy::PolicyEvalInput {
        database: Arc::from("billing"),
        user: Arc::from("reporter"),
        application_name: Some(Arc::from("dashboard")),
        route: Some(Arc::from("billing/reporter/dashboard/default")),
        shard: None,
        backend_role: BackendRole::Primary,
        query_class: QueryClass::Unknown,
        transaction_mode: TransactionAccessMode::ReadWrite,
        freshness_state: FreshnessStatus::Satisfied,
        routing_decision: None,
        shard_route_decision: None,
        password: Some(Arc::from("topsecret")),
        bind_values: vec![Arc::from("secret-bind-1")],
        tls_certificate_body: None,
        raw_sql_text: Some(Arc::from("SELECT * FROM users WHERE password = $1")),
        secrets: vec![Arc::from("token=abc123")],
    }
}

fn policy_event(
    runtime: &PolicyRuntime,
    kind: PolicyAuditKind,
    policy_version: u64,
    outcome: PolicyOutcome,
    input: &pg_kinetic::proxy_runtime::policy::PolicyEvalInput,
) -> PolicyAuditEvent {
    let decision = PolicyDecision::new(
        PolicyId::new("route-fallback").expect("policy id"),
        PolicyVersion::new(policy_version).expect("policy version"),
        PolicyAction::deny(),
        outcome,
        PolicyHookPoint::BeforeRouting,
        Duration::from_millis(1),
    );
    runtime.build_audit_event_from_input(kind, decision, input)
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
    assert_admin_table_columns(frames, expected_columns);
    let data_rows = data_rows(frames);
    let expected_rows = expected_rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(data_rows, expected_rows);
    assert!(frames.iter().any(|frame| frame.tag == b'C'));
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
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

fn row_description_columns(frame: &BackendFrame) -> Vec<String> {
    assert_eq!(frame.tag, b'T');
    let payload = frame.payload.as_ref();
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

fn data_rows(frames: &[BackendFrame]) -> Vec<Vec<String>> {
    frames
        .iter()
        .filter(|frame| frame.tag == b'D')
        .map(data_row_values)
        .collect()
}

fn data_row_values(frame: &BackendFrame) -> Vec<String> {
    assert_eq!(frame.tag, b'D');
    let payload = frame.payload.as_ref();
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

fn startup_packet(user: &str) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_i32(0);
    bytes.put_i32(ProtocolVersion::V3.to_i32());
    bytes.extend_from_slice(b"user\0");
    bytes.extend_from_slice(user.as_bytes());
    bytes.extend_from_slice(b"\0database\0pgkinetic\0\0");
    let len = bytes.len() as i32;
    bytes[..4].copy_from_slice(&len.to_be_bytes());
    bytes
}

fn query_packet(sql: &str) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'Q');
    bytes.put_i32((sql.len() + 5) as i32);
    bytes.extend_from_slice(sql.as_bytes());
    bytes.put_u8(0);
    bytes
}

async fn read_until_ready(stream: &mut TcpStream) -> Vec<BackendFrame> {
    let mut frames = Vec::new();
    let mut buffer = BytesMut::with_capacity(8 * 1024);
    loop {
        let mut chunk = [0_u8; 512];
        let bytes_read = stream.read(&mut chunk).await.expect("read admin frame");
        assert!(bytes_read > 0, "admin stream closed before ready");
        buffer.extend_from_slice(&chunk[..bytes_read]);

        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            if frame.ready_status() == Some(ReadyStatus::Idle) {
                frames.push(frame);
                return frames;
            }
            frames.push(frame);
        }
    }
}

async fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port");
    let addr = listener.local_addr().expect("free port");
    drop(listener);
    addr
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

fn metrics_lock() -> &'static Mutex<()> {
    METRICS_LOCK.get_or_init(|| Mutex::new(()))
}

fn assert_no_sensitive_metric_labels(recorder: &TestRecorder) {
    let forbidden = [
        "SELECT * FROM users",
        "secret-bind-1",
        "topsecret",
        "-----BEGIN CERTIFICATE-----",
        "127.0.0.1",
        "tenant-a",
        "tenant-b",
        "tenant-c",
        "missing-route",
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

#[cfg(feature = "policy-wasm")]
fn wasm_module_path(label: &str, source: &str) -> std::path::PathBuf {
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("pg-kinetic-{label}-{unique_suffix}.wat"));
    std::fs::write(&path, source).expect("write wasm module");
    path
}

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();
static METRICS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
