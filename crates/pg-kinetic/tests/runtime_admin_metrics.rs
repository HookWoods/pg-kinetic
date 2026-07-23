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
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, CapacityConfig,
        ClientTlsMode, Config, ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig,
        PerformanceConfig, QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    core::{
        adaptive::{
            AdaptiveAction, AdaptiveMode, AdaptiveOutcome, AdaptiveSignal, TunableKnob, TuningBound,
        },
        benchmark::{
            BenchmarkComparison, BenchmarkDriver, BenchmarkMetric, BenchmarkResult,
            BenchmarkScenario, BenchmarkTarget,
        },
        control::PeerHealth,
        mirror::MirrorMode,
        performance::{
            BenchmarkTarget as PerformanceBenchmarkTarget, PerformanceBudget,
            PerformanceBudgetOutcome, PerformanceMetric, PerformanceRegressionResult,
            PerformanceRegressionThreshold, ProcessMetricCollectionStatus, ProcessMetricKind,
            ProcessMetricSample, ProcessMetricValue, ProfileCaptureStatus,
        },
        runtime::{NodeId, ReadinessState, RuntimeEngine, RuntimeLifecycleState, ShutdownReason},
    },
    proxy::Proxy,
    proxy_runtime::{
        metrics as proxy_metrics,
        snapshot::{
            AdaptiveOutcomeSnapshot, AdaptiveRecommendationSnapshot, BenchmarkRunSnapshot,
            MirrorSummarySnapshot, NodeSummaryRole, NodeSummarySnapshot, PerformanceSnapshot,
            RuntimeSnapshot, SnapshotStore,
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
    time,
};

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();
static ASYNC_METRICS_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[tokio::test(flavor = "current_thread")]
async fn show_runtime_views_cover_telemetry_and_redaction() {
    let _guard = async_metrics_lock().lock().await;
    let recorder = install_metrics_recorder();
    recorder.clear();

    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let mut config = test_config(Some(admin_addr), Some("admin"), backend_addr);
    config.runtime.production.adaptive.adaptive_mode = AdaptiveMode::Apply;
    config
        .runtime
        .production
        .adaptive
        .apply
        .adaptive_apply_enabled = true;
    config
        .runtime
        .production
        .adaptive
        .apply
        .adaptive_apply_allowlist = vec![TunableKnob::MirrorSampling];
    config
        .runtime
        .production
        .adaptive
        .guardrail
        .adaptive_max_change_percent = 20;
    let (run_handle, _, snapshot_store) = spawn_proxy(config).await;

    let node_id = NodeId::new("node-a").expect("node id");
    let runtime_snapshot = RuntimeSnapshot::new(
        node_id.clone(),
        RuntimeLifecycleState::Ready,
        ReadinessState::Ready,
        RuntimeEngine::TokioCurrentThread,
        Duration::from_secs(73),
    );
    let mut runtime_snapshot = runtime_snapshot;
    runtime_snapshot.shutdown_reason = Some(ShutdownReason::AdminRequest);
    snapshot_store.set_runtime_snapshot(runtime_snapshot.clone());

    snapshot_store.set_node_snapshot(NodeSummarySnapshot {
        role: NodeSummaryRole::Local,
        node_id: node_id.clone(),
        lifecycle_state: RuntimeLifecycleState::Ready,
        readiness_state: ReadinessState::Ready,
        health: PeerHealth::Healthy,
        route_map_generation: 41,
        policy_generation: 7,
        heartbeat_age: Duration::from_millis(250),
        overloaded: false,
    });
    snapshot_store.set_node_snapshot(NodeSummarySnapshot {
        role: NodeSummaryRole::Peer,
        node_id: NodeId::new("node-b").expect("peer node id"),
        lifecycle_state: RuntimeLifecycleState::Draining,
        readiness_state: ReadinessState::Draining,
        health: PeerHealth::Overloaded,
        route_map_generation: 42,
        policy_generation: 8,
        heartbeat_age: Duration::from_secs(3),
        overloaded: true,
    });

    let mut mirror_snapshot = MirrorSummarySnapshot::new(MirrorMode::ReadOnly, 0.125);
    mirror_snapshot.in_flight = 6;
    mirror_snapshot.dropped_total = 1;
    mirror_snapshot.timed_out_total = 2;
    mirror_snapshot.mirrored_total = 3;
    mirror_snapshot.skipped_total = 4;
    mirror_snapshot.rejected_total = 5;
    mirror_snapshot.last_duration = Some(Duration::from_millis(17));
    snapshot_store.set_mirror_snapshot(mirror_snapshot);

    snapshot_store.record_adaptive_recommendation(AdaptiveRecommendationSnapshot {
        signal: AdaptiveSignal::MirrorSamplingPressure,
        action: AdaptiveAction::Apply,
        knob: TunableKnob::MirrorSampling,
        confidence: 0.875,
        reason: String::from("SELECT * FROM sensitive_table"),
        window: Duration::from_secs(30),
        safety_bound: TuningBound::percent(20),
    });
    snapshot_store.record_adaptive_outcome(AdaptiveOutcomeSnapshot {
        signal: AdaptiveSignal::MirrorSamplingPressure,
        knob: TunableKnob::MirrorSampling,
        outcome: AdaptiveOutcome::Applied,
        reason: String::from("applied after analysis"),
        before_value: Some(10.0),
        after_value: Some(12.5),
        change_percent: Some(20),
        disabled_by_reload: false,
    });

    let benchmark_target = BenchmarkTarget::new(
        "primary",
        BenchmarkComparison::PgKinetic,
        "postgres://benchmark_user:secret-password@127.0.0.1:5432/pgkinetic",
    )
    .expect("benchmark target");
    let direct_benchmark_target = BenchmarkTarget::new(
        "direct",
        BenchmarkComparison::DirectPostgreSQL,
        "postgres://benchmark_user:secret-password@127.0.0.1:5432/postgres",
    )
    .expect("direct benchmark target");
    let benchmark_scenario = BenchmarkScenario::new(
        "SELECT * FROM sensitive_table -- secret-password",
        BenchmarkDriver::PgBench,
        12_000,
        1_000,
        vec![direct_benchmark_target, benchmark_target.clone()],
    )
    .expect("benchmark scenario");
    let benchmark_result = BenchmarkResult::new(
        benchmark_scenario.name(),
        benchmark_target,
        BenchmarkDriver::PgBench,
        12_000,
        BenchmarkMetric::new(1.25, 2.5, 3.0, 4_500.0, "c7g.large", "4GiB", 0.005)
            .expect("benchmark metrics"),
    )
    .expect("benchmark result");
    snapshot_store.record_benchmark_run(BenchmarkRunSnapshot::new(
        benchmark_scenario,
        vec![benchmark_result],
    ));
    snapshot_store.set_performance_snapshot(PerformanceSnapshot {
        budgets: vec![PerformanceBudget::new(
            PerformanceMetric::LatencyP95,
            PerformanceRegressionThreshold::Percentage(5.0),
            PerformanceRegressionThreshold::Percentage(10.0),
        )],
        regressions: vec![PerformanceRegressionResult::new(
            "SELECT * FROM sensitive_table -- secret-password",
            PerformanceBenchmarkTarget::PgKinetic,
            PerformanceMetric::LatencyP95,
            2.5,
            Some(2.0),
            PerformanceBudgetOutcome::Failed,
        )],
        profile_status: ProfileCaptureStatus::Captured,
        process_status: ProcessMetricCollectionStatus::Complete,
        process_sample: Some(ProcessMetricSample::new(
            1,
            [
                (ProcessMetricKind::CpuTime, ProcessMetricValue::Float(12.0)),
                (
                    ProcessMetricKind::ResidentMemory,
                    ProcessMetricValue::Integer(4_096),
                ),
            ],
        )),
        cpu_per_query: Some(0.012),
        memory_per_client_bytes: Some(512.0),
        protocol_buffer_copies: 7,
        pool_checkout_lock_wait_ms: Some(3.5),
        prepared_cache_hits: 9,
        prepared_cache_misses: 2,
        observability_hot_path_allocations: 1,
        idle_clients: 2,
    });

    let runtime_frames = admin_query(admin_addr, "SHOW RUNTIME").await;
    assert_admin_table_response(
        &runtime_frames,
        &[
            "node_id",
            "lifecycle_state",
            "readiness_state",
            "runtime_engine",
            "uptime_ms",
        ],
        &[vec![
            "node-a",
            "ready",
            "ready",
            "tokio_current_thread",
            "73000",
        ]],
    );

    let nodes_frames = admin_query(admin_addr, "SHOW NODES").await;
    assert_admin_table_response(
        &nodes_frames,
        &[
            "role",
            "node_id",
            "lifecycle_state",
            "readiness_state",
            "health",
            "route_map_generation_id",
            "policy_generation_id",
            "heartbeat_age_ms",
            "overloaded",
        ],
        &[
            vec![
                "local", "node-a", "ready", "ready", "healthy", "41", "7", "250", "false",
            ],
            vec![
                "peer",
                "node-b",
                "draining",
                "draining",
                "overloaded",
                "42",
                "8",
                "3000",
                "true",
            ],
        ],
    );

    let mirroring_frames = admin_query(admin_addr, "SHOW MIRRORING").await;
    assert_admin_table_response(
        &mirroring_frames,
        &[
            "mode",
            "sample_rate",
            "in_flight",
            "dropped",
            "timeout_total",
            "decisions_total",
            "mirrored_total",
            "skipped_total",
            "rejected_total",
        ],
        &[vec![
            "read_only",
            "0.125",
            "6",
            "1",
            "2",
            "15",
            "3",
            "4",
            "5",
        ]],
    );

    let adaptive_frames = admin_query(admin_addr, "SHOW ADAPTIVE").await;
    assert_admin_table_response(
        &adaptive_frames,
        &[
            "mode",
            "latest_recommendation",
            "apply_status",
            "guardrails",
        ],
        &[vec![
            "apply",
            "mirror_sampling_pressure:mirror_sampling:0.875",
            "applied",
            "mode=apply;apply=true;allowlist=mirror_sampling;max_change=20%",
        ]],
    );

    let benchmark_frames = admin_query(admin_addr, "SHOW BENCHMARKS").await;
    assert_admin_table_response(
        &benchmark_frames,
        &[
            "scenario",
            "target",
            "comparison",
            "driver",
            "duration_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "throughput_qps",
            "error_rate",
            "cpu_label",
            "memory_label",
            "workload",
            "matrix_targets",
            "comparison_outcome",
        ],
        &[vec![
            "configured",
            "pg_kinetic",
            "pg_kinetic",
            "pgbench",
            "12000",
            "1.250",
            "2.500",
            "3.000",
            "4500.000",
            "0.005",
            "redacted",
            "redacted",
            "simple_query",
            "direct_postgresql,pg_kinetic",
            "failed",
        ]],
    );

    let performance_frames = admin_query(admin_addr, "SHOW PERFORMANCE").await;
    assert_admin_table_response(
        &performance_frames,
        &[
            "metric",
            "warning_threshold",
            "failure_threshold",
            "observed_value",
            "baseline_value",
            "regression_outcome",
            "profile_status",
            "process_status",
            "process_cpu_seconds",
            "process_resident_memory_bytes",
            "cpu_per_query",
            "memory_per_client_bytes",
            "protocol_buffer_copies",
            "pool_checkout_lock_wait_ms",
            "prepared_cache_hits",
            "prepared_cache_misses",
            "observability_hot_path_allocations",
            "idle_clients",
        ],
        &[vec![
            "latency_p95",
            "percentage:5.000",
            "percentage:10.000",
            "2.500",
            "2.000",
            "failed",
            "captured",
            "complete",
            "12.000",
            "4096.000",
            "0.012",
            "512.000",
            "7",
            "3.500",
            "9",
            "2",
            "1",
            "2",
        ]],
    );

    let redaction_text = format!(
        "{}{}{}",
        table_text(&adaptive_frames),
        table_text(&benchmark_frames),
        table_text(&performance_frames)
    );
    for forbidden in [
        "SELECT * FROM sensitive_table",
        "secret-password",
        "postgres://benchmark_user",
        "127.0.0.1:5432",
    ] {
        assert!(
            !redaction_text.contains(forbidden),
            "sensitive text leaked into admin output: {forbidden}"
        );
    }

    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test(flavor = "current_thread")]
async fn metric_labels_stay_low_cardinality() {
    let _guard = async_metrics_lock().lock().await;
    let recorder = install_metrics_recorder();
    recorder.clear();

    let store = SnapshotStore::new();
    let runtime_node = NodeId::new("node-a").expect("runtime node id");
    let peer_node = NodeId::new("node-b").expect("peer node id");

    let mut runtime_snapshot = RuntimeSnapshot::new(
        runtime_node.clone(),
        RuntimeLifecycleState::Ready,
        ReadinessState::Ready,
        RuntimeEngine::TokioCurrentThread,
        Duration::from_secs(5),
    );
    runtime_snapshot.shutdown_reason = Some(ShutdownReason::AdminRequest);
    store.set_runtime_snapshot(runtime_snapshot);

    store.set_node_snapshot(NodeSummarySnapshot {
        role: NodeSummaryRole::Local,
        node_id: runtime_node,
        lifecycle_state: RuntimeLifecycleState::Ready,
        readiness_state: ReadinessState::Ready,
        health: PeerHealth::Healthy,
        route_map_generation: 10,
        policy_generation: 11,
        heartbeat_age: Duration::from_secs(2),
        overloaded: false,
    });
    store.set_node_snapshot(NodeSummarySnapshot {
        role: NodeSummaryRole::Peer,
        node_id: peer_node,
        lifecycle_state: RuntimeLifecycleState::Draining,
        readiness_state: ReadinessState::Draining,
        health: PeerHealth::Overloaded,
        route_map_generation: 12,
        policy_generation: 13,
        heartbeat_age: Duration::from_secs(4),
        overloaded: true,
    });

    let mut mirror_snapshot = MirrorSummarySnapshot::new(MirrorMode::ReadOnly, 0.5);
    mirror_snapshot.in_flight = 3;
    mirror_snapshot.dropped_total = 2;
    mirror_snapshot.timed_out_total = 4;
    mirror_snapshot.mirrored_total = 5;
    mirror_snapshot.skipped_total = 6;
    mirror_snapshot.rejected_total = 7;
    mirror_snapshot.last_duration = Some(Duration::from_millis(9));
    store.set_mirror_snapshot(mirror_snapshot);

    store.record_adaptive_recommendation(AdaptiveRecommendationSnapshot {
        signal: AdaptiveSignal::BackpressureThresholdPressure,
        action: AdaptiveAction::Apply,
        knob: TunableKnob::BackpressureThresholds,
        confidence: 0.9,
        reason: String::from("bounded"),
        window: Duration::from_secs(15),
        safety_bound: TuningBound::percent(10),
    });
    store.record_adaptive_outcome(AdaptiveOutcomeSnapshot {
        signal: AdaptiveSignal::BackpressureThresholdPressure,
        knob: TunableKnob::BackpressureThresholds,
        outcome: AdaptiveOutcome::Applied,
        reason: String::from("applied"),
        before_value: Some(10.0),
        after_value: Some(11.0),
        change_percent: Some(10),
        disabled_by_reload: false,
    });

    let benchmark_target = BenchmarkTarget::new(
        "primary",
        BenchmarkComparison::PgKinetic,
        "postgres://benchmark_user:secret-password@127.0.0.1:5432/pgkinetic",
    )
    .expect("benchmark target");
    let direct_benchmark_target = BenchmarkTarget::new(
        "direct",
        BenchmarkComparison::DirectPostgreSQL,
        "postgres://benchmark_user:secret-password@127.0.0.1:5432/postgres",
    )
    .expect("direct benchmark target");
    let benchmark_scenario = BenchmarkScenario::new(
        "nightly_latency",
        BenchmarkDriver::PgBench,
        12_000,
        1_000,
        vec![direct_benchmark_target, benchmark_target.clone()],
    )
    .expect("benchmark scenario");
    let benchmark_result = BenchmarkResult::new(
        benchmark_scenario.name(),
        benchmark_target,
        BenchmarkDriver::PgBench,
        12_000,
        BenchmarkMetric::new(1.25, 2.5, 3.0, 4_500.0, "c7g.large", "4GiB", 0.005)
            .expect("benchmark metrics"),
    )
    .expect("benchmark result");
    store.record_benchmark_run(BenchmarkRunSnapshot::new(
        benchmark_scenario,
        vec![benchmark_result],
    ));
    store.set_performance_snapshot(PerformanceSnapshot {
        regressions: vec![PerformanceRegressionResult::new(
            "nightly_latency",
            PerformanceBenchmarkTarget::PgKinetic,
            PerformanceMetric::LatencyP95,
            2.5,
            Some(2.0),
            PerformanceBudgetOutcome::Failed,
        )],
        profile_status: ProfileCaptureStatus::Captured,
        process_status: ProcessMetricCollectionStatus::Complete,
        process_sample: Some(ProcessMetricSample::new(
            1,
            [
                (ProcessMetricKind::CpuTime, ProcessMetricValue::Float(12.0)),
                (
                    ProcessMetricKind::ResidentMemory,
                    ProcessMetricValue::Integer(4_096),
                ),
            ],
        )),
        cpu_per_query: Some(0.012),
        memory_per_client_bytes: Some(512.0),
        protocol_buffer_copies: 7,
        pool_checkout_lock_wait_ms: Some(3.5),
        prepared_cache_hits: 9,
        prepared_cache_misses: 2,
        observability_hot_path_allocations: 1,
        idle_clients: 2,
        ..Default::default()
    });
    proxy_metrics::record_pool_checkout_lock_wait(3.5);

    proxy_metrics::record_preflight_finding("tls_files", "warning");
    proxy_metrics::record_preflight_finding("lifecycle_guardrails", "error");

    assert!(recorder.has_metric("pg_kinetic_runtime_lifecycle_state", &[("state", "ready")],));
    assert!(recorder.has_metric("pg_kinetic_runtime_readiness_state", &[("state", "ready")],));
    assert!(recorder.has_metric(
        "pg_kinetic_runtime_shutdown_total",
        &[("reason", "admin_request")],
    ));
    assert!(recorder.has_metric("pg_kinetic_node_heartbeat_age_ms", &[("node", "node-a")],));
    assert!(recorder.has_metric("pg_kinetic_node_heartbeat_age_ms", &[("node", "node-b")],));
    assert!(recorder.has_metric(
        "pg_kinetic_mirror_in_flight",
        &[("mode", "read_only"), ("target", "mirror")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_mirror_dropped_total",
        &[("mode", "read_only"), ("reason", "sampled_out")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_mirror_decisions_total",
        &[
            ("mode", "read_only"),
            ("target", "mirror"),
            ("outcome", "mirrored")
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_mirror_duration_ms",
        &[
            ("mode", "read_only"),
            ("target", "mirror"),
            ("outcome", "mirrored")
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_adaptive_recommendations_total",
        &[
            ("mode", "recommend"),
            ("target", "backpressure_thresholds"),
            ("outcome", "apply"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_adaptive_apply_total",
        &[
            ("mode", "apply"),
            ("target", "backpressure_thresholds"),
            ("outcome", "applied"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_benchmark_runs_total",
        &[
            ("engine", "pgbench"),
            ("target", "pg_kinetic"),
            ("outcome", "ok")
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_benchmark_latency_ms",
        &[
            ("scenario", "configured"),
            ("target", "pg_kinetic"),
            ("workload", "simple_query"),
            ("driver", "pgbench"),
            ("metric", "p95"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_benchmark_throughput_qps",
        &[
            ("scenario", "configured"),
            ("target", "pg_kinetic"),
            ("workload", "simple_query"),
            ("driver", "pgbench"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_benchmark_errors_total",
        &[
            ("scenario", "configured"),
            ("target", "pg_kinetic"),
            ("workload", "simple_query"),
            ("driver", "pgbench"),
            ("outcome", "nonzero_error_rate"),
        ],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_performance_budget_status",
        &[("metric", "latency_p95"), ("outcome", "failed")],
    ));
    assert!(recorder.has_metric("pg_kinetic_process_cpu_seconds", &[]));
    assert!(recorder.has_metric("pg_kinetic_process_resident_memory_bytes", &[],));
    assert!(recorder.has_metric("pg_kinetic_cpu_per_query", &[]));
    assert!(recorder.has_metric("pg_kinetic_memory_per_client_bytes", &[]));
    assert!(recorder.has_metric(
        "pg_kinetic_protocol_buffer_copies_total",
        &[("feature", "protocol")],
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_pool_checkout_lock_wait_ms",
        &[("outcome", "ok")],
    ));
    assert!(recorder.has_metric("pg_kinetic_prepared_cache_hits_total", &[]));
    assert!(recorder.has_metric("pg_kinetic_prepared_cache_misses_total", &[]));
    assert!(recorder.has_metric(
        "pg_kinetic_observability_hot_path_allocations_total",
        &[("feature", "metrics")],
    ));
    assert!(recorder.has_metric("pg_kinetic_idle_clients", &[]));
    assert!(recorder.has_metric(
        "pg_kinetic_preflight_findings_total",
        &[("check", "tls_files"), ("severity", "warning")],
    ));

    assert_no_sensitive_metric_labels(&recorder);
}

fn async_metrics_lock() -> &'static tokio::sync::Mutex<()> {
    ASYNC_METRICS_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
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

fn assert_no_sensitive_metric_labels(recorder: &TestRecorder) {
    let forbidden = [
        "SELECT * FROM sensitive_table",
        "secret-password",
        "postgres://",
        "127.0.0.1:5432",
        "client_addr",
        "password",
        "sql_text",
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
        pools: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 1,
            max_checkout_waiters: 4,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 250,
            pool_mode: Default::default(),
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
            auth_query_enabled: false,
            auth_query: String::from("SELECT usename, passwd FROM pg_shadow WHERE usename = $1"),
            auth_query_cache_ttl_ms: 60_000,
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
    assert!(frames.iter().any(|frame| frame.tag == b'C'));
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
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
