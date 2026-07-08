use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use ::metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use pg_kinetic::{
    core::{
        ha::{
            EndpointHealth, EndpointRoleState, HealthProbeOutcome, ReplicaLagState,
            RoleProbeOutcome,
        },
        lsn::{FreshnessStatus, PgLsn},
        routing::BackendRole,
    },
    proxy_runtime::{
        metrics as proxy_metrics,
        routing::{ReplicaCandidate, RoutingReason, RoutingTarget},
        snapshot::{ReplicaHealthSnapshot, RouteCheckoutSnapshot, SnapshotStore},
    },
    route::{QueryClass, RouteKey},
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
            ("query_class", "read"),
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

fn route_key(application_name: &str) -> RouteKey {
    RouteKey::new(
        "postgres",
        "pgkinetic",
        Some(application_name),
        Some("127.0.0.1:5432".parse().expect("socket address")),
        QueryClass::Read,
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
