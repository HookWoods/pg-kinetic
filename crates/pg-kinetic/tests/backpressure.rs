use metrics::{Counter, Gauge, Histogram, Key, Metadata, Recorder};
use pg_kinetic::backpressure::{
    BackpressureCoordinator, BackpressureError, BackpressureGate, RouteBackpressureSnapshot,
    RouteFairnessConfig, RoutePriority,
};
use pg_kinetic::route::{QueryClass, RouteKey};
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();

fn route_key(application_name: &str) -> RouteKey {
    RouteKey::new(
        "postgres",
        "pgkinetic",
        Some(application_name),
        None,
        QueryClass::Default,
    )
}

#[test]
fn qos_metric_labels_are_stable() {
    let recorder = install_metrics_recorder();
    recorder.clear();

    let route = route_key("api-a");
    let route_label = route.metric_label();

    for outcome in [
        "ok",
        "queue_full",
        "timeout",
        "canceled",
        "buffer_limit",
        "idle_timeout",
        "query_timeout",
    ] {
        pg_kinetic::metrics::increment_backpressure_event(&route, outcome);
    }

    pg_kinetic::metrics::record_route_wait(&route, 12.5, "ok");
    pg_kinetic::metrics::record_route_in_flight(&route, 3);
    pg_kinetic::metrics::record_route_waiting(&route, 4);
    pg_kinetic::metrics::increment_route_shed(&route, "sheddable");
    pg_kinetic::metrics::increment_timeout("idle_timeout");
    pg_kinetic::metrics::increment_timeout("query_timeout");
    pg_kinetic::metrics::increment_buffer_limit("buffer_limit");

    assert!(recorder.has_metric(
        "pg_kinetic_backpressure_events_total",
        &[("route", route_label.as_str()), ("outcome", "ok")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_backpressure_events_total",
        &[("route", route_label.as_str()), ("outcome", "queue_full")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_backpressure_events_total",
        &[("route", route_label.as_str()), ("outcome", "timeout")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_backpressure_events_total",
        &[("route", route_label.as_str()), ("outcome", "canceled")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_backpressure_events_total",
        &[("route", route_label.as_str()), ("outcome", "buffer_limit")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_backpressure_events_total",
        &[("route", route_label.as_str()), ("outcome", "idle_timeout")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_backpressure_events_total",
        &[
            ("route", route_label.as_str()),
            ("outcome", "query_timeout")
        ]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_route_checkout_wait_ms",
        &[("route", route_label.as_str()), ("outcome", "ok")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_route_in_flight",
        &[("route", route_label.as_str()), ("scope", "route")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_route_waiting",
        &[("route", route_label.as_str()), ("scope", "route")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_route_shed_total",
        &[("route", route_label.as_str()), ("priority", "sheddable")]
    ));
    assert!(recorder.has_metric("pg_kinetic_timeout_total", &[("kind", "idle_timeout")]));
    assert!(recorder.has_metric("pg_kinetic_timeout_total", &[("kind", "query_timeout")]));
    assert!(recorder.has_metric("pg_kinetic_buffer_limit_total", &[("kind", "buffer_limit")]));
}

#[tokio::test]
async fn grants_capacity_when_slot_available() {
    let gate = BackpressureGate::new(1, 1);

    let permit = gate
        .checkout(Duration::from_millis(10))
        .await
        .expect("permit granted");

    assert_eq!(gate.in_flight(), 1);
    drop(permit);
    assert_eq!(gate.in_flight(), 0);
}

#[tokio::test]
async fn dynamic_gate_ceiling_lowers_without_revoking_permits() {
    let limit = Arc::new(AtomicUsize::new(2));
    let gate = BackpressureGate::with_dynamic_limit(4, 1, Arc::clone(&limit));
    let first = gate
        .checkout(Duration::from_millis(10))
        .await
        .expect("first permit");
    let second = gate
        .checkout(Duration::from_millis(10))
        .await
        .expect("second permit");

    gate.set_limit(1);

    assert_eq!(gate.in_flight(), 2);
    assert_eq!(gate.limit(), 1);
    drop(second);
    assert_eq!(gate.in_flight(), 1);
    drop(first);
    assert_eq!(gate.in_flight(), 0);
}

#[tokio::test]
async fn dynamic_gate_restore_never_exceeds_configured_capacity() {
    let limit = Arc::new(AtomicUsize::new(1));
    let gate = BackpressureGate::with_dynamic_limit(3, 1, Arc::clone(&limit));

    gate.set_limit(99);

    assert_eq!(gate.limit(), 3);
    assert_eq!(limit.load(Ordering::Acquire), 3);
}

#[tokio::test]
async fn two_route_keys_have_independent_in_flight_limits() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let route_a = route_key("api-a");
    let route_b = route_key("api-b");

    let permit_a = coordinator
        .checkout(route_a.clone(), Duration::from_millis(10))
        .await
        .expect("route a permit granted");

    let permit_b = coordinator
        .checkout(route_b.clone(), Duration::from_millis(10))
        .await
        .expect("route b permit granted");

    assert_eq!(coordinator.route_snapshot(&route_a).in_flight, 1);
    assert_eq!(coordinator.route_snapshot(&route_b).in_flight, 1);
    assert_eq!(coordinator.global_snapshot().in_flight, 2);

    drop(permit_b);
    drop(permit_a);
}

#[tokio::test]
async fn one_saturated_route_key_does_not_block_an_idle_route_key() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let saturated = route_key("api-a");
    let idle = route_key("api-b");

    let _held = coordinator
        .checkout(saturated.clone(), Duration::from_millis(10))
        .await
        .expect("first route permit granted");

    let permit = coordinator
        .checkout(idle.clone(), Duration::from_millis(10))
        .await
        .expect("idle route still grants capacity");

    assert_eq!(coordinator.route_snapshot(&saturated).in_flight, 1);
    assert_eq!(coordinator.route_snapshot(&idle).in_flight, 1);
    assert_eq!(coordinator.global_snapshot().in_flight, 2);

    drop(permit);
}

#[tokio::test]
async fn rejects_when_waiter_limit_is_reached() {
    let coordinator = BackpressureCoordinator::new(1, 0);
    let route = route_key("api-a");
    let _held = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let error = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect_err("second checkout rejected");

    assert_eq!(error, BackpressureError::QueueFull);
}

#[tokio::test]
async fn times_out_waiting_for_capacity() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let route = route_key("api-a");
    let _held = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let error = coordinator
        .checkout(route.clone(), Duration::from_millis(1))
        .await
        .expect_err("second checkout times out");

    assert_eq!(error, BackpressureError::Timeout);
}

#[tokio::test]
async fn dropped_permits_decrement_route_and_global_counters() {
    let coordinator = BackpressureCoordinator::new(2, 1);
    let route = route_key("api-a");

    let permit = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("permit granted");

    assert_eq!(
        coordinator.route_snapshot(&route),
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 0,
        }
    );
    assert_eq!(
        coordinator.global_snapshot(),
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 0,
        }
    );

    drop(permit);

    assert_eq!(
        coordinator.route_snapshot(&route),
        RouteBackpressureSnapshot::default()
    );
    assert_eq!(
        coordinator.global_snapshot(),
        RouteBackpressureSnapshot::default()
    );
}

#[tokio::test]
async fn snapshots_expose_route_waiting_and_in_flight_counts() {
    let coordinator = BackpressureCoordinator::new(1, 1);
    let route = route_key("api-a");
    let held = coordinator
        .checkout(route.clone(), Duration::from_millis(10))
        .await
        .expect("first permit granted");

    let waiter = {
        let coordinator = coordinator.clone();
        let route = route.clone();
        tokio::spawn(async move {
            coordinator
                .checkout(route, Duration::from_secs(1))
                .await
                .expect("second permit granted");
        })
    };

    let snapshot = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            let snapshot = coordinator.route_snapshot(&route);
            if snapshot.waiting == 1 {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiter entered queue");

    assert_eq!(
        snapshot,
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 1,
        }
    );
    assert_eq!(
        coordinator.global_snapshot(),
        RouteBackpressureSnapshot {
            in_flight: 1,
            waiting: 0,
        }
    );

    drop(held);
    waiter.await.expect("waiter task completed");
}

#[tokio::test]
async fn configured_route_cap_does_not_reduce_other_route_capacity() {
    let coordinator = BackpressureCoordinator::with_capacity(3, 100, 4);
    let route_a = route_key("api-a");
    let route_b = route_key("api-b");
    coordinator
        .configure_route(
            route_a.clone(),
            RouteFairnessConfig {
                max_in_flight: Some(2),
                ..RouteFairnessConfig::default()
            },
        )
        .expect("valid route policy");

    let first = coordinator
        .checkout(route_a.clone(), Duration::from_millis(10))
        .await
        .expect("first route A permit");
    let second = coordinator
        .checkout(route_a.clone(), Duration::from_millis(10))
        .await
        .expect("second route A permit");
    assert_eq!(coordinator.route_snapshot(&route_a).in_flight, 2);

    let other_route = coordinator
        .checkout(route_b.clone(), Duration::from_millis(10))
        .await
        .expect("route B remains available");
    assert_eq!(coordinator.global_snapshot().in_flight, 3);
    assert_eq!(coordinator.route_snapshot(&route_b).in_flight, 1);

    let capped = coordinator
        .checkout(route_a, Duration::from_millis(1))
        .await
        .expect_err("route A cap is enforced");
    assert_eq!(capped, BackpressureError::Timeout);
    drop(other_route);
    drop(second);
    drop(first);
}

#[tokio::test]
async fn sheddable_routes_are_rejected_before_critical_routes() {
    let coordinator = BackpressureCoordinator::with_capacity(1, 10, 2);
    let holder_route = route_key("holder");
    let critical_route = route_key("critical");
    let sheddable_route = route_key("batch");
    coordinator
        .configure_route(
            critical_route.clone(),
            RouteFairnessConfig {
                priority: RoutePriority::Critical,
                ..RouteFairnessConfig::default()
            },
        )
        .expect("valid critical policy");
    coordinator
        .configure_route(
            sheddable_route.clone(),
            RouteFairnessConfig {
                priority: RoutePriority::Sheddable,
                ..RouteFairnessConfig::default()
            },
        )
        .expect("valid sheddable policy");
    let held = coordinator
        .checkout(holder_route, Duration::from_millis(10))
        .await
        .expect("global capacity holder");

    assert_eq!(
        coordinator
            .checkout(sheddable_route, Duration::from_millis(10))
            .await
            .expect_err("sheddable work is shed first"),
        BackpressureError::QueueFull
    );
    let waiter = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .checkout(critical_route, Duration::from_millis(100))
                .await
                .expect("critical work waits for capacity")
        })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    drop(held);
    waiter.await.expect("critical checkout completes");
}

#[tokio::test]
async fn noisy_route_cannot_starve_second_client() {
    let coordinator = BackpressureCoordinator::with_capacity(1, 1, 8);
    let noisy = route_key("noisy");
    let quiet = route_key("quiet");
    let held = coordinator
        .checkout(noisy.clone(), Duration::from_millis(10))
        .await
        .expect("initial noisy permit");
    let quiet_waiter = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .checkout(quiet, Duration::from_millis(200))
                .await
                .expect("quiet client eventually gets capacity")
        })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    drop(held);

    let quiet_permit = quiet_waiter.await.expect("quiet client is not starved");
    drop(quiet_permit);
    for _ in 0..8 {
        let permit = coordinator
            .checkout(noisy.clone(), Duration::from_millis(10))
            .await
            .expect("noisy route can continue after quiet client");
        drop(permit);
    }
}

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
    registrations: Mutex<HashMap<String, usize>>,
}

impl TestRecorder {
    fn clear(&self) {
        self.registrations.lock().expect("lock recorder").clear();
    }

    fn has_metric(&self, name: &str, labels: &[(&str, &str)]) -> bool {
        self.registrations
            .lock()
            .expect("lock recorder")
            .contains_key(&metric_signature(name, labels))
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
            .insert(metric_signature_from_key(key), 1);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.registrations
            .lock()
            .expect("lock recorder")
            .insert(metric_signature_from_key(key), 1);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.registrations
            .lock()
            .expect("lock recorder")
            .insert(metric_signature_from_key(key), 1);
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
