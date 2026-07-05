use pg_kinetic::backpressure::{
    BackpressureCoordinator, BackpressureError, BackpressureGate, RouteBackpressureSnapshot,
};
use pg_kinetic::route::{QueryClass, RouteKey};
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};
use metrics::{Counter, Gauge, Histogram, Key, Metadata, Recorder};

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
        &[("route", route_label.as_str()), ("outcome", "query_timeout")]
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
        &[("route", route_label.as_str()), ("scope", "global")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_timeout_total",
        &[("kind", "idle_timeout")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_timeout_total",
        &[("kind", "query_timeout")]
    ));
    assert!(recorder.has_metric(
        "pg_kinetic_buffer_limit_total",
        &[("kind", "buffer_limit")]
    ));
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
