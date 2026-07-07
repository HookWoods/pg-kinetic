use std::{
    sync::{Mutex, OnceLock},
    time::Duration,
};

use metrics::{Counter, Gauge, Histogram, Key, Metadata, Recorder};
use pg_kinetic::{
    core::observability::{MetricOutcome, ProtocolPhase},
    proxy_runtime::telemetry::{self, PhaseTimer, PhaseTimingRecorder},
};

#[test]
fn phase_timer_records_startup_duration() {
    let recorder = TestPhaseTimingRecorder::default();
    let timer = PhaseTimer::start(ProtocolPhase::Startup, &recorder);

    timer.finish(MetricOutcome::Ok);

    assert_calls(&recorder, &[(ProtocolPhase::Startup, MetricOutcome::Ok)]);
}

#[test]
fn phase_timer_records_backend_checkout_duration() {
    let recorder = TestPhaseTimingRecorder::default();
    let timer = PhaseTimer::start(ProtocolPhase::BackendCheckout, &recorder);

    timer.finish(MetricOutcome::Rejected);

    assert_calls(
        &recorder,
        &[(ProtocolPhase::BackendCheckout, MetricOutcome::Rejected)],
    );
}

#[test]
fn phase_timer_records_execute_and_rows_duration() {
    let recorder = TestPhaseTimingRecorder::default();

    PhaseTimer::start(ProtocolPhase::Execute, &recorder).finish(MetricOutcome::Ok);
    PhaseTimer::start(ProtocolPhase::Rows, &recorder).finish(MetricOutcome::Ok);

    assert_calls(
        &recorder,
        &[
            (ProtocolPhase::Execute, MetricOutcome::Ok),
            (ProtocolPhase::Rows, MetricOutcome::Ok),
        ],
    );
}

#[test]
fn phase_timer_records_drain_and_reset_duration() {
    let recorder = TestPhaseTimingRecorder::default();

    PhaseTimer::start(ProtocolPhase::Drain, &recorder).finish(MetricOutcome::Canceled);
    PhaseTimer::start(ProtocolPhase::Reset, &recorder).finish(MetricOutcome::Ok);

    assert_calls(
        &recorder,
        &[
            (ProtocolPhase::Drain, MetricOutcome::Canceled),
            (ProtocolPhase::Reset, MetricOutcome::Ok),
        ],
    );
}

#[test]
fn metric_labels_use_protocol_phase_enum_values() {
    let recorder = install_metrics_recorder();
    recorder.clear();

    telemetry::record_protocol_phase_duration(
        ProtocolPhase::Cancel,
        MetricOutcome::Canceled,
        Duration::from_millis(27),
    );

    assert!(recorder.has_histogram(
        "pg_kinetic_protocol_phase_duration_ms",
        &[("phase", "cancel"), ("outcome", "canceled")]
    ));
}

#[derive(Debug, Default)]
struct TestPhaseTimingRecorder {
    calls: Mutex<Vec<(ProtocolPhase, MetricOutcome, Duration)>>,
}

impl PhaseTimingRecorder for TestPhaseTimingRecorder {
    fn record_protocol_phase_duration(
        &self,
        phase: ProtocolPhase,
        outcome: MetricOutcome,
        duration: Duration,
    ) {
        self.calls
            .lock()
            .expect("lock timing recorder")
            .push((phase, outcome, duration));
    }
}

fn assert_calls(recorder: &TestPhaseTimingRecorder, expected: &[(ProtocolPhase, MetricOutcome)]) {
    let calls = recorder.calls.lock().expect("lock timing recorder");
    assert_eq!(calls.len(), expected.len());
    for (index, (phase, outcome)) in expected.iter().copied().enumerate() {
        assert_eq!(calls[index].0, phase);
        assert_eq!(calls[index].1, outcome);
    }
}

fn install_metrics_recorder() -> std::sync::Arc<TestRecorder> {
    METRICS_RECORDER
        .get_or_init(|| {
            let recorder = std::sync::Arc::new(TestRecorder::default());
            metrics::set_global_recorder(recorder.clone()).expect("install metrics recorder");
            recorder
        })
        .clone()
}

static METRICS_RECORDER: OnceLock<std::sync::Arc<TestRecorder>> = OnceLock::new();

#[derive(Debug, Default)]
struct TestRecorder {
    registrations: Mutex<Vec<String>>,
}

impl TestRecorder {
    fn clear(&self) {
        self.registrations.lock().expect("lock recorder").clear();
    }

    fn has_histogram(&self, name: &str, labels: &[(&str, &str)]) -> bool {
        self.registrations
            .lock()
            .expect("lock recorder")
            .iter()
            .any(|signature| signature == &metric_signature(name, labels))
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

    fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.registrations
            .lock()
            .expect("lock recorder")
            .push(metric_signature_from_key(key));
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
