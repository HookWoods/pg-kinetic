use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use pg_kinetic::{
    core::observability::{MetricOutcome, ProtocolPhase},
    proxy_runtime::telemetry::{PhaseTimingRecorder, SampledPhaseTimingRecorder},
};

#[derive(Default)]
struct TestMetrics {
    queries: Mutex<u64>,
    phase_histograms: Mutex<Vec<(ProtocolPhase, MetricOutcome, Duration)>>,
}

impl TestMetrics {
    fn new(sample_rate: f64, session_id: u64) -> (Arc<Self>, SampledPhaseTimingRecorder) {
        let metrics = Arc::new(Self::default());
        let recorder = SampledPhaseTimingRecorder::new(metrics.clone(), sample_rate, session_id);
        (metrics, recorder)
    }

    fn record_query_success(&self) {
        *self.queries.lock().expect("lock query counter") += 1;
    }

    fn counter(&self) -> u64 {
        *self.queries.lock().expect("lock query counter")
    }

    fn histogram_samples(&self) -> usize {
        self.phase_histograms
            .lock()
            .expect("lock phase histogram")
            .len()
    }
}

impl PhaseTimingRecorder for TestMetrics {
    fn record_protocol_phase_duration(
        &self,
        phase: ProtocolPhase,
        outcome: MetricOutcome,
        duration: Duration,
    ) {
        self.phase_histograms
            .lock()
            .expect("lock phase histogram")
            .push((phase, outcome, duration));
    }
}

#[test]
fn zero_sampling_keeps_core_metrics_but_emits_no_phase_histogram() {
    let (metrics, recorder) = TestMetrics::new(0.0, 7);
    metrics.record_query_success();
    recorder.record_protocol_phase_duration(
        ProtocolPhase::Execute,
        MetricOutcome::Ok,
        Duration::from_millis(1),
    );

    assert_eq!(metrics.counter(), 1);
    assert_eq!(metrics.histogram_samples(), 0);
}

#[test]
fn one_sampling_emits_each_phase_histogram() {
    let (metrics, recorder) = TestMetrics::new(1.0, 7);
    recorder.record_protocol_phase_duration(
        ProtocolPhase::Execute,
        MetricOutcome::Ok,
        Duration::from_millis(1),
    );

    assert_eq!(metrics.histogram_samples(), 1);
}

#[test]
fn session_sampling_is_consistent_across_phase_sequence() {
    let (metrics, recorder) = TestMetrics::new(0.5, 7);
    let sampler = recorder.clone();
    for phase in [
        ProtocolPhase::Startup,
        ProtocolPhase::Execute,
        ProtocolPhase::Rows,
    ] {
        sampler.record_protocol_phase_duration(phase, MetricOutcome::Ok, Duration::from_millis(1));
    }

    assert!(metrics.histogram_samples() == 0 || metrics.histogram_samples() == 3);
}
