use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use pg_kinetic_core::observability::{MetricOutcome, ProtocolPhase};

pub trait PhaseTimingRecorder: Send + Sync {
    fn record_protocol_phase_duration(
        &self,
        phase: ProtocolPhase,
        outcome: MetricOutcome,
        duration: Duration,
    );
}

#[derive(Clone, Debug, Default)]
pub struct MetricsPhaseTimingRecorder;

impl PhaseTimingRecorder for MetricsPhaseTimingRecorder {
    fn record_protocol_phase_duration(
        &self,
        phase: ProtocolPhase,
        outcome: MetricOutcome,
        duration: Duration,
    ) {
        record_protocol_phase_duration(phase, outcome, duration);
    }
}

#[must_use]
pub fn shared_phase_timing_recorder() -> Arc<dyn PhaseTimingRecorder> {
    Arc::new(MetricsPhaseTimingRecorder)
}

pub fn record_protocol_phase_duration(
    phase: ProtocolPhase,
    outcome: MetricOutcome,
    duration: Duration,
) {
    crate::metrics::record_protocol_phase_duration(phase, outcome, duration);
}

pub struct PhaseTimer<'a> {
    phase: ProtocolPhase,
    started: Instant,
    recorder: &'a dyn PhaseTimingRecorder,
}

impl std::fmt::Debug for PhaseTimer<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PhaseTimer")
            .field("phase", &self.phase)
            .field("started", &self.started)
            .finish()
    }
}

impl<'a> PhaseTimer<'a> {
    #[must_use]
    pub fn start(phase: ProtocolPhase, recorder: &'a dyn PhaseTimingRecorder) -> Self {
        Self {
            phase,
            started: Instant::now(),
            recorder,
        }
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn finish(self, outcome: MetricOutcome) -> Duration {
        let duration = self.started.elapsed();
        self.recorder
            .record_protocol_phase_duration(self.phase, outcome, duration);
        duration
    }
}
