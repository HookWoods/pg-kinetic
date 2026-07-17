use std::{
    collections::VecDeque,
    sync::{Arc, RwLock as StdRwLock},
};

use anyhow::Context;
use tokio::{
    sync::RwLock,
    time::sleep,
};

use crate::{
    config::{AdaptiveConfig, Config},
    mirror::{MirrorOutcomeRecorder, MirrorTaskStatus},
    snapshot::{
        AdaptiveOutcomeSnapshot, AdaptiveRecommendationSnapshot, BackpressureSnapshot,
        SnapshotStore,
    },
};
use pg_kinetic_core::adaptive::{
    AdaptiveAction, AdaptiveOutcome, AdaptiveRecommendation, AdaptiveSignal,
    TuningBound, TunableKnob,
};

const DEFAULT_HISTORY_CAPACITY: usize = 64;
const HIGH_POOL_WAIT_THRESHOLD: usize = 8;
const HIGH_OVERLOAD_THRESHOLD: u64 = 3;
const MIRROR_DROP_THRESHOLD: usize = 1;

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveTuningSnapshot {
    pub pool_size: usize,
    pub backpressure_thresholds: usize,
    pub mirror_sample_rate: f64,
    pub checkout_timeout_ms: u64,
}

impl AdaptiveTuningSnapshot {
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            pool_size: config.capacity.max_backends,
            backpressure_thresholds: config.qos.max_route_waiters,
            mirror_sample_rate: 0.25,
            checkout_timeout_ms: config.performance.checkout_timeout_ms,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AdaptiveSignalSnapshot {
    pub pool_waiting_clients: usize,
    pub pool_active_backends: usize,
    pub pool_configured_backends: usize,
    pub max_backends: usize,
    pub max_checkout_waiters: usize,
    pub max_route_waiters: usize,
    pub checkout_timeout_ms: u64,
    pub backpressure_waiting: usize,
    pub backpressure_rejected: u64,
    pub backpressure_timed_out: u64,
    pub backpressure_canceled: u64,
    pub mirror_observations: usize,
    pub mirror_completed: usize,
    pub mirror_timed_out: usize,
    pub mirror_dropped: usize,
    pub mirror_skipped: usize,
    pub mirror_rejected: usize,
    pub mirror_errors: usize,
}

impl AdaptiveSignalSnapshot {
    #[must_use]
    pub const fn overload_events(&self) -> u64 {
        self.backpressure_rejected + self.backpressure_timed_out + self.backpressure_canceled
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveHistory {
    capacity: usize,
    recommendations: VecDeque<AdaptiveRecommendationSnapshot>,
    outcomes: VecDeque<AdaptiveOutcomeSnapshot>,
}

impl AdaptiveHistory {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            recommendations: VecDeque::new(),
            outcomes: VecDeque::new(),
        }
    }

    fn push_recommendation(&mut self, snapshot: AdaptiveRecommendationSnapshot) {
        if self.recommendations.len() == self.capacity {
            self.recommendations.pop_front();
        }
        self.recommendations.push_back(snapshot);
    }

    fn push_outcome(&mut self, snapshot: AdaptiveOutcomeSnapshot) {
        if self.outcomes.len() == self.capacity {
            self.outcomes.pop_front();
        }
        self.outcomes.push_back(snapshot);
    }

    pub fn record_recommendation(&mut self, snapshot: AdaptiveRecommendationSnapshot) {
        self.push_recommendation(snapshot);
    }

    pub fn record_outcome(&mut self, snapshot: AdaptiveOutcomeSnapshot) {
        self.push_outcome(snapshot);
    }

    #[must_use]
    pub fn recommendations(&self) -> Vec<AdaptiveRecommendationSnapshot> {
        self.recommendations.iter().cloned().collect()
    }

    #[must_use]
    pub fn outcomes(&self) -> Vec<AdaptiveOutcomeSnapshot> {
        self.outcomes.iter().cloned().collect()
    }
}

impl Default for AdaptiveHistory {
    fn default() -> Self {
        Self::new(DEFAULT_HISTORY_CAPACITY)
    }
}

#[derive(Clone, Debug)]
pub struct AdaptiveSignalCollector {
    snapshot_store: SnapshotStore,
    mirror_outcomes: MirrorOutcomeRecorder,
}

impl AdaptiveSignalCollector {
    #[must_use]
    pub fn new(snapshot_store: SnapshotStore, mirror_outcomes: MirrorOutcomeRecorder) -> Self {
        Self {
            snapshot_store,
            mirror_outcomes,
        }
    }

    #[must_use]
    pub fn collect(&self) -> AdaptiveSignalSnapshot {
        let pool = self.snapshot_store.pool_snapshot();
        let limits = self.snapshot_store.limits_snapshot();
        let backpressure_snapshots = self.snapshot_store.backpressure_snapshots();
        let mirror_observations = self.mirror_outcomes.snapshot();

        let (mirror_completed, mirror_timed_out, mirror_dropped, mirror_skipped, mirror_rejected, mirror_errors) =
            mirror_observations.iter().fold(
                (0usize, 0usize, 0usize, 0usize, 0usize, 0usize),
                |(completed, timed_out, dropped, skipped, rejected, errors), observation| match &observation.status {
                    MirrorTaskStatus::Completed => (completed + 1, timed_out, dropped, skipped, rejected, errors),
                    MirrorTaskStatus::TimedOut => (completed, timed_out + 1, dropped, skipped, rejected, errors),
                    MirrorTaskStatus::Dropped { .. } => (completed, timed_out, dropped + 1, skipped, rejected, errors),
                    MirrorTaskStatus::Skipped { .. } => (completed, timed_out, dropped, skipped + 1, rejected, errors),
                    MirrorTaskStatus::Rejected { .. } => (completed, timed_out, dropped, skipped, rejected + 1, errors),
                    MirrorTaskStatus::Error => (completed, timed_out, dropped, skipped, rejected, errors + 1),
                },
            );

        let (backpressure_waiting, backpressure_rejected, backpressure_timed_out, backpressure_canceled) =
            aggregate_backpressure(&backpressure_snapshots);

        AdaptiveSignalSnapshot {
            pool_waiting_clients: pool.waiting_clients,
            pool_active_backends: pool.active_backends,
            pool_configured_backends: pool.configured_backends,
            max_backends: limits.max_backends,
            max_checkout_waiters: limits.max_checkout_waiters,
            max_route_waiters: limits.max_route_waiters,
            checkout_timeout_ms: limits.checkout_timeout.as_millis() as u64,
            backpressure_waiting,
            backpressure_rejected,
            backpressure_timed_out,
            backpressure_canceled,
            mirror_observations: mirror_observations.len(),
            mirror_completed,
            mirror_timed_out,
            mirror_dropped,
            mirror_skipped,
            mirror_rejected,
            mirror_errors,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AdaptiveRecommendationEngine;

impl AdaptiveRecommendationEngine {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn recommend(
        &self,
        signals: &AdaptiveSignalSnapshot,
        tuning: &AdaptiveTuningSnapshot,
        adaptive: &AdaptiveConfig,
    ) -> anyhow::Result<Vec<AdaptiveRecommendation>> {
        let mut recommendations = Vec::with_capacity(3);
        let window_ms = adaptive.recommendation_window().as_millis() as u64;
        let confidence = recommendation_confidence(adaptive.adaptive_min_confidence);
        let safety_bound = adaptive.guardrail.safety_bound();

        if signals.pool_waiting_clients >= HIGH_POOL_WAIT_THRESHOLD {
            let reason = format!(
                "{} checkout waiters are elevated; review pool sizing and backpressure thresholds",
                signals.pool_waiting_clients
            );
            recommendations.push(build_recommendation(
                AdaptiveSignal::PoolSizePressure,
                TunableKnob::PoolSize,
                confidence,
                reason,
                window_ms,
                safety_bound,
            )?);
        }

        if signals.overload_events() >= HIGH_OVERLOAD_THRESHOLD {
            let reason = format!(
                "{} overload events were recorded; tighten backpressure or review capacity",
                signals.overload_events()
            );
            recommendations.push(build_recommendation(
                AdaptiveSignal::BackpressureThresholdPressure,
                TunableKnob::BackpressureThresholds,
                confidence,
                reason,
                window_ms,
                safety_bound,
            )?);
        }

        if signals.mirror_dropped >= MIRROR_DROP_THRESHOLD && tuning.mirror_sample_rate > 0.0 {
            let reason = format!(
                "{} mirror drops were observed at a {:.3} sample rate; lower the mirror sample rate",
                signals.mirror_dropped,
                tuning.mirror_sample_rate
            );
            recommendations.push(build_recommendation(
                AdaptiveSignal::MirrorSamplingPressure,
                TunableKnob::MirrorSampling,
                confidence,
                reason,
                window_ms,
                safety_bound,
            )?);
        }

        Ok(recommendations)
    }
}

#[derive(Clone, Debug, Default)]
pub struct AdaptiveApplyEngine;

impl AdaptiveApplyEngine {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn apply(
        &self,
        config: &Config,
        recommendation: &AdaptiveRecommendation,
        tuning: &AdaptiveTuningSnapshot,
    ) -> AdaptiveOutcomeSnapshot {
        let adaptive = &config.runtime.production.adaptive;
        let disabled_by_reload = !adaptive.adaptive_mode.is_apply() || !adaptive.apply.adaptive_apply_enabled;
        match adaptive.evaluate(recommendation) {
            Ok(AdaptiveOutcome::Recommended) => AdaptiveOutcomeSnapshot::new(
                recommendation.signal(),
                recommendation.knob(),
                AdaptiveOutcome::Recommended,
                recommendation.reason().to_string(),
                None,
                None,
                recommendation.safety_bound().max_change_percent(),
                disabled_by_reload,
            ),
            Ok(AdaptiveOutcome::Applied) => {
                let change_percent = recommendation.safety_bound().max_change_percent();
                let (before_value, after_value) = self
                .apply_adjustment(config, recommendation, tuning, change_percent)
                    .unwrap_or((None, None));
                AdaptiveOutcomeSnapshot::new(
                    recommendation.signal(),
                    recommendation.knob(),
                    AdaptiveOutcome::Applied,
                    recommendation.reason().to_string(),
                    before_value,
                    after_value,
                    change_percent,
                    false,
                )
            }
            Ok(AdaptiveOutcome::Skipped) => AdaptiveOutcomeSnapshot::new(
                recommendation.signal(),
                recommendation.knob(),
                AdaptiveOutcome::Skipped,
                recommendation.reason().to_string(),
                None,
                None,
                recommendation.safety_bound().max_change_percent(),
                disabled_by_reload,
            ),
            Ok(AdaptiveOutcome::Rejected) => AdaptiveOutcomeSnapshot::new(
                recommendation.signal(),
                recommendation.knob(),
                AdaptiveOutcome::Rejected,
                recommendation.reason().to_string(),
                None,
                None,
                recommendation.safety_bound().max_change_percent(),
                false,
            ),
            Err(error) => AdaptiveOutcomeSnapshot::new(
                recommendation.signal(),
                recommendation.knob(),
                AdaptiveOutcome::Rejected,
                error.to_string(),
                None,
                None,
                recommendation.safety_bound().max_change_percent(),
                false,
            ),
        }
    }

    fn apply_adjustment(
        &self,
        config: &Config,
        recommendation: &AdaptiveRecommendation,
        tuning: &AdaptiveTuningSnapshot,
        change_percent: Option<u8>,
    ) -> anyhow::Result<(Option<f64>, Option<f64>)> {
        let change_percent = change_percent.unwrap_or(config.runtime.production.adaptive.guardrail.adaptive_max_change_percent);
        let before_value = current_value(recommendation.knob(), tuning);
        let after_value = adjusted_value(before_value, recommendation.knob(), change_percent);
        Ok((Some(before_value), Some(after_value)))
    }
}

#[derive(Clone, Debug)]
pub struct AdaptiveController {
    collector: AdaptiveSignalCollector,
    recommendation_engine: AdaptiveRecommendationEngine,
    apply_engine: AdaptiveApplyEngine,
    active_config: Arc<RwLock<Config>>,
    snapshot_store: SnapshotStore,
    history: Arc<StdRwLock<AdaptiveHistory>>,
}

impl AdaptiveController {
    #[must_use]
    pub fn new(
        snapshot_store: SnapshotStore,
        mirror_outcomes: MirrorOutcomeRecorder,
        active_config: Arc<RwLock<Config>>,
    ) -> Self {
        Self {
            collector: AdaptiveSignalCollector::new(snapshot_store.clone(), mirror_outcomes),
            recommendation_engine: AdaptiveRecommendationEngine::new(),
            apply_engine: AdaptiveApplyEngine::new(),
            active_config,
            snapshot_store,
            history: Arc::new(StdRwLock::new(AdaptiveHistory::default())),
        }
    }

    #[must_use]
    pub fn history(&self) -> AdaptiveHistory {
        self.history.read().expect("adaptive history poisoned").clone()
    }

    pub async fn tick(&self) -> anyhow::Result<Vec<AdaptiveRecommendation>> {
        let config = self.active_config.read().await.clone();
        if !config.runtime.production.adaptive_enabled {
            return Ok(Vec::new());
        }

        let signals = self.collector.collect();
        let tuning = AdaptiveTuningSnapshot::from_config(&config);
        let recommendations = self
            .recommendation_engine
            .recommend(&signals, &tuning, &config.runtime.production.adaptive)
            .context("build adaptive recommendations")?;

        for recommendation in &recommendations {
            let recommendation_snapshot = AdaptiveRecommendationSnapshot::from_recommendation(recommendation);
            self.snapshot_store
                .record_adaptive_recommendation(recommendation_snapshot.clone());
            self.history
                .write()
                .expect("adaptive history poisoned")
                .record_recommendation(recommendation_snapshot);

            let outcome_snapshot = self.apply_engine.apply(&config, recommendation, &tuning);
            self.snapshot_store
                .record_adaptive_outcome(outcome_snapshot.clone());
            self.history
                .write()
                .expect("adaptive history poisoned")
                .record_outcome(outcome_snapshot);
        }

        Ok(recommendations)
    }

    pub async fn run(self) {
        loop {
            if let Err(error) = self.tick().await {
                tracing::warn!(error = %error, "adaptive controller tick failed");
            }

            let interval = {
                let config = self.active_config.read().await;
                config.runtime.production.adaptive.recommendation_window()
            };
            sleep(interval).await;
        }
    }
}

fn build_recommendation(
    signal: AdaptiveSignal,
    knob: TunableKnob,
    confidence: f64,
    reason: String,
    window_ms: u64,
    safety_bound: TuningBound,
) -> anyhow::Result<AdaptiveRecommendation> {
    AdaptiveRecommendation::new(
        signal,
        AdaptiveAction::Recommend,
        knob,
        confidence,
        reason,
        window_ms,
        safety_bound,
    )
    .map_err(|error| anyhow::anyhow!(error))
}

fn recommendation_confidence(min_confidence: f64) -> f64 {
    (min_confidence + 0.05).min(0.99).max(min_confidence)
}

fn aggregate_backpressure(backpressure_snapshots: &[BackpressureSnapshot]) -> (usize, u64, u64, u64) {
    backpressure_snapshots.iter().fold(
        (0usize, 0u64, 0u64, 0u64),
        |(waiting, rejected, timed_out, canceled), snapshot| {
            (
                waiting + snapshot.waiting,
                rejected + snapshot.rejected,
                timed_out + snapshot.timed_out,
                canceled + snapshot.canceled,
            )
        },
    )
}

fn current_value(knob: TunableKnob, tuning: &AdaptiveTuningSnapshot) -> f64 {
    match knob {
        TunableKnob::PoolSize => tuning.pool_size as f64,
        TunableKnob::BackpressureThresholds => tuning.backpressure_thresholds as f64,
        TunableKnob::MirrorSampling => tuning.mirror_sample_rate,
        TunableKnob::Timeout => tuning.checkout_timeout_ms as f64,
    }
}

fn adjusted_value(before_value: f64, knob: TunableKnob, change_percent: u8) -> f64 {
    let change = change_percent as f64 / 100.0;
    let adjusted = match knob {
        TunableKnob::PoolSize => (before_value * (1.0 + change)).ceil(),
        TunableKnob::BackpressureThresholds | TunableKnob::Timeout | TunableKnob::MirrorSampling => {
            (before_value * (1.0 - change)).floor()
        }
    };

    match knob {
        TunableKnob::PoolSize => adjusted.max(before_value + 1.0),
        TunableKnob::BackpressureThresholds | TunableKnob::Timeout => adjusted.max(1.0),
        TunableKnob::MirrorSampling => adjusted.clamp(0.0, 1.0),
    }
}
