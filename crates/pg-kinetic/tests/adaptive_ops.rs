use std::{sync::Arc, time::Duration};

use pg_kinetic::config::{
    AdaptiveApplyConfig, AdaptiveConfig, AdaptiveGuardrailConfig, Config, ProductionConfig,
};
use pg_kinetic::core::mirror::{MirrorDecision, MirrorMode, MirrorReason, MirrorSafetyGate};
use pg_kinetic::proxy_runtime::{
    adaptive::{
        AdaptiveApplyEngine, AdaptiveController, AdaptiveRecommendationEngine,
        AdaptiveSignalCollector, AdaptiveSignalSnapshot, AdaptiveTuningSnapshot,
    },
    mirror::{MirrorObservation, MirrorOutcomeRecorder, MirrorTaskStatus, MirrorTelemetry},
    snapshot::{
        AdaptiveOutcomeSnapshot, AdaptiveRecommendationSnapshot, BackpressureSnapshot,
        LimitsSnapshot, PoolSnapshot, SnapshotStore,
    },
};
use pg_kinetic::route::{QueryClass, RouteKey};
use pg_kinetic_core::adaptive::{
    AdaptiveAction, AdaptiveGuardrail, AdaptiveMode, AdaptiveOutcome, AdaptiveRecommendation,
    AdaptiveSignal, TuningBound, TunableKnob,
};
use tokio::sync::RwLock;

fn recommendation(
    signal: AdaptiveSignal,
    knob: TunableKnob,
    confidence: f64,
    reason: &str,
    safety_bound: TuningBound,
) -> AdaptiveRecommendation {
    AdaptiveRecommendation::new(
        signal,
        AdaptiveAction::Recommend,
        knob,
        confidence,
        reason,
        60_000,
        safety_bound,
    )
    .expect("valid adaptive recommendation")
}

fn route_key(name: &str) -> RouteKey {
    RouteKey::new(
        "billing",
        "reporter",
        Some(name),
        Some("127.0.0.1:6100".parse().expect("client addr")),
        QueryClass::Default,
    )
}

fn mirror_observation(route_key: &RouteKey, status: MirrorTaskStatus) -> MirrorObservation {
    let decision = match &status {
        MirrorTaskStatus::Completed => {
            MirrorDecision::mirrored(MirrorMode::ReadOnly, MirrorSafetyGate::TargetConfigured)
        }
        MirrorTaskStatus::TimedOut => MirrorDecision::skipped(
            MirrorMode::ReadOnly,
            MirrorSafetyGate::Sampling,
            MirrorReason::SampledOut,
        ),
        MirrorTaskStatus::Error => MirrorDecision::skipped(
            MirrorMode::ReadOnly,
            MirrorSafetyGate::TargetConfigured,
            MirrorReason::Disabled,
        ),
        MirrorTaskStatus::Dropped { reason } => MirrorDecision::skipped(
            MirrorMode::ReadOnly,
            MirrorSafetyGate::Sampling,
            *reason,
        ),
        MirrorTaskStatus::Skipped { reason } => MirrorDecision::skipped(
            MirrorMode::ReadOnly,
            MirrorSafetyGate::Sampling,
            *reason,
        ),
        MirrorTaskStatus::Rejected { reason } => MirrorDecision::rejected(
            MirrorMode::ReadOnly,
            MirrorSafetyGate::TargetConfigured,
            *reason,
        ),
    };

    MirrorObservation {
        telemetry: MirrorTelemetry {
            session_id: 7,
            query_id: 11,
            route_label: route_key.metric_label(),
            command_label: "select",
            frame_count: 3,
            replay_count: 1,
            mode: MirrorMode::ReadOnly,
        },
        decision,
        status,
        duration: Duration::from_millis(5),
    }
}

fn configured_store() -> SnapshotStore {
    let store = SnapshotStore::new();
    let mut limits = LimitsSnapshot::default();
    limits.max_backends = 16;
    limits.max_checkout_waiters = 24;
    limits.max_route_waiters = 32;
    limits.checkout_timeout = Duration::from_millis(1_000);
    store.set_limits_snapshot(limits);
    store
}

fn tuned_config(
    mode: AdaptiveMode,
    apply_enabled: bool,
    allowlist: Vec<TunableKnob>,
) -> Config {
    let mut config = Config::default();
    config.runtime.production.adaptive_enabled = true;
    config.runtime.production.adaptive.adaptive_mode = mode;
    config.runtime.production.adaptive.apply.adaptive_apply_enabled = apply_enabled;
    config.runtime.production.adaptive.apply.adaptive_apply_allowlist = allowlist;
    config.runtime.production.adaptive.guardrail.adaptive_max_change_percent = 10;
    config.capacity.max_backends = 16;
    config.qos.max_route_waiters = 32;
    config.performance.checkout_timeout_ms = 1_000;
    config
}

fn tuning_snapshot() -> AdaptiveTuningSnapshot {
    AdaptiveTuningSnapshot {
        pool_size: 16,
        backpressure_thresholds: 32,
        mirror_sample_rate: 0.25,
        checkout_timeout_ms: 1_000,
    }
}

#[test]
fn adaptive_ops_are_disabled_by_default() {
    let production = ProductionConfig::default();
    let adaptive = AdaptiveConfig::default();

    assert!(!production.adaptive_enabled);
    assert_eq!(adaptive.adaptive_mode, AdaptiveMode::Recommend);
    assert_eq!(adaptive.adaptive_window_ms, 60_000);
    assert_eq!(adaptive.adaptive_min_confidence, 0.8);
    assert!(!adaptive.apply.adaptive_apply_enabled);
    assert!(adaptive.apply.adaptive_apply_allowlist.is_empty());
    assert_eq!(adaptive.guardrail.adaptive_max_change_percent, 10);
    assert_eq!(
        adaptive.guardrail.safety_bound(),
        TuningBound::percent(10)
    );
}

#[test]
fn recommendation_mode_can_run_without_applying_changes() {
    let adaptive = toml::from_str::<AdaptiveConfig>(
        r#"
        adaptive_mode = "recommend"
        adaptive_window_ms = 60_000
        adaptive_min_confidence = 0.8
        "#,
    )
    .expect("recommendation mode parses");

    adaptive.validate().expect("recommendation mode is valid");

    let recommendation = recommendation(
        AdaptiveSignal::PoolSizePressure,
        TunableKnob::PoolSize,
        0.91,
        "checkout wait time is increasing",
        TuningBound::percent(8),
    );

    assert_eq!(
        adaptive.evaluate(&recommendation).expect("recommendation outcome"),
        AdaptiveOutcome::Recommended
    );
}

#[test]
fn apply_mode_requires_explicit_allowlist_of_tunable_knobs() {
    let adaptive = toml::from_str::<AdaptiveConfig>(
        r#"
        adaptive_mode = "apply"
        adaptive_apply_enabled = true
        "#,
    )
    .expect("apply mode parses");

    let error = adaptive.validate().expect_err("missing allowlist is rejected");
    assert!(
        error.to_lowercase().contains("allow"),
        "validation error: {error}"
    );

    let allowlisted = toml::from_str::<AdaptiveConfig>(
        r#"
        adaptive_mode = "apply"
        adaptive_apply_enabled = true
        adaptive_apply_allowlist = ["pool_size", "timeout"]
        "#,
    )
    .expect("allowlisted apply mode parses");

    allowlisted.validate().expect("allowlisted apply mode is valid");
    assert!(allowlisted.apply.allows(TunableKnob::PoolSize));
    assert!(allowlisted.apply.allows(TunableKnob::Timeout));
}

#[test]
fn stable_labels_cover_pool_backpressure_mirror_and_timeout_recommendations() {
    assert_eq!(TunableKnob::PoolSize.as_str(), "pool_size");
    assert_eq!(
        TunableKnob::BackpressureThresholds.as_str(),
        "backpressure_thresholds"
    );
    assert_eq!(TunableKnob::MirrorSampling.as_str(), "mirror_sampling");
    assert_eq!(TunableKnob::Timeout.as_str(), "timeout");

    let labels = [
        (
            AdaptiveSignal::PoolSizePressure,
            TunableKnob::PoolSize,
            "pool_size_pressure",
        ),
        (
            AdaptiveSignal::BackpressureThresholdPressure,
            TunableKnob::BackpressureThresholds,
            "backpressure_threshold_pressure",
        ),
        (
            AdaptiveSignal::MirrorSamplingPressure,
            TunableKnob::MirrorSampling,
            "mirror_sampling_pressure",
        ),
        (
            AdaptiveSignal::TimeoutPressure,
            TunableKnob::Timeout,
            "timeout_pressure",
        ),
    ];

    for (signal, knob, expected_signal_label) in labels {
        let recommendation = recommendation(
            signal,
            knob,
            0.9,
            "stable recommendation label",
            TuningBound::percent(10),
        );

        assert_eq!(signal.as_str(), expected_signal_label);
        assert_eq!(recommendation.signal().as_str(), expected_signal_label);
        assert_eq!(recommendation.knob().as_str(), knob.as_str());
    }
}

#[test]
fn adaptive_recommendations_include_confidence_reason_window_and_safety_bound() {
    let recommendation = recommendation(
        AdaptiveSignal::MirrorSamplingPressure,
        TunableKnob::MirrorSampling,
        0.84,
        "mirror sample rate is too high for current load",
        TuningBound::percent(7),
    );

    assert_eq!(recommendation.action(), AdaptiveAction::Recommend);
    assert_eq!(recommendation.confidence(), 0.84);
    assert_eq!(
        recommendation.reason(),
        "mirror sample rate is too high for current load"
    );
    assert_eq!(recommendation.window_ms(), 60_000);
    assert_eq!(recommendation.safety_bound(), TuningBound::percent(7));
}

#[test]
fn adaptive_apply_rejects_unsafe_or_unbounded_changes() {
    let adaptive = AdaptiveConfig {
        adaptive_mode: AdaptiveMode::Apply,
        adaptive_window_ms: 60_000,
        adaptive_min_confidence: 0.8,
        apply: AdaptiveApplyConfig {
            adaptive_apply_enabled: true,
            adaptive_apply_allowlist: vec![TunableKnob::PoolSize],
        },
        guardrail: AdaptiveGuardrailConfig {
            adaptive_max_change_percent: 10,
        },
    };

    adaptive.validate().expect("apply config is valid");

    let safe = recommendation(
        AdaptiveSignal::PoolSizePressure,
        TunableKnob::PoolSize,
        0.94,
        "pool checkout latency is elevated",
        TuningBound::percent(6),
    );
    assert_eq!(
        adaptive.evaluate(&safe).expect("safe change is applied"),
        AdaptiveOutcome::Applied
    );

    let unbounded = recommendation(
        AdaptiveSignal::PoolSizePressure,
        TunableKnob::PoolSize,
        0.94,
        "pool checkout latency is elevated",
        TuningBound::unbounded(),
    );
    let error = adaptive
        .evaluate(&unbounded)
        .expect_err("unbounded changes are rejected");
    assert_eq!(error.guardrail(), AdaptiveGuardrail::UnboundedChange);

    let excessive = recommendation(
        AdaptiveSignal::PoolSizePressure,
        TunableKnob::PoolSize,
        0.94,
        "pool checkout latency is elevated",
        TuningBound::percent(25),
    );
    let error = adaptive
        .evaluate(&excessive)
        .expect_err("oversized changes are rejected");
    assert_eq!(error.guardrail(), AdaptiveGuardrail::MaxChangePercent);
}

#[test]
fn recommendation_loop_reads_low_cardinality_metrics_and_snapshots_only() {
    let store = configured_store();
    let route_a = route_key("primary");
    let route_b = route_key("secondary");

    let mut pool = PoolSnapshot::new(16, 12);
    pool.waiting_clients = 12;
    store.set_pool_snapshot(pool);

    let mut first_backpressure = BackpressureSnapshot::new(route_a.clone());
    first_backpressure.waiting = 5;
    first_backpressure.rejected = 2;
    first_backpressure.timed_out = 1;
    store.set_backpressure_snapshot(first_backpressure);

    let mut second_backpressure = BackpressureSnapshot::new(route_b.clone());
    second_backpressure.waiting = 3;
    second_backpressure.canceled = 1;
    store.set_backpressure_snapshot(second_backpressure);

    let recorder = MirrorOutcomeRecorder::new();
    recorder.record(mirror_observation(
        &route_a,
        MirrorTaskStatus::Dropped {
            reason: MirrorReason::SampledOut,
        },
    ));
    recorder.record(mirror_observation(&route_a, MirrorTaskStatus::Completed));
    recorder.record(mirror_observation(
        &route_b,
        MirrorTaskStatus::Dropped {
            reason: MirrorReason::SampledOut,
        },
    ));

    let collector = AdaptiveSignalCollector::new(store.clone(), recorder);
    let signals = collector.collect();

    assert_eq!(signals.pool_waiting_clients, 12);
    assert_eq!(signals.pool_active_backends, 12);
    assert_eq!(signals.pool_configured_backends, 16);
    assert_eq!(signals.backpressure_waiting, 8);
    assert_eq!(signals.backpressure_rejected, 2);
    assert_eq!(signals.backpressure_timed_out, 1);
    assert_eq!(signals.backpressure_canceled, 1);
    assert_eq!(signals.mirror_observations, 3);
    assert_eq!(signals.mirror_completed, 1);
    assert_eq!(signals.mirror_dropped, 2);
}

#[test]
fn high_checkout_wait_recommends_pool_and_backpressure_review() {
    let engine = AdaptiveRecommendationEngine::new();
    let signals = AdaptiveSignalSnapshot {
        pool_waiting_clients: 12,
        pool_active_backends: 14,
        pool_configured_backends: 16,
        max_backends: 16,
        max_checkout_waiters: 24,
        max_route_waiters: 32,
        checkout_timeout_ms: 1_000,
        ..AdaptiveSignalSnapshot::default()
    };

    let recommendations = engine
        .recommend(&signals, &tuning_snapshot(), &AdaptiveConfig::default())
        .expect("recommendations are built");

    let recommendation = recommendations
        .iter()
        .find(|item| item.signal() == AdaptiveSignal::PoolSizePressure)
        .expect("pool recommendation present");

    assert_eq!(recommendation.knob(), TunableKnob::PoolSize);
    assert!(recommendation.reason().contains("pool sizing"));
    assert!(recommendation.reason().contains("backpressure thresholds"));
}

#[test]
fn repeated_overload_recommends_stricter_backpressure_or_capacity_review() {
    let engine = AdaptiveRecommendationEngine::new();
    let signals = AdaptiveSignalSnapshot {
        pool_waiting_clients: 2,
        pool_active_backends: 12,
        pool_configured_backends: 16,
        max_backends: 16,
        max_checkout_waiters: 24,
        max_route_waiters: 32,
        checkout_timeout_ms: 1_000,
        backpressure_rejected: 2,
        backpressure_timed_out: 1,
        backpressure_canceled: 1,
        ..AdaptiveSignalSnapshot::default()
    };

    let recommendations = engine
        .recommend(&signals, &tuning_snapshot(), &AdaptiveConfig::default())
        .expect("recommendations are built");

    let recommendation = recommendations
        .iter()
        .find(|item| item.signal() == AdaptiveSignal::BackpressureThresholdPressure)
        .expect("backpressure recommendation present");

    assert_eq!(recommendation.knob(), TunableKnob::BackpressureThresholds);
    assert!(recommendation.reason().contains("tighten backpressure"));
    assert!(recommendation.reason().contains("capacity"));
}

#[test]
fn mirror_drops_recommend_lower_mirror_sample_rate() {
    let engine = AdaptiveRecommendationEngine::new();
    let signals = AdaptiveSignalSnapshot {
        mirror_dropped: 2,
        mirror_observations: 3,
        mirror_completed: 1,
        ..AdaptiveSignalSnapshot::default()
    };
    let tuning = AdaptiveTuningSnapshot {
        mirror_sample_rate: 0.25,
        ..tuning_snapshot()
    };

    let recommendations = engine
        .recommend(&signals, &tuning, &AdaptiveConfig::default())
        .expect("recommendations are built");

    let recommendation = recommendations
        .iter()
        .find(|item| item.signal() == AdaptiveSignal::MirrorSamplingPressure)
        .expect("mirror recommendation present");

    assert_eq!(recommendation.knob(), TunableKnob::MirrorSampling);
    assert!(recommendation.reason().contains("lower the mirror sample rate"));
}

#[test]
fn recommendations_are_bounded_and_include_reasons() {
    let engine = AdaptiveRecommendationEngine::new();
    let signals = AdaptiveSignalSnapshot {
        pool_waiting_clients: 12,
        backpressure_rejected: 2,
        backpressure_timed_out: 1,
        backpressure_canceled: 1,
        mirror_dropped: 2,
        ..AdaptiveSignalSnapshot::default()
    };
    let recommendations = engine
        .recommend(&signals, &tuning_snapshot(), &AdaptiveConfig::default())
        .expect("recommendations are built");

    assert!(!recommendations.is_empty());
    for recommendation in recommendations {
        assert!(!recommendation.reason().trim().is_empty());
        assert_eq!(recommendation.window_ms(), 60_000);
        assert!(recommendation.safety_bound().is_bounded());
    }
}

#[test]
fn apply_mode_only_changes_allowlisted_knobs() {
    let config = tuned_config(AdaptiveMode::Apply, true, vec![TunableKnob::MirrorSampling]);
    let engine = AdaptiveApplyEngine::new();
    let tuning = tuning_snapshot();

    let allowlisted = recommendation(
        AdaptiveSignal::MirrorSamplingPressure,
        TunableKnob::MirrorSampling,
        0.91,
        "mirror sample rate is too high for current load",
        TuningBound::percent(8),
    );
    let applied = engine.apply(&config, &allowlisted, &tuning);
    assert_eq!(applied.outcome, AdaptiveOutcome::Applied);
    assert_eq!(applied.change_percent, Some(8));
    assert!(applied.before_value.expect("before value") > applied.after_value.expect("after value"));

    let rejected = recommendation(
        AdaptiveSignal::TimeoutPressure,
        TunableKnob::Timeout,
        0.91,
        "checkout timeout is too high for current load",
        TuningBound::percent(8),
    );
    let rejected = engine.apply(&config, &rejected, &tuning);
    assert_eq!(rejected.outcome, AdaptiveOutcome::Rejected);
    assert!(rejected.before_value.is_none());
    assert!(rejected.after_value.is_none());
}

#[tokio::test]
async fn apply_mode_records_before_after_values_and_can_be_disabled_by_reload() {
    let store = configured_store();
    let recorder = MirrorOutcomeRecorder::new();
    let route = route_key("reload");
    recorder.record(mirror_observation(
        &route,
        MirrorTaskStatus::Dropped {
            reason: MirrorReason::SampledOut,
        },
    ));

    let active_config = Arc::new(RwLock::new(tuned_config(
        AdaptiveMode::Apply,
        true,
        vec![TunableKnob::MirrorSampling],
    )));
    let controller = AdaptiveController::new(store.clone(), recorder, Arc::clone(&active_config));

    controller.tick().await.expect("first adaptive tick");

    let recommendation_snapshots = store.adaptive_recommendation_snapshots();
    let outcome_snapshots = store.adaptive_outcome_snapshots();
    assert_eq!(recommendation_snapshots.len(), 1);
    assert_eq!(outcome_snapshots.len(), 1);
    assert_eq!(outcome_snapshots[0].outcome, AdaptiveOutcome::Applied);
    assert!(outcome_snapshots[0].before_value.expect("before value") > outcome_snapshots[0].after_value.expect("after value"));
    assert!(!outcome_snapshots[0].disabled_by_reload);

    {
        let mut config = active_config.write().await;
        config.runtime.production.adaptive.adaptive_mode = AdaptiveMode::Recommend;
        config.runtime.production.adaptive.apply.adaptive_apply_enabled = false;
    }

    controller.tick().await.expect("second adaptive tick");

    let outcome_snapshots = store.adaptive_outcome_snapshots();
    assert_eq!(outcome_snapshots.len(), 2);
    let latest = outcome_snapshots.last().expect("latest outcome");
    assert_eq!(latest.outcome, AdaptiveOutcome::Recommended);
    assert!(latest.disabled_by_reload);
    assert!(latest.before_value.is_none());
    assert!(latest.after_value.is_none());
}
