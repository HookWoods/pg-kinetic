use pg_kinetic::config::{AdaptiveApplyConfig, AdaptiveConfig, AdaptiveGuardrailConfig, ProductionConfig};
use pg_kinetic_core::adaptive::{
    AdaptiveAction, AdaptiveGuardrail, AdaptiveMode, AdaptiveOutcome, AdaptiveRecommendation,
    AdaptiveSignal, TuningBound, TunableKnob,
};

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
