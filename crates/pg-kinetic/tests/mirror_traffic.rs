use pg_kinetic::{
    config::{
        MirrorConfig, MirrorSafetyConfig, MirrorSamplingConfig, MirrorTargetConfig,
    },
    core::mirror::{MirrorDecision, MirrorMode, MirrorOutcome, MirrorReason, MirrorSafetyGate, MirrorSample},
};

#[test]
fn mirroring_is_disabled_by_default() {
    let config = MirrorConfig::default();

    assert!(!config.is_enabled());
    assert_eq!(config.mirror_mode.as_str(), "off");
    assert_eq!(config.mirror_timeout_ms, 100);
    assert_eq!(config.mirror_max_in_flight, 128);
    assert_eq!(config.target, MirrorTargetConfig::default());
    assert_eq!(config.sampling, MirrorSamplingConfig::default());
    assert_eq!(config.safety, MirrorSafetyConfig::default());
}

#[test]
fn mirror_modes_have_stable_labels() {
    assert_eq!(MirrorMode::Off.as_str(), "off");
    assert_eq!(MirrorMode::ReadOnly.as_str(), "read_only");
    assert_eq!(MirrorMode::Explicit.as_str(), "explicit");
}

#[test]
fn mirror_target_must_be_explicitly_configured() {
    let mut config = MirrorConfig::default();

    assert!(!config.target.is_configured());
    config.mirroring_enabled = true;
    config.mirror_mode = MirrorMode::ReadOnly;

    let error = config
        .validate("127.0.0.1:5432".parse().expect("production target"))
        .expect_err("missing target is rejected");
    assert_eq!(error, "mirror target must be explicitly configured");
}

#[test]
fn mirror_sampling_rate_is_bounded() {
    let disabled = MirrorSample::new(0.0);
    assert_eq!(disabled.rate(), 0.0);

    let enabled = MirrorSample::new(1.0);
    assert_eq!(enabled.rate(), 1.0);

    let clamped_high = MirrorSample::new(2.5);
    assert_eq!(clamped_high.rate(), 1.0);

    let clamped_low = MirrorSample::new(-0.5);
    assert_eq!(clamped_low.rate(), 0.0);

    let config = MirrorSamplingConfig {
        mirror_sample_rate: 1.5,
    };
    assert_eq!(config.sample_rate(), 1.0);
}

#[test]
fn writes_and_session_mutation_traffic_are_not_mirrored_by_default() {
    let safety = MirrorSafetyConfig::default();

    assert!(!safety.mirror_writes_enabled);
    assert!(!safety.mirror_transactions_enabled);
    assert!(!safety.mirror_copy_enabled);
    assert!(!safety.mirror_listen_notify_enabled);
    assert!(!safety.mirror_temp_table_enabled);
    assert!(!safety.mirror_session_mutation_enabled);
    assert!(safety.mirror_require_isolated_target);
}

#[test]
fn mirror_config_rejects_using_the_same_production_target_without_isolation() {
    let mut config = MirrorConfig::default();
    config.mirroring_enabled = true;
    config.mirror_mode = MirrorMode::ReadOnly;
    config.target.address = Some("127.0.0.1:5432".parse().expect("mirror target"));

    let error = config
        .validate("127.0.0.1:5432".parse().expect("production target"))
        .expect_err("shared target is rejected");
    assert_eq!(
        error,
        "mirror target must be marked isolated when it matches the production target"
    );

    config.target.isolated = true;
    config
        .validate("127.0.0.1:5432".parse().expect("production target"))
        .expect("isolated shared target is accepted");
}

#[test]
fn mirror_decisions_expose_stable_reasons() {
    let mirrored = MirrorDecision::mirrored(MirrorMode::ReadOnly, MirrorSafetyGate::Sampling);
    assert_eq!(mirrored.mode(), MirrorMode::ReadOnly);
    assert_eq!(mirrored.safety_gate(), MirrorSafetyGate::Sampling);
    assert_eq!(mirrored.reason().as_str(), "eligible");
    assert_eq!(mirrored.outcome(), MirrorOutcome::Mirrored);

    let blocked = MirrorDecision::skipped(
        MirrorMode::ReadOnly,
        MirrorSafetyGate::Writes,
        MirrorReason::WritesDisabled,
    );
    assert_eq!(blocked.reason().as_str(), "writes_disabled");
    assert_eq!(blocked.outcome(), MirrorOutcome::Skipped);

    let rejected = MirrorDecision::rejected(
        MirrorMode::Explicit,
        MirrorSafetyGate::TargetIsolated,
        MirrorReason::TargetSharedWithProduction,
    );
    assert_eq!(rejected.reason().as_str(), "target_shared_with_production");
    assert_eq!(rejected.outcome(), MirrorOutcome::Rejected);
}
