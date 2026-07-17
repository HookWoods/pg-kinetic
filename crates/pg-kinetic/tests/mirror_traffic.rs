use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use pg_kinetic::{
    config::{
        MirrorConfig, MirrorSafetyConfig, MirrorSamplingConfig, MirrorTargetConfig, SocketConfig,
        TlsConfig,
    },
    core::{
        mirror::{
            MirrorDecision, MirrorMode, MirrorOutcome, MirrorReason, MirrorSafetyGate,
            MirrorSample, MirrorTarget,
        },
        route::{QueryClass as RouteQueryClass, RouteKey},
        sql::SqlCommand,
    },
    proxy_runtime::{
        mirror::{
            MirrorDispatchConfig, MirrorDispatcher, MirrorOutcomeRecorder, MirrorSampler,
            MirrorTask, MirrorTaskStatus,
        },
        routing::{RoutingReason, RoutingTarget},
    },
    wire::{frame::FrontendFrame, protocol::FrontendTag},
};
use tokio::{sync::Notify, time::timeout};

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

fn route_key(label: &str) -> RouteKey {
    RouteKey::new(
        "pgkinetic",
        "postgres",
        Some(label),
        None,
        RouteQueryClass::Default,
    )
}

fn query_frame(sql: &str) -> FrontendFrame {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.extend_from_slice(&[0]);
    FrontendFrame {
        tag: u8::from(FrontendTag::Query),
        payload: payload.freeze(),
    }
}

fn dispatch_config(
    mode: MirrorMode,
    target_isolated: bool,
    sample_rate: f64,
    timeout_ms: u64,
    max_in_flight: usize,
) -> MirrorDispatchConfig {
    MirrorDispatchConfig {
        production_target: "127.0.0.1:5432".parse().expect("production target"),
        target: Some(MirrorTarget::new(
            "127.0.0.1:6543".parse().expect("mirror target"),
            target_isolated,
        )),
        mode,
        sample_rate,
        safety: MirrorSafetyConfig::default(),
        timeout: Duration::from_millis(timeout_ms),
        max_in_flight,
        tls: TlsConfig::default(),
        socket: SocketConfig::default(),
    }
}

fn mirror_task(
    session_id: u64,
    query_id: u64,
    command: SqlCommand,
    route_target: RoutingTarget,
    sql: &str,
) -> MirrorTask {
    MirrorTask::new(
        session_id,
        query_id,
        route_key("mirror-route"),
        route_target,
        command,
        Bytes::from_static(b"startup"),
        Vec::new(),
        vec![query_frame(sql)],
        None,
    )
}

#[tokio::test]
async fn mirror_dispatch_is_best_effort_and_never_blocks_production_response() {
    let recorder = MirrorOutcomeRecorder::new();
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let dispatcher = MirrorDispatcher::with_runner(
        dispatch_config(MirrorMode::ReadOnly, true, 1.0, 1_000, 4),
        recorder.clone(),
        {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            move |_task| {
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                async move {
                    started.fetch_add(1, Ordering::Relaxed);
                    release.notified().await;
                    Ok(())
                }
            }
        },
    );

    let start = Instant::now();
    let decision = dispatcher.dispatch(mirror_task(
        42,
        7,
        SqlCommand::Query,
        RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        "SELECT 1",
    ));
    assert_eq!(decision.outcome(), MirrorOutcome::Mirrored);
    assert!(start.elapsed() < Duration::from_millis(250));

    timeout(Duration::from_secs(1), async {
        while started.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mirror task started");

    release.notify_waiters();
    recorder.wait_for_count(1).await;

    let observation = &recorder.snapshot()[0];
    assert_eq!(observation.status, MirrorTaskStatus::Completed);
}

#[tokio::test]
async fn mirror_timeout_records_timeout_outcome() {
    let recorder = MirrorOutcomeRecorder::new();
    let dispatcher = MirrorDispatcher::with_runner(
        dispatch_config(MirrorMode::ReadOnly, true, 1.0, 50, 4),
        recorder.clone(),
        |_task| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(())
        },
    );

    let decision = dispatcher.dispatch(mirror_task(
        9,
        1,
        SqlCommand::Query,
        RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        "SELECT 1",
    ));
    assert_eq!(decision.outcome(), MirrorOutcome::Mirrored);

    recorder.wait_for_count(1).await;
    let observation = &recorder.snapshot()[0];
    assert_eq!(observation.status, MirrorTaskStatus::TimedOut);
}

#[tokio::test]
async fn mirror_max_in_flight_drops_samples_with_stable_reason() {
    let recorder = MirrorOutcomeRecorder::new();
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let dispatcher = MirrorDispatcher::with_runner(
        dispatch_config(MirrorMode::ReadOnly, true, 1.0, 1_000, 1),
        recorder.clone(),
        {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            move |_task| {
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                async move {
                    started.fetch_add(1, Ordering::Relaxed);
                    release.notified().await;
                    Ok(())
                }
            }
        },
    );

    let first = dispatcher.dispatch(mirror_task(
        100,
        1,
        SqlCommand::Query,
        RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        "SELECT 1",
    ));
    assert_eq!(first.outcome(), MirrorOutcome::Mirrored);

    timeout(Duration::from_secs(1), async {
        while started.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mirror task started");

    let second = dispatcher.dispatch(mirror_task(
        100,
        2,
        SqlCommand::Query,
        RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        "SELECT 2",
    ));
    assert_eq!(second.outcome(), MirrorOutcome::Skipped);
    assert_eq!(second.reason().as_str(), "sampled_out");

    release.notify_waiters();
    recorder.wait_for_count(1).await;
    let snapshot = recorder.snapshot();
    assert_eq!(
        snapshot[0].status,
        MirrorTaskStatus::Dropped {
            reason: MirrorReason::SampledOut,
        }
    );

    recorder.wait_for_count(2).await;
    assert_eq!(recorder.snapshot()[1].status, MirrorTaskStatus::Completed);
}

#[tokio::test]
async fn mirror_results_are_discarded_and_never_sent_to_clients() {
    let recorder = MirrorOutcomeRecorder::new();
    let dispatcher = MirrorDispatcher::with_runner(
        dispatch_config(MirrorMode::ReadOnly, true, 1.0, 1_000, 4),
        recorder.clone(),
        |_task| async move { Ok(()) },
    );

    let decision = dispatcher.dispatch(mirror_task(
        7,
        3,
        SqlCommand::Query,
        RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        "SELECT 'secret-response'",
    ));
    assert_eq!(decision.outcome(), MirrorOutcome::Mirrored);

    recorder.wait_for_count(1).await;
    let observation = &recorder.snapshot()[0];
    let debug = format!("{observation:?}");
    assert!(!debug.contains("secret-response"));
    assert!(!debug.contains("rows"));
    assert_eq!(observation.status, MirrorTaskStatus::Completed);
}

#[tokio::test]
async fn mirror_errors_do_not_fail_production_traffic() {
    let recorder = MirrorOutcomeRecorder::new();
    let dispatcher = MirrorDispatcher::with_runner(
        dispatch_config(MirrorMode::ReadOnly, true, 1.0, 1_000, 4),
        recorder.clone(),
        |_task| async move { Err(anyhow::anyhow!("mirror backend failed")) },
    );

    let decision = dispatcher.dispatch(mirror_task(
        8,
        4,
        SqlCommand::Query,
        RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        "SELECT 1",
    ));
    assert_eq!(decision.outcome(), MirrorOutcome::Mirrored);

    recorder.wait_for_count(1).await;
    assert_eq!(recorder.snapshot()[0].status, MirrorTaskStatus::Error);
}

#[tokio::test]
async fn mirror_uses_redacted_telemetry_only() {
    let recorder = MirrorOutcomeRecorder::new();
    let dispatcher = MirrorDispatcher::with_runner(
        dispatch_config(MirrorMode::ReadOnly, true, 1.0, 1_000, 4),
        recorder.clone(),
        |_task| async move { Ok(()) },
    );

    let _ = dispatcher.dispatch(mirror_task(
        11,
        17,
        SqlCommand::Query,
        RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        "SELECT 'bind-value-should-not-appear'",
    ));

    recorder.wait_for_count(1).await;
    let observation = &recorder.snapshot()[0];
    assert_eq!(observation.telemetry.command_label, "query");
    let debug = format!("{observation:?}");
    assert!(!debug.contains("bind-value-should-not-appear"));
    assert!(!debug.contains("SELECT 'bind-value-should-not-appear'"));
}

#[tokio::test]
async fn mirror_dispatch_respects_policy_deny_and_recovery_drain_safety() {
    let recorder = MirrorOutcomeRecorder::new();
    let dispatcher = MirrorDispatcher::with_runner(
        dispatch_config(MirrorMode::ReadOnly, true, 1.0, 1_000, 4),
        recorder.clone(),
        |_task| async move { Ok(()) },
    );

    let rejected = dispatcher.dispatch(mirror_task(
        19,
        1,
        SqlCommand::Query,
        RoutingTarget::Reject {
            reason: RoutingReason::PolicyDenied,
        },
        "SELECT 1",
    ));
    assert_eq!(rejected.outcome(), MirrorOutcome::Rejected);
    assert_eq!(rejected.reason().as_str(), "disabled");

    let skipped = dispatcher.dispatch(mirror_task(
        19,
        2,
        SqlCommand::Query,
        RoutingTarget::Wait {
            reason: RoutingReason::FallbackWait,
        },
        "SELECT 1",
    ));
    assert_eq!(skipped.outcome(), MirrorOutcome::Skipped);
    assert_eq!(skipped.reason().as_str(), "unsupported_mode");

    recorder.wait_for_count(2).await;
    let snapshot = recorder.snapshot();
    assert_eq!(
        snapshot[0].status,
        MirrorTaskStatus::Rejected {
            reason: MirrorReason::Disabled,
        }
    );
    assert_eq!(
        snapshot[1].status,
        MirrorTaskStatus::Skipped {
            reason: MirrorReason::UnsupportedMode,
        }
    );
}

#[test]
fn deterministic_sampling_is_testable_per_session_query_id() {
    let sampler = MirrorSampler::with_seed(0.25, 0x1234);
    let sample_one = sampler.sample_value(7, 11);
    let sample_two = sampler.sample_value(7, 11);
    let sample_three = sampler.sample_value(7, 12);

    assert_eq!(sample_one, sample_two);
    assert!((0.0..=1.0).contains(&sample_one));
    assert_ne!(sample_one, sample_three);
    assert_eq!(sampler.should_mirror(7, 11), sampler.should_mirror(7, 11));
}
