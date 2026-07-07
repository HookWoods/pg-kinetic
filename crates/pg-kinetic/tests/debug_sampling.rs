use std::net::SocketAddr;

use pg_kinetic::{
    core::{
        observability::{MetricOutcome, ProtocolPhase},
        recovery::{RecoveryAction, RecoveryTrigger},
        route::{QueryClass, RouteKey},
        virtual_session::PinReason,
    },
    proxy_runtime::telemetry::{redact_debug_value, DebugSample, DebugSampler},
};

fn test_route_key() -> RouteKey {
    RouteKey::new(
        "postgres",
        "pgkinetic",
        Some("api"),
        Some(
            "127.0.0.1:5432"
                .parse::<SocketAddr>()
                .expect("socket address"),
        ),
        QueryClass::Write,
    )
}

#[test]
fn debug_sampler_respects_the_sampling_rate_bounds() {
    let disabled = DebugSampler::new(0.0);
    assert_eq!(disabled.sampling_ratio(), 0.0);
    assert!(!disabled.should_sample(7));
    assert!(disabled
        .sample(
            7,
            DebugSample::client_close(7, "127.0.0.1:1".parse().expect("addr"), Default::default())
        )
        .is_none());

    let enabled = DebugSampler::new(1.0);
    assert_eq!(enabled.sampling_ratio(), 1.0);
    assert!(enabled.should_sample(7));
    assert!(enabled
        .sample(
            7,
            DebugSample::client_close(7, "127.0.0.1:1".parse().expect("addr"), Default::default())
        )
        .is_some());

    let clamped_high = DebugSampler::new(2.5);
    assert_eq!(clamped_high.sampling_ratio(), 1.0);

    let clamped_low = DebugSampler::new(-1.0);
    assert_eq!(clamped_low.sampling_ratio(), 0.0);
}

#[test]
fn sampled_events_carry_structured_safe_fields() {
    let sampler = DebugSampler::new(1.0);
    let session_id = 42;
    let route_key = test_route_key();

    let client_accepted = sampler
        .sample(
            session_id,
            DebugSample::client_accepted(
                session_id,
                "127.0.0.1:12345".parse().expect("socket address"),
                "verify_client",
                true,
            ),
        )
        .expect("client accepted sample");
    assert_eq!(client_accepted.session_id, session_id);
    assert_eq!(client_accepted.phase, Some(ProtocolPhase::Startup));
    assert_eq!(client_accepted.outcome, Some(MetricOutcome::Ok));
    assert!(client_accepted.route_key.is_none());

    let startup_complete = sampler
        .sample(
            session_id,
            DebugSample::startup_complete(
                session_id,
                route_key.clone(),
                "trust",
                "allow",
                MetricOutcome::Ok,
            ),
        )
        .expect("startup sample");
    assert_eq!(startup_complete.route_key, Some(route_key.clone()));
    assert_eq!(startup_complete.phase, Some(ProtocolPhase::Startup));
    assert_eq!(startup_complete.outcome, Some(MetricOutcome::Ok));

    let backend_checkout = sampler
        .sample(
            session_id,
            DebugSample::backend_checkout(
                session_id,
                route_key.clone(),
                "reuse_only",
                MetricOutcome::Ok,
                std::time::Duration::from_millis(12),
            ),
        )
        .expect("backend checkout sample");
    assert_eq!(backend_checkout.route_key, Some(route_key.clone()));
    assert_eq!(backend_checkout.phase, Some(ProtocolPhase::BackendCheckout));
    assert_eq!(backend_checkout.outcome, Some(MetricOutcome::Ok));

    let query_complete = sampler
        .sample(
            session_id,
            DebugSample::query_complete(
                session_id,
                route_key.clone(),
                MetricOutcome::Ok,
                4,
                "idle",
                Some("SELECT password FROM users WHERE token = $1"),
                &["secret-token", "certificate-bytes"],
            ),
        )
        .expect("query sample");
    assert_eq!(query_complete.route_key, Some(route_key.clone()));
    assert_eq!(query_complete.phase, Some(ProtocolPhase::Rows));
    assert_eq!(query_complete.outcome, Some(MetricOutcome::Ok));

    let pinning = sampler
        .sample(
            session_id,
            DebugSample::pinning(
                session_id,
                route_key.clone(),
                PinReason::OpenTransaction,
                9,
                std::time::Duration::from_millis(33),
            ),
        )
        .expect("pinning sample");
    assert_eq!(pinning.pin_reason, Some(PinReason::OpenTransaction));
    assert_eq!(pinning.phase, Some(ProtocolPhase::Close));

    let recovery = sampler
        .sample(
            session_id,
            DebugSample::recovery(
                session_id,
                route_key.clone(),
                RecoveryTrigger::AbandonedResponse,
                RecoveryAction::DrainAndSync,
                MetricOutcome::Timeout,
            ),
        )
        .expect("recovery sample");
    assert_eq!(recovery.recovery_action, Some(RecoveryAction::DrainAndSync));
    assert_eq!(recovery.outcome, Some(MetricOutcome::Timeout));

    let overload = sampler
        .sample(
            session_id,
            DebugSample::overload_rejected(session_id, route_key.clone(), "allow_connect"),
        )
        .expect("overload sample");
    assert_eq!(overload.phase, Some(ProtocolPhase::BackendCheckout));
    assert_eq!(overload.outcome, Some(MetricOutcome::Rejected));

    let client_close = sampler
        .sample(
            session_id,
            DebugSample::client_close(
                session_id,
                "127.0.0.1:12345".parse().expect("socket address"),
                std::time::Duration::from_millis(4),
            ),
        )
        .expect("client close sample");
    assert_eq!(client_close.phase, Some(ProtocolPhase::Close));
    assert_eq!(client_close.outcome, Some(MetricOutcome::Canceled));
}

#[test]
fn redaction_hides_sensitive_payloads() {
    assert_eq!(redact_debug_value(""), "");
    assert_eq!(redact_debug_value("SELECT * FROM users"), "<redacted>");
    assert_eq!(redact_debug_value("password=letmein"), "<redacted>");
    assert_eq!(
        redact_debug_value("-----BEGIN CERTIFICATE-----"),
        "<redacted>"
    );

    let sample = DebugSample::query_complete(
        9,
        test_route_key(),
        MetricOutcome::Ok,
        0,
        "idle",
        Some("SELECT * FROM secrets WHERE password = $1"),
        &["super-secret", "BEGIN CERTIFICATE"],
    );
    let rendered = format!("{sample:?}");
    for forbidden in [
        "SELECT * FROM secrets",
        "password = $1",
        "super-secret",
        "BEGIN CERTIFICATE",
        "certificate",
        "letmein",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "debug sample leaked forbidden payload: {forbidden}"
        );
    }
    assert!(rendered.contains("<redacted>"));
}
