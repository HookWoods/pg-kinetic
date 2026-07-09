use std::{sync::Arc, time::Duration};

use pg_kinetic::{
    core::{
        policy::{
            PolicyAction, PolicyAuditKind, PolicyDecision, PolicyHookPoint, PolicyId, PolicyMode,
            PolicyOutcome, PolicyRouteTargetId, PolicyVersion,
        },
        routing::{BackendRole, FallbackPolicy, FreshnessPolicy, QueryClass as CoreQueryClass},
        session::TransactionAccessMode,
        virtual_session::ReadAfterWriteState,
    },
    proxy::{apply_policy_action_to_routing_target_with_mode, policy_audit_event_from_decision},
    proxy_runtime::{
        policy::{PolicyEvalInput, PolicyRuntime},
        routing::{
            choose_routing_target, ReadRoutingPlanner, ReplicaCandidate, RouteHealthSnapshot,
            RoutingContext,
        },
        snapshot::SnapshotStore,
    },
};

#[test]
fn dry_run_deny_keeps_original_target_and_records_would_deny() {
    let runtime = sample_runtime(1.0).with_policy_mode(PolicyMode::DryRun);
    let planner = sample_planner();
    let context = sample_routing_context();
    let current_target = choose_routing_target(&planner, context.clone());
    let action = PolicyAction::deny();

    let routed = apply_policy_action_to_routing_target_with_mode(
        &planner,
        context,
        Some(current_target.clone()),
        Some(&action),
        PolicyMode::DryRun,
    );

    assert_eq!(routed, current_target);

    let event = policy_audit_event_from_decision(
        &runtime,
        PolicyAuditKind::Decision,
        sample_decision(
            PolicyId::new("deny-policy").expect("policy id"),
            PolicyVersion::new(1).expect("policy version"),
            action,
            PolicyOutcome::DryRun,
            PolicyHookPoint::BeforeRouting,
        ),
        &sample_policy_input(),
        &current_target,
    );

    assert_eq!(event.reason.as_deref(), Some("would_deny"));
    assert_eq!(event.outcome, PolicyOutcome::DryRun);
}

#[test]
fn dry_run_route_override_keeps_original_target_and_records_would_override() {
    let runtime = sample_runtime(1.0).with_policy_mode(PolicyMode::DryRun);
    let planner = sample_planner();
    let context = sample_routing_context();
    let current_target = choose_routing_target(&planner, context.clone());
    let action = PolicyAction::route_override(
        PolicyRouteTargetId::new("route-b").expect("valid route target"),
    );

    let routed = apply_policy_action_to_routing_target_with_mode(
        &planner,
        context,
        Some(current_target.clone()),
        Some(&action),
        PolicyMode::DryRun,
    );

    assert_eq!(routed, current_target);

    let event = policy_audit_event_from_decision(
        &runtime,
        PolicyAuditKind::Decision,
        sample_decision(
            PolicyId::new("route-policy").expect("policy id"),
            PolicyVersion::new(2).expect("policy version"),
            action,
            PolicyOutcome::DryRun,
            PolicyHookPoint::AfterRouting,
        ),
        &sample_policy_input(),
        &current_target,
    );

    assert_eq!(event.reason.as_deref(), Some("would_override"));
    assert_eq!(event.outcome, PolicyOutcome::DryRun);
}

#[test]
fn dry_run_require_primary_keeps_original_target_and_records_would_require_primary() {
    let runtime = sample_runtime(1.0).with_policy_mode(PolicyMode::DryRun);
    let planner = sample_planner();
    let context = sample_routing_context();
    let current_target = choose_routing_target(&planner, context.clone());
    let action = PolicyAction::require_primary();

    let routed = apply_policy_action_to_routing_target_with_mode(
        &planner,
        context,
        Some(current_target.clone()),
        Some(&action),
        PolicyMode::DryRun,
    );

    assert_eq!(routed, current_target);

    let event = policy_audit_event_from_decision(
        &runtime,
        PolicyAuditKind::Decision,
        sample_decision(
            PolicyId::new("primary-policy").expect("policy id"),
            PolicyVersion::new(3).expect("policy version"),
            action,
            PolicyOutcome::DryRun,
            PolicyHookPoint::BeforeCheckout,
        ),
        &sample_policy_input(),
        &current_target,
    );

    assert_eq!(event.reason.as_deref(), Some("would_require_primary"));
    assert_eq!(event.outcome, PolicyOutcome::DryRun);
}

#[test]
fn audit_event_includes_safe_metadata_without_sensitive_inputs() {
    let runtime = sample_runtime(1.0);
    let action = PolicyAction::deny();
    let event = runtime.build_audit_event_from_input(
        PolicyAuditKind::Decision,
        sample_decision(
            PolicyId::new("audit-policy").expect("policy id"),
            PolicyVersion::new(4).expect("policy version"),
            action,
            PolicyOutcome::DryRun,
            PolicyHookPoint::BeforeRouting,
        ),
        &sample_policy_input(),
    );

    assert_eq!(event.policy_id.as_str(), "audit-policy");
    assert_eq!(event.policy_version.as_u64(), 4);
    assert_eq!(event.hook_point, PolicyHookPoint::BeforeRouting);
    assert_eq!(event.action.as_str(), "deny");
    assert_eq!(event.outcome, PolicyOutcome::DryRun);
    assert_eq!(event.reason.as_deref(), Some("would_deny"));
    assert_eq!(event.route.as_deref(), Some("read-route"));
    assert_eq!(event.shard.as_deref(), Some("tenant-a"));
    assert_eq!(event.target_role.as_deref(), Some("replica"));

    let rendered = format!("{event:?}");
    for forbidden in [
        "swordfish",
        "alpha=1",
        "BEGIN CERTIFICATE",
        "super-secret-token",
        "SELECT password FROM users WHERE token = $1",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "audit event leaked sensitive payload: {forbidden}"
        );
    }
    assert!(rendered.contains("<redacted>"));
}

#[test]
fn audit_sampling_rate_zero_emits_no_sampled_events() {
    let runtime = sample_runtime(0.0);
    let snapshot_store = SnapshotStore::new();
    let event = runtime.build_audit_event_from_input(
        PolicyAuditKind::Decision,
        sample_decision(
            PolicyId::new("sample-policy").expect("policy id"),
            PolicyVersion::new(5).expect("policy version"),
            PolicyAction::deny(),
            PolicyOutcome::DryRun,
            PolicyHookPoint::BeforeRouting,
        ),
        &sample_policy_input(),
    );

    assert!(!runtime.record_audit_event(&snapshot_store, &event));
    assert!(snapshot_store.policy_audit_events().is_empty());
}

#[test]
fn audit_sampling_rate_one_emits_deterministic_events() {
    let runtime = sample_runtime(1.0);
    let snapshot_store = SnapshotStore::new();
    let event = runtime.build_audit_event_from_input(
        PolicyAuditKind::Decision,
        sample_decision(
            PolicyId::new("sample-policy").expect("policy id"),
            PolicyVersion::new(6).expect("policy version"),
            PolicyAction::deny(),
            PolicyOutcome::DryRun,
            PolicyHookPoint::BeforeRouting,
        ),
        &sample_policy_input(),
    );

    assert!(runtime.should_sample_audit_event(&event));
    assert!(runtime.record_audit_event(&snapshot_store, &event));
    assert!(runtime.record_audit_event(&snapshot_store, &event));

    let events = snapshot_store.policy_audit_events();
    assert_eq!(events, vec![event.clone(), event]);
}

fn sample_runtime(sample_rate: f64) -> PolicyRuntime {
    PolicyRuntime::new(Duration::from_millis(5), 8_192)
        .with_policy_audit_enabled(true)
        .with_policy_audit_sample_rate(sample_rate)
}

fn sample_planner() -> ReadRoutingPlanner {
    ReadRoutingPlanner::new(
        pg_kinetic::core::routing::ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        1_000,
    )
}

fn sample_routing_context<'a>() -> RoutingContext<'a> {
    let health = sample_health();
    RoutingContext::new(
        "SELECT 1",
        pg_kinetic::core::session::TransactionState::Idle,
        ReadAfterWriteState::Required(sample_lsn()),
        Box::leak(Box::new(health)),
    )
}

fn sample_health() -> RouteHealthSnapshot {
    RouteHealthSnapshot::new(vec![ReplicaCandidate {
        replica_id: 7,
        healthy: true,
        split_brain: false,
        replay_lsn: Some(sample_lsn()),
        lag_ms: Some(12),
    }])
}

fn sample_policy_input() -> PolicyEvalInput {
    let routing_decision = pg_kinetic::core::routing::RoutingDecision::new(
        BackendRole::Replica,
        CoreQueryClass::ReadOnly,
        pg_kinetic::core::routing::RoutingHint::StrictFresh,
        pg_kinetic::core::routing::RoutingReason::ReadOnlyQuery,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
    );

    PolicyEvalInput {
        database: Arc::from("billing"),
        user: Arc::from("reporter"),
        application_name: Some(Arc::from("dashboard")),
        route: Some(Arc::from("read-route")),
        shard: Some(Arc::from("tenant-a")),
        backend_role: BackendRole::Replica,
        query_class: CoreQueryClass::ReadOnly,
        transaction_mode: TransactionAccessMode::ReadOnly,
        freshness_state: pg_kinetic::core::lsn::FreshnessStatus::Waiting,
        routing_decision: Some(routing_decision),
        shard_route_decision: None,
        password: Some(Arc::from("swordfish")),
        bind_values: vec![Arc::from("alpha=1"), Arc::from("beta=2")],
        tls_certificate_body: Some(Arc::from("-----BEGIN CERTIFICATE-----")),
        raw_sql_text: Some(Arc::from("SELECT password FROM users WHERE token = $1")),
        secrets: vec![Arc::from("super-secret-token")],
    }
}

fn sample_decision(
    policy_id: PolicyId,
    policy_version: PolicyVersion,
    action: PolicyAction,
    outcome: PolicyOutcome,
    hook_point: PolicyHookPoint,
) -> PolicyDecision {
    PolicyDecision::new(
        policy_id,
        policy_version,
        action,
        outcome,
        hook_point,
        Duration::from_millis(1),
    )
}

fn sample_lsn() -> pg_kinetic::core::lsn::PgLsn {
    "0/16B6C50".parse().expect("lsn")
}
