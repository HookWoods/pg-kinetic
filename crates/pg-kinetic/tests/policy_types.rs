use std::time::Duration;

use pg_kinetic::core::policy::{
    PolicyAction, PolicyAuditEvent, PolicyAuditKind, PolicyContext, PolicyContextField,
    PolicyDecision, PolicyDecisionReason, PolicyEffect, PolicyHookPoint, PolicyId, PolicyMode,
    PolicyOutcome, PolicyRouteTargetId, PolicyShardTargetId, PolicyVersion,
};

#[test]
fn policy_mode_labels_are_stable() {
    assert_eq!(PolicyMode::Disabled.as_str(), "disabled");
    assert_eq!(PolicyMode::Enforce.as_str(), "enforce");
    assert_eq!(PolicyMode::DryRun.as_str(), "dry_run");
}

#[test]
fn policy_hook_point_labels_are_stable() {
    assert_eq!(PolicyHookPoint::BeforeRouting.as_str(), "before_routing");
    assert_eq!(PolicyHookPoint::AfterRouting.as_str(), "after_routing");
    assert_eq!(PolicyHookPoint::BeforeCheckout.as_str(), "before_checkout");
}

#[test]
fn deny_action_carries_stable_reason_and_sqlstate() {
    match PolicyAction::deny() {
        PolicyAction::Deny { reason, sqlstate } => {
            assert_eq!(reason, PolicyDecisionReason::PolicyDenied);
            assert_eq!(sqlstate, PolicyAction::DENY_SQLSTATE);
        }
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn allow_action_does_not_override_routing_by_itself() {
    let action = PolicyAction::allow();

    assert_eq!(action.effect(), PolicyEffect::NoChange);
    assert!(!action.overrides_routing());
}

#[test]
fn explicit_primary_and_replica_actions_override_routing() {
    assert_eq!(
        PolicyAction::require_primary().effect(),
        PolicyEffect::RequirePrimary
    );
    assert_eq!(
        PolicyAction::require_replica().effect(),
        PolicyEffect::RequireReplica
    );
    assert!(PolicyAction::require_primary().overrides_routing());
    assert!(PolicyAction::require_replica().overrides_routing());
}

#[test]
fn override_actions_require_validated_target_ids() {
    assert!(PolicyRouteTargetId::new("").is_err());
    assert!(PolicyShardTargetId::new("").is_err());

    let route_target_id = PolicyRouteTargetId::new("route-a").unwrap();
    let shard_target_id = PolicyShardTargetId::new("shard-a").unwrap();

    match PolicyAction::route_override(route_target_id.clone()) {
        PolicyAction::RouteOverride { target_id } => assert_eq!(target_id, route_target_id),
        other => panic!("unexpected action: {other:?}"),
    }

    match PolicyAction::shard_override(shard_target_id.clone()) {
        PolicyAction::ShardOverride { target_id } => assert_eq!(target_id, shard_target_id),
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn policy_decision_records_core_fields() {
    let policy_id = PolicyId::new("route-fallback").unwrap();
    let policy_version = PolicyVersion::new(7).unwrap();
    let action = PolicyAction::require_primary();
    let outcome = PolicyOutcome::Applied;
    let hook_point = PolicyHookPoint::AfterRouting;
    let latency = Duration::from_millis(12);

    let decision = PolicyDecision::new(
        policy_id.clone(),
        policy_version,
        action.clone(),
        outcome,
        hook_point,
        latency,
    );

    assert_eq!(decision.policy_id, policy_id);
    assert_eq!(decision.policy_version, policy_version);
    assert_eq!(decision.action, action);
    assert_eq!(decision.outcome, outcome);
    assert_eq!(decision.hook_point, hook_point);
    assert_eq!(decision.latency, latency);
}

#[test]
fn policy_audit_event_redacts_secrets() {
    let decision = PolicyDecision::new(
        PolicyId::new("policy-audit").unwrap(),
        PolicyVersion::new(1).unwrap(),
        PolicyAction::deny(),
        PolicyOutcome::Rejected,
        PolicyHookPoint::BeforeRouting,
        Duration::from_millis(1),
    );
    let context = PolicyContext::new(vec![
        PolicyContextField::public("route", "primary"),
        PolicyContextField::secret("password", "swordfish"),
    ]);
    let audit_event = PolicyAuditEvent::new(PolicyAuditKind::Decision, decision, context);

    let rendered = format!("{audit_event:?}");
    assert!(!rendered.contains("swordfish"));
    assert!(rendered.contains("<redacted>"));
}
