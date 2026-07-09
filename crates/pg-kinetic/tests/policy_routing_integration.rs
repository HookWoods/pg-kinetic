use std::time::Duration;

use pg_kinetic::{
    config::PolicyConfig,
    core::{
        lsn::{FreshnessStatus, PgLsn},
        policy::{
            PolicyAction, PolicyContext, PolicyContextField, PolicyHookPoint, PolicyId,
            PolicyMode, PolicyOutcome, PolicyPluginAction, PolicyPluginInput,
            PolicyPluginOutput, PolicyRouteTargetId, PolicyShardTargetId, PolicyVersion,
        },
        route::{QueryClass, RouteKey},
        routing::{FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
        session::TransactionState,
        virtual_session::ReadAfterWriteState,
    },
    proxy::{
        apply_policy_after_routing_target, apply_policy_before_checkout_target,
        apply_policy_before_routing_target, checkout_debug_fields, checkout_postgres_error_for_target,
        route_checkout_snapshot_for_target,
    },
    proxy_runtime::{
        routing::{
            choose_routing_target, ReadRoutingPlanner, ReplicaCandidate, RouteHealthSnapshot,
            RoutingContext, RoutingReason, RoutingTarget,
        },
        sharding::{
            apply_policy_action_to_sharded_routing_target, ShardRoutingContext, ShardRoutingPlanner,
            ShardRouteMapStore,
        },
        policy::PolicyPluginHostLimits,
    },
};

#[test]
fn disabled_policy_mode_leaves_routing_untouched() {
    let policy = PolicyConfig::default();
    assert_eq!(policy.policy_mode, PolicyMode::Disabled);

    let planner = sample_planner();
    let context = sample_routing_context();
    let expected = choose_routing_target(&planner, context.clone());

    let before_routing = apply_policy_before_routing_target(&planner, context.clone(), None);
    let after_routing =
        apply_policy_after_routing_target(&planner, context.clone(), expected.clone(), None);
    let before_checkout =
        apply_policy_before_checkout_target(&planner, context, expected.clone(), None);

    assert_eq!(before_routing, expected);
    assert_eq!(after_routing, expected);
    assert_eq!(before_checkout, expected);
}

#[test]
fn before_routing_deny_blocks_checkout_with_stable_sqlstate() {
    let planner = sample_planner();
    let context = sample_routing_context();
    let deny = PolicyAction::deny();

    let target = apply_policy_before_routing_target(&planner, context, Some(&deny));

    assert!(matches!(
        target,
        RoutingTarget::Reject {
            reason: RoutingReason::PolicyDenied,
        }
    ));
    assert_eq!(
        checkout_postgres_error_for_target(&target),
        Some(("P0001", "policy denied"))
    );
}

#[test]
fn after_routing_require_primary_forces_primary() {
    let planner = sample_planner();
    let context = sample_routing_context();
    let require_primary = PolicyAction::require_primary();
    let current_target = RoutingTarget::Replica {
        candidate: healthy_replica_candidate(1, Some(sample_lsn()), Some(7)),
        reason: RoutingReason::ReadCandidateQuery,
    };

    let target = apply_policy_after_routing_target(
        &planner,
        context,
        current_target,
        Some(&require_primary),
    );

    assert!(matches!(
        target,
        RoutingTarget::Primary {
            reason: RoutingReason::PolicyRequirePrimary,
        }
    ));
}

#[test]
fn after_routing_require_replica_respects_freshness_and_health() {
    let planner = ReadRoutingPlanner::new(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Reject,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        1_000,
    );
    let healthy_health = RouteHealthSnapshot::new(vec![healthy_replica_candidate(
        7,
        Some(sample_lsn()),
        Some(12),
    )]);
    let healthy_context = RoutingContext::new(
        "SELECT 1",
        TransactionState::Idle,
        ReadAfterWriteState::Required(sample_lsn()),
        &healthy_health,
    );
    let require_replica = PolicyAction::require_replica();
    let current_target = RoutingTarget::Primary {
        reason: RoutingReason::ReadOnlyQuery,
    };

    let healthy_target = apply_policy_after_routing_target(
        &planner,
        healthy_context,
        current_target.clone(),
        Some(&require_replica),
    );

    assert!(matches!(
        healthy_target,
        RoutingTarget::Replica {
            candidate,
            reason: RoutingReason::PolicyRequireReplica,
        } if candidate.replica_id == 7
    ));

    let unhealthy_health = RouteHealthSnapshot::new(vec![ReplicaCandidate {
        replica_id: 9,
        healthy: false,
        split_brain: false,
        replay_lsn: Some(sample_lsn()),
        lag_ms: Some(12),
    }]);
    let unhealthy_context = RoutingContext::new(
        "SELECT 1",
        TransactionState::Idle,
        ReadAfterWriteState::Required(sample_lsn()),
        &unhealthy_health,
    );
    let rejected_target = apply_policy_after_routing_target(
        &planner,
        unhealthy_context,
        current_target,
        Some(&require_replica),
    );

    assert!(matches!(
        rejected_target,
        RoutingTarget::Reject {
            reason: RoutingReason::PolicyRequireReplica,
        }
    ));
}

#[test]
fn route_override_and_shard_override_validate_against_phase_8_context() {
    let route_policy = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"

        [[policy.inline_rules]]
        policy_id = "route-fallback"
        hook_point = "after_routing"
        kind = "route_override"
        target_id = "route-1"

        [[policy.inline_rules]]
        policy_id = "shard-fallback"
        hook_point = "after_routing"
        kind = "shard_override"
        target_id = "tenant-a"
        "#,
    )
    .expect("policy config parses");

    let route_error = route_policy
        .policy
        .validate_with_context(["route-0"], false, std::iter::empty::<&str>())
        .expect_err("invalid route target is rejected");
    assert_eq!(
        route_error,
        "route override target 'route-1' does not reference an existing route"
    );

    let shard_policy = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"

        [[policy.inline_rules]]
        policy_id = "shard-fallback"
        hook_point = "after_routing"
        kind = "shard_override"
        target_id = "tenant-a"
        "#,
    )
    .expect("policy config parses");

    let shard_error = shard_policy
        .policy
        .validate_with_context(["route-0"], true, ["tenant-b"])
        .expect_err("invalid shard target is rejected");
    assert_eq!(
        shard_error,
        "shard override target 'tenant-a' does not reference an existing shard"
    );
}

#[test]
fn policy_overrides_do_not_bypass_health_checks() {
    let planner = sample_planner();
    let context = sample_routing_context();
    let require_replica = PolicyAction::require_replica();
    let stale_health = RouteHealthSnapshot::new(vec![ReplicaCandidate {
        replica_id: 3,
        healthy: true,
        split_brain: false,
        replay_lsn: Some("0/1".parse().expect("lsn")),
        lag_ms: Some(5_000),
    }]);
    let stale_context = RoutingContext::new(
        "SELECT 1",
        TransactionState::Idle,
        ReadAfterWriteState::Required("0/2".parse().expect("lsn")),
        &stale_health,
    );

    let target = apply_policy_before_checkout_target(
        &planner,
        stale_context,
        RoutingTarget::Primary {
            reason: RoutingReason::ReadOnlyQuery,
        },
        Some(&require_replica),
    );

    assert!(matches!(
        target,
        RoutingTarget::Reject {
            reason: RoutingReason::PolicyRequireReplica,
        }
    ));

    let route_override = PolicyAction::route_override(
        PolicyRouteTargetId::new("route-1").expect("valid route target"),
    );
    let route_override_target = apply_policy_after_routing_target(
        &planner,
        context.clone(),
        RoutingTarget::Primary {
            reason: RoutingReason::ReadOnlyQuery,
        },
        Some(&route_override),
    );
    assert!(matches!(
        route_override_target,
        RoutingTarget::Primary {
            reason: RoutingReason::PolicyRouteOverride,
        }
    ));

    let shard_override = PolicyAction::shard_override(
        PolicyShardTargetId::new("tenant-a").expect("valid shard target"),
    );
    let shard_override_target = apply_policy_action_to_sharded_routing_target(
        &sample_shard_planner(),
        sample_shard_context(),
        RoutingTarget::Primary {
            reason: RoutingReason::ReadOnlyQuery,
        },
        Some(&shard_override),
    );
    assert!(matches!(
        shard_override_target,
        RoutingTarget::Primary {
            reason: RoutingReason::PolicyShardOverride,
        }
    ));
}

#[test]
fn plugin_host_limits_enforce_bytes_duration_and_private_access() {
    let runtime = pg_kinetic::proxy_runtime::policy::PolicyRuntime::new(
        Duration::from_millis(5),
        16,
    );
    let limits = runtime.plugin_host_limits();

    assert_eq!(limits.max_input_bytes(), 16);
    assert_eq!(limits.max_output_bytes(), 16);
    assert_eq!(limits.max_evaluation_duration(), Duration::from_millis(5));
    assert!(!limits.filesystem_access_allowed());
    assert!(!limits.network_access_allowed());
    assert!(!limits.secret_access_allowed());

    let oversized_input = PolicyPluginInput::new(
        1,
        PolicyId::new("plugin-policy").expect("policy id"),
        PolicyVersion::new(1).expect("policy version"),
        PolicyHookPoint::BeforeRouting,
        PolicyContext::new(vec![PolicyContextField::public("database", "appdb")]),
        false,
        false,
        false,
    )
    .expect("plugin input");
    let input_error = limits
        .validate_input(&oversized_input)
        .expect_err("oversized input is rejected");
    assert_eq!(input_error.code().as_str(), "input_too_large");
    assert_eq!(input_error.outcome(), PolicyOutcome::Skipped);

    let private_limits = PolicyPluginHostLimits::new(
        256,
        256,
        Duration::from_millis(5),
    );
    let private_input = PolicyPluginInput::new(
        1,
        PolicyId::new("plugin-policy").expect("policy id"),
        PolicyVersion::new(1).expect("policy version"),
        PolicyHookPoint::BeforeRouting,
        PolicyContext::default(),
        true,
        true,
        true,
    )
    .expect("plugin input");
    let private_error = private_limits
        .validate_input(&private_input)
        .expect_err("private access is rejected");
    assert_eq!(private_error.code().as_str(), "filesystem_access_denied");
    assert_eq!(private_error.outcome(), PolicyOutcome::Skipped);

    let oversized_output = PolicyPluginOutput::new(
        1,
        PolicyId::new("plugin-policy").expect("policy id"),
        PolicyVersion::new(1).expect("policy version"),
        PolicyHookPoint::BeforeRouting,
        PolicyPluginAction::allow(),
        PolicyOutcome::Applied,
    )
    .expect("plugin output");
    let output_error = limits
        .validate_output(&oversized_output)
        .expect_err("oversized output is rejected");
    assert_eq!(output_error.code().as_str(), "output_too_large");
    assert_eq!(output_error.outcome(), PolicyOutcome::Skipped);

    let timeout_error = limits
        .validate_evaluation_duration(Duration::from_millis(50))
        .expect_err("slow evaluation is rejected");
    assert_eq!(timeout_error.code().as_str(), "evaluation_timeout");
    assert_eq!(timeout_error.outcome(), PolicyOutcome::Skipped);
}

#[test]
fn plugin_output_is_validated_like_declarative_policy_output() {
    let limits = PolicyPluginHostLimits::new(
        8_192,
        8_192,
        Duration::from_millis(5),
    );
    let route_override = PolicyPluginOutput::new(
        1,
        PolicyId::new("route-fallback").expect("policy id"),
        PolicyVersion::new(1).expect("policy version"),
        PolicyHookPoint::AfterRouting,
        PolicyPluginAction::route_override(
            PolicyRouteTargetId::new("route-1").expect("valid route target"),
        ),
        PolicyOutcome::Applied,
    )
    .expect("plugin output");
    let route_error = limits
        .validate_output_like_declarative_policy_output(
            &route_override,
            ["route-0"],
            false,
            std::iter::empty::<&str>(),
        )
        .expect_err("missing route is rejected");
    assert_eq!(route_error.code().as_str(), "output_validation_failed");
    assert_eq!(route_error.outcome(), PolicyOutcome::Rejected);

    let unsupported = PolicyPluginOutput::new(
        1,
        PolicyId::new("unsupported-action").expect("policy id"),
        PolicyVersion::new(1).expect("policy version"),
        PolicyHookPoint::BeforeRouting,
        PolicyPluginAction::unsupported("mirror"),
        PolicyOutcome::Applied,
    )
    .expect("plugin output");
    let unsupported_error = limits
        .validate_output_like_declarative_policy_output(
            &unsupported,
            ["route-0"],
            false,
            std::iter::empty::<&str>(),
        )
        .expect_err("unsupported actions are rejected");
    assert_eq!(unsupported_error.code().as_str(), "unsupported_action");
    assert_eq!(unsupported_error.outcome(), PolicyOutcome::Rejected);
}

#[test]
fn policy_decision_reason_appears_in_snapshots_and_debug_traces() {
    let planner = sample_planner();
    let context = sample_routing_context();
    let deny = PolicyAction::deny();
    let denied_target = apply_policy_before_routing_target(&planner, context, Some(&deny));
    let route_key = sample_route_key();

    let snapshot = route_checkout_snapshot_for_target(
        route_key.clone(),
        denied_target.clone(),
        ReadAfterWriteState::Disabled,
    );
    let debug_fields = checkout_debug_fields(&denied_target, "allow_connect");

    assert_eq!(snapshot.decision.reason(), RoutingReason::PolicyDenied);
    assert_eq!(snapshot.freshness_outcome, Some(FreshnessStatus::Unavailable));
    assert!(debug_fields.iter().any(|(name, value)| {
        name == "reason" && value == RoutingReason::PolicyDenied.as_str()
    }));
    assert!(debug_fields.iter().any(|(name, value)| {
        name == "target_role" && value == "unknown"
    }));
}

#[derive(serde::Deserialize)]
struct PolicyDocument {
    policy: PolicyConfig,
}

fn sample_planner() -> ReadRoutingPlanner {
    ReadRoutingPlanner::new(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        1_000,
    )
}

fn sample_health() -> RouteHealthSnapshot {
    RouteHealthSnapshot::new(vec![healthy_replica_candidate(
        1,
        Some(sample_lsn()),
        Some(10),
    )])
}

fn sample_routing_context<'a>() -> RoutingContext<'a> {
    let health = sample_health();
    RoutingContext::new(
        "SELECT 1",
        TransactionState::Idle,
        ReadAfterWriteState::Required(sample_lsn()),
        Box::leak(Box::new(health)),
    )
}

fn sample_shard_planner() -> ShardRoutingPlanner {
    ShardRoutingPlanner::new(
        sample_planner(),
        true,
        ShardRouteMapStore::new(Vec::new()),
    )
}

fn sample_shard_context<'a>() -> ShardRoutingContext<'a> {
    let health = sample_health();
    ShardRoutingContext::new(
        "SELECT 1",
        TransactionState::Idle,
        ReadAfterWriteState::Required(sample_lsn()),
        Box::leak(Box::new(health)),
        None,
    )
}

fn healthy_replica_candidate(
    replica_id: u64,
    replay_lsn: Option<PgLsn>,
    lag_ms: Option<u64>,
) -> ReplicaCandidate {
    ReplicaCandidate {
        replica_id,
        healthy: true,
        split_brain: false,
        replay_lsn,
        lag_ms,
    }
}

fn sample_lsn() -> PgLsn {
    "0/16B6C50".parse().expect("lsn")
}

fn sample_route_key() -> RouteKey {
    RouteKey::new(
        "appdb",
        "reporter",
        Some("psql"),
        None,
        QueryClass::Read,
    )
}
