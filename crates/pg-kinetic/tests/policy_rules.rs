use std::sync::Arc;

use pg_kinetic::core::{
    policy::{
        PolicyDecisionReason, PolicyHookPoint, PolicyRouteTargetId, PolicyShardTargetId,
    },
    policy_rule::{
        PolicyRule, PolicyRuleAction, PolicyRuleContext, PolicyRuleMatch, PolicyRuleSet,
    },
    routing::{BackendRole, QueryClass},
};

#[test]
fn rule_matches_supported_policy_fields() {
    let rule = PolicyRule::new(
        "match-all",
        PolicyRuleMatch::new()
            .database("appdb")
            .user("reporter")
            .application_name("psql")
            .route("route-a")
            .shard("shard-a")
            .backend_role(BackendRole::Replica)
            .query_class(QueryClass::ReadOnly)
            .hook_point(PolicyHookPoint::BeforeRouting)
            .read_only_transaction(true)
            .has_shard_key(true)
            .policy_tag("tenant-a"),
        PolicyRuleAction::allow(),
    );
    let context = PolicyRuleContext {
        database: Arc::from("appdb"),
        user: Arc::from("reporter"),
        application_name: Some(Arc::from("psql")),
        route: Some(Arc::from("route-a")),
        shard: Some(Arc::from("shard-a")),
        backend_role: BackendRole::Replica,
        query_class: QueryClass::ReadOnly,
        hook_point: PolicyHookPoint::BeforeRouting,
        read_only_transaction: true,
        has_shard_key: true,
        policy_tags: vec![Arc::from("tenant-a"), Arc::from("team-reporting")],
    };

    let result = PolicyRuleSet::new(vec![rule]).evaluate(&context);

    assert_eq!(result.matched_rule_ids, vec![Arc::from("match-all")]);
    assert_eq!(result.audit_rule_ids, Vec::<Arc<str>>::new());
    assert_eq!(result.terminal_rule_id, Some(Arc::from("match-all")));
    assert!(matches!(
        result.applied_action,
        Some(PolicyRuleAction::Allow {
            audit: false,
            terminal: true
        })
    ));
}

#[test]
fn unsafe_query_classes_can_be_denied() {
    let rule = PolicyRule::new(
        "deny-write",
        PolicyRuleMatch::new()
            .query_class(QueryClass::Write)
            .hook_point(PolicyHookPoint::BeforeRouting),
        PolicyRuleAction::deny().with_audit(true),
    );
    let context = PolicyRuleContext {
        database: Arc::from("appdb"),
        user: Arc::from("reporter"),
        application_name: None,
        route: None,
        shard: None,
        backend_role: BackendRole::Primary,
        query_class: QueryClass::Write,
        hook_point: PolicyHookPoint::BeforeRouting,
        read_only_transaction: false,
        has_shard_key: false,
        policy_tags: Vec::new(),
    };

    let result = PolicyRuleSet::new(vec![rule]).evaluate(&context);

    assert!(matches!(
        result.applied_action,
        Some(PolicyRuleAction::Deny {
            reason: PolicyDecisionReason::PolicyDenied,
            audit: true,
            terminal: true
        })
    ));
    assert_eq!(result.terminal_rule_id, Some(Arc::from("deny-write")));
}

#[test]
fn require_primary_can_target_selected_users_and_routes() {
    let rule = PolicyRule::new(
        "require-primary",
        PolicyRuleMatch::new()
            .user("admin")
            .route("admin-route")
            .hook_point(PolicyHookPoint::AfterRouting),
        PolicyRuleAction::require_primary(),
    );
    let context = PolicyRuleContext {
        database: Arc::from("appdb"),
        user: Arc::from("admin"),
        application_name: Some(Arc::from("psql")),
        route: Some(Arc::from("admin-route")),
        shard: None,
        backend_role: BackendRole::Replica,
        query_class: QueryClass::ReadOnly,
        hook_point: PolicyHookPoint::AfterRouting,
        read_only_transaction: true,
        has_shard_key: false,
        policy_tags: Vec::new(),
    };

    let result = PolicyRuleSet::new(vec![rule]).evaluate(&context);

    assert!(matches!(
        result.applied_action,
        Some(PolicyRuleAction::RequirePrimary {
            audit: false,
            terminal: true
        })
    ));
}

#[test]
fn require_replica_can_target_read_only_routes() {
    let rule = PolicyRule::new(
        "require-replica",
        PolicyRuleMatch::new()
            .route("read-route")
            .query_class(QueryClass::ReadOnly)
            .read_only_transaction(true)
            .hook_point(PolicyHookPoint::BeforeRouting),
        PolicyRuleAction::require_replica(),
    );
    let context = PolicyRuleContext {
        database: Arc::from("appdb"),
        user: Arc::from("reader"),
        application_name: Some(Arc::from("reporting")),
        route: Some(Arc::from("read-route")),
        shard: None,
        backend_role: BackendRole::Primary,
        query_class: QueryClass::ReadOnly,
        hook_point: PolicyHookPoint::BeforeRouting,
        read_only_transaction: true,
        has_shard_key: false,
        policy_tags: Vec::new(),
    };

    let result = PolicyRuleSet::new(vec![rule]).evaluate(&context);

    assert!(matches!(
        result.applied_action,
        Some(PolicyRuleAction::RequireReplica {
            audit: false,
            terminal: true
        })
    ));
}

#[test]
fn route_and_shard_overrides_only_accept_validated_targets() {
    assert!(PolicyRouteTargetId::new("").is_err());
    assert!(PolicyShardTargetId::new("").is_err());

    let route_target = PolicyRouteTargetId::new("route-b").expect("valid route target");
    let shard_target = PolicyShardTargetId::new("shard-b").expect("valid shard target");

    let route_rule = PolicyRule::new(
        "route-override",
        PolicyRuleMatch::new().route("route-a"),
        PolicyRuleAction::route_override(route_target.clone()),
    );
    let shard_rule = PolicyRule::new(
        "shard-override",
        PolicyRuleMatch::new().shard("shard-a"),
        PolicyRuleAction::shard_override(shard_target.clone()),
    );

    assert!(matches!(
        route_rule.action,
        PolicyRuleAction::RouteOverride {
            target_id,
            audit: false,
            terminal: true
        } if target_id == route_target
    ));
    assert!(matches!(
        shard_rule.action,
        PolicyRuleAction::ShardOverride {
            target_id,
            audit: false,
            terminal: true
        } if target_id == shard_target
    ));
}

#[test]
fn first_terminal_rule_stops_evaluation() {
    let rule_set = PolicyRuleSet::new(vec![
        PolicyRule::new(
            "audit-allow",
            PolicyRuleMatch::new().database("appdb"),
            PolicyRuleAction::allow()
                .with_audit(true)
                .with_terminal(false),
        ),
        PolicyRule::new(
            "deny-fallthrough",
            PolicyRuleMatch::new().database("appdb"),
            PolicyRuleAction::deny(),
        ),
    ]);
    let context = PolicyRuleContext {
        database: Arc::from("appdb"),
        user: Arc::from("reporter"),
        application_name: None,
        route: None,
        shard: None,
        backend_role: BackendRole::Primary,
        query_class: QueryClass::ReadOnly,
        hook_point: PolicyHookPoint::BeforeRouting,
        read_only_transaction: true,
        has_shard_key: false,
        policy_tags: Vec::new(),
    };

    let result = rule_set.evaluate(&context);

    assert_eq!(
        result.matched_rule_ids,
        vec![Arc::from("audit-allow"), Arc::from("deny-fallthrough")]
    );
    assert_eq!(result.audit_rule_ids, vec![Arc::from("audit-allow")]);
    assert_eq!(result.terminal_rule_id, Some(Arc::from("deny-fallthrough")));
    assert_eq!(result.applied_rule_id, Some(Arc::from("deny-fallthrough")));
}

#[test]
fn non_terminal_allow_rules_record_audit_and_continue() {
    let rule_set = PolicyRuleSet::new(vec![
        PolicyRule::new(
            "allow-audit",
            PolicyRuleMatch::new().policy_tag("tenant-a"),
            PolicyRuleAction::allow()
                .with_audit(true)
                .with_terminal(false),
        ),
        PolicyRule::new(
            "require-primary",
            PolicyRuleMatch::new().policy_tag("tenant-a"),
            PolicyRuleAction::require_primary(),
        ),
    ]);
    let context = PolicyRuleContext {
        database: Arc::from("appdb"),
        user: Arc::from("reporter"),
        application_name: None,
        route: None,
        shard: None,
        backend_role: BackendRole::Replica,
        query_class: QueryClass::ReadOnly,
        hook_point: PolicyHookPoint::BeforeRouting,
        read_only_transaction: true,
        has_shard_key: false,
        policy_tags: vec![Arc::from("tenant-a")],
    };

    let result = rule_set.evaluate(&context);

    assert_eq!(result.audit_rule_ids, vec![Arc::from("allow-audit")]);
    assert_eq!(result.terminal_rule_id, Some(Arc::from("require-primary")));
    assert!(matches!(
        result.applied_action,
        Some(PolicyRuleAction::RequirePrimary {
            audit: false,
            terminal: true
        })
    ));
}

#[test]
fn invalid_match_fields_are_rejected_during_validation() {
    let rule = PolicyRule::new(
        "invalid-match",
        PolicyRuleMatch::new().database("   "),
        PolicyRuleAction::allow(),
    );
    let error = PolicyRuleSet::new(vec![rule])
        .validate()
        .expect_err("blank match values are rejected");

    assert_eq!(
        error.to_string(),
        "policy rule 'invalid-match' match field 'database' cannot be empty"
    );
}

#[test]
fn policy_rules_cannot_match_raw_sql_text_by_default() {
    let rule = PolicyRule::new(
        "raw-sql",
        PolicyRuleMatch::new().raw_sql_text("select * from accounts"),
        PolicyRuleAction::allow(),
    );
    let error = PolicyRuleSet::new(vec![rule])
        .validate()
        .expect_err("raw sql text matching is rejected");

    assert_eq!(
        error.to_string(),
        "raw sql text matches are not supported by policy rules"
    );
}
