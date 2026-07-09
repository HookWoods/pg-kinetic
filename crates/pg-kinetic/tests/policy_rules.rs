use std::{sync::Arc, time::Duration};

use pg_kinetic::core::{
    lsn::FreshnessStatus,
    policy::{
        PolicyDecisionReason, PolicyHookPoint, PolicyRouteTargetId, PolicyShardTargetId,
    },
    policy_rule::{
        PolicyRule, PolicyRuleAction, PolicyRuleContext, PolicyRuleMatch, PolicyRuleSet,
    },
    route::{QueryClass as RouteQueryClass, RouteKey},
    routing::{BackendRole, FallbackPolicy, FreshnessPolicy, QueryClass, RoutingDecision, RoutingHint, RoutingReason},
    session::TransactionAccessMode,
    sharding::{MultiShardPolicy, ShardId, ShardRoute, ShardRouteDecision, ShardRouteReason, ShardTarget},
};
use pg_kinetic::proxy_runtime::policy::{PolicyEvalInput, PolicyRuntime};

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

#[test]
fn policy_context_includes_routing_dimensions_and_safe_labels() {
    let runtime = PolicyRuntime::new(Duration::from_millis(5), 8_192);
    let output = runtime.context_builder().build(&sample_policy_eval_input());

    let field_names = output
        .context
        .fields()
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>();

    assert!(field_names.contains(&"database"));
    assert!(field_names.contains(&"user"));
    assert!(field_names.contains(&"application_name"));
    assert!(field_names.contains(&"route"));
    assert!(field_names.contains(&"shard"));
    assert!(field_names.contains(&"backend_role"));
    assert!(field_names.contains(&"query_class"));
    assert!(field_names.contains(&"transaction_mode"));
    assert!(field_names.contains(&"freshness_state"));
    assert!(field_names.contains(&"route_key"));
    assert!(field_names.contains(&"routing_reason"));
    assert!(field_names.contains(&"shard_route_reason"));
}

#[test]
fn policy_context_excludes_sensitive_material() {
    let runtime = PolicyRuntime::new(Duration::from_millis(5), 8_192);
    let output = runtime.context_builder().build(&sample_policy_eval_input());

    assert!(!output.rendered_context.contains("swordfish"));
    assert!(!output.rendered_context.contains("alpha=1"));
    assert!(!output.rendered_context.contains("BEGIN SELECT 1"));
    assert!(!output.rendered_context.contains("-----BEGIN CERTIFICATE-----"));
    assert!(output.rendered_context.contains("sensitive_inputs=<redacted>"));
}

#[test]
fn policy_context_size_is_bounded() {
    let runtime = PolicyRuntime::new(Duration::from_millis(5), 160);
    let output = runtime.context_builder().build(&large_policy_eval_input());

    assert!(output.context.rendered_len_bytes() <= 160);
    assert!(output.rendered_context_bytes <= 160);
    assert!(output.truncated);
}

#[test]
fn redacted_policy_context_rendering_is_stable() {
    let runtime = PolicyRuntime::new(Duration::from_millis(5), 8_192);
    let input = sample_policy_eval_input();

    let first = runtime.context_builder().build(&input);
    let second = runtime.context_builder().build(&input);

    assert_eq!(first.context, second.context);
    assert_eq!(first.rendered_context, second.rendered_context);
    assert_eq!(format!("{}", first.context), first.rendered_context);
}

fn sample_policy_eval_input() -> PolicyEvalInput {
    let route_key = RouteKey::new(
        "appdb",
        "reporter",
        Some("psql"),
        None,
        RouteQueryClass::Read,
    );
    let routing_decision = RoutingDecision::new(
        BackendRole::Replica,
        QueryClass::ReadOnly,
        RoutingHint::StrictFresh,
        RoutingReason::ReadOnlyQuery,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
    );
    let shard_route = ShardRoute::new(
        ShardTarget::new(
            route_key.clone(),
            BackendRole::Replica,
            ShardId::new("tenant-a").expect("valid shard id"),
        ),
        ShardRouteReason::HashMatch,
    );
    let shard_route_decision = ShardRouteDecision::new(
        Some(shard_route),
        ShardRouteReason::HashMatch,
        MultiShardPolicy::FirstMatch,
    );

    PolicyEvalInput {
        database: Arc::from("appdb"),
        user: Arc::from("reporter"),
        application_name: Some(Arc::from("psql")),
        route: Some(Arc::from("read-route")),
        shard: Some(Arc::from("tenant-a")),
        backend_role: BackendRole::Replica,
        query_class: QueryClass::ReadOnly,
        transaction_mode: TransactionAccessMode::ReadOnly,
        freshness_state: FreshnessStatus::Waiting,
        routing_decision: Some(routing_decision),
        shard_route_decision: Some(shard_route_decision),
        password: Some(Arc::from("swordfish")),
        bind_values: vec![Arc::from("alpha=1"), Arc::from("beta=2")],
        tls_certificate_body: Some(Arc::from("-----BEGIN CERTIFICATE-----")),
        raw_sql_text: Some(Arc::from("BEGIN SELECT 1")),
        secrets: vec![Arc::from("super-secret-token")],
    }
}

fn large_policy_eval_input() -> PolicyEvalInput {
    let mut input = sample_policy_eval_input();
    input.database = Arc::from("database-with-an-intentionally-long-name-to-exercise-truncation");
    input.user = Arc::from("reporting-user-with-an-intentionally-long-name-to-exercise-truncation");
    input.application_name = Some(Arc::from("application-name-with-an-intentionally-long-name-to-exercise-truncation"));
    input.route = Some(Arc::from("route-name-with-an-intentionally-long-name-to-exercise-truncation"));
    input.shard = Some(Arc::from("shard-name-with-an-intentionally-long-name-to-exercise-truncation"));
    input
}
