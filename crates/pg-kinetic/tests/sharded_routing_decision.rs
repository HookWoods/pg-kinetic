use pg_kinetic_core::{
    route::{QueryClass, RouteKey},
    routing::{BackendRole, FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
    session::TransactionState,
    sharding::{
        MultiShardPolicy, RouteDefinition, RouteMapValidationInput, ShardId, ShardRoute,
        ShardRouteDecision, ShardRouteMap, ShardRouteReason, ShardScope, ShardStrategy,
        ShardTarget, ShardedTableDefinition,
    },
    virtual_session::ReadAfterWriteState,
};
use pg_kinetic_proxy::{
    routing::{
        bridge_shard_route_decision, choose_routing_target, ReadRoutingPlanner, ReplicaCandidate,
        RouteHealthSnapshot, RoutingContext, RoutingReason, RoutingTarget,
    },
    sharding::{
        apply_multi_shard_policy, choose_sharded_routing_target, plan_sharded_route,
        ShardRouteMapStore, ShardRoutingContext, ShardRoutingPlanner,
    },
};

fn read_planner(
    read_routing_mode: ReadRoutingMode,
    fallback_policy: FallbackPolicy,
    freshness_policy: FreshnessPolicy,
) -> ReadRoutingPlanner {
    ReadRoutingPlanner::new(read_routing_mode, fallback_policy, freshness_policy, 1_000)
}

fn route_key() -> RouteKey {
    RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        None,
        QueryClass::Default,
    )
}

fn route_target(shard_id: &str, backend_role: BackendRole) -> ShardTarget {
    ShardTarget::new(
        route_key(),
        backend_role,
        ShardId::new(shard_id).expect("valid shard id"),
    )
}

fn route(shard_id: &str, backend_role: BackendRole) -> ShardRoute {
    ShardRoute::new(
        route_target(shard_id, backend_role),
        ShardRouteReason::HashMatch,
    )
}

fn route_map(shard_id: &str, backend_role: BackendRole) -> ShardRouteMap {
    ShardRouteMap::new(
        ShardScope::global(),
        ShardStrategy::Hash,
        MultiShardPolicy::FirstMatch,
        vec![route(shard_id, backend_role)],
    )
    .expect("valid route map")
}

fn health(primary_healthy: bool, replica_healthy: bool) -> RouteHealthSnapshot {
    RouteHealthSnapshot::new(vec![
        ReplicaCandidate::new(
            1,
            primary_healthy,
            None,
            Some(if primary_healthy { 5 } else { 500 }),
        ),
        ReplicaCandidate::new(
            2,
            replica_healthy,
            None,
            Some(if replica_healthy { 5 } else { 500 }),
        ),
    ])
}

fn validation_input() -> RouteMapValidationInput {
    RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: None,
            is_default: true,
        }],
        sharded_tables: vec![ShardedTableDefinition {
            name: String::from("public.accounts"),
            enabled: true,
            shard_key_column: Some(String::from("tenant_id")),
        }],
        shard_rules: vec![],
    }
}

fn context<'a>(
    sql: &'a str,
    health: &'a RouteHealthSnapshot,
    route_map_validation_input: Option<&'a RouteMapValidationInput>,
) -> ShardRoutingContext<'a> {
    ShardRoutingContext::new(
        sql,
        TransactionState::Idle,
        ReadAfterWriteState::Disabled,
        health,
        route_map_validation_input,
    )
}

#[test]
fn sharding_disabled_preserves_read_routing_decisions() {
    let planner = ShardRoutingPlanner::new(
        read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Primary,
            FreshnessPolicy::None,
        ),
        false,
        ShardRouteMapStore::new(vec![route_map("tenant-a", BackendRole::Primary)]),
    );
    let health = health(true, true);
    let context = context("select 1", &health, None);

    let expected = choose_routing_target(
        &read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Primary,
            FreshnessPolicy::None,
        ),
        RoutingContext::new(
            "select 1",
            TransactionState::Idle,
            ReadAfterWriteState::Disabled,
            &health,
        ),
    );
    let actual = choose_sharded_routing_target(&planner, context);

    assert_eq!(actual, expected);
}

#[test]
fn explicit_shard_hint_routes_to_the_target_shard_when_valid() {
    let planner = ShardRoutingPlanner::new(
        read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Primary,
            FreshnessPolicy::None,
        ),
        true,
        ShardRouteMapStore::new(vec![
            route_map("tenant-a", BackendRole::Primary),
            route_map("tenant-b", BackendRole::Replica),
        ]),
    );
    let health = health(true, true);
    let context = context("/* pg-kinetic: shard=tenant-b */ select 1", &health, None);

    let decision = plan_sharded_route(&planner, context);
    let route = decision.route().expect("selected route");

    assert_eq!(route.target().shard_id().as_str(), "tenant-b");
    assert_eq!(decision.reason(), ShardRouteReason::AdminOverride);
}

#[test]
fn invalid_shard_hint_is_rejected() {
    let planner = ShardRoutingPlanner::new(
        read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Reject,
            FreshnessPolicy::None,
        ),
        true,
        ShardRouteMapStore::new(vec![route_map("tenant-a", BackendRole::Primary)]),
    );
    let health = health(true, true);
    let context = context("/* pg-kinetic: shard=tenant-b */ select 1", &health, None);

    let target = choose_sharded_routing_target(&planner, context);

    assert!(matches!(target, RoutingTarget::Reject { .. }));
    assert_eq!(target.reason(), RoutingReason::FallbackReject);
}

#[test]
fn simple_shard_key_routes_to_one_shard() {
    let planner = ShardRoutingPlanner::new(
        read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Primary,
            FreshnessPolicy::None,
        ),
        true,
        ShardRouteMapStore::new(vec![route_map("tenant-a", BackendRole::Primary)]),
    );
    let health = health(true, true);
    let validation_input = validation_input();
    let context = context(
        "select * from public.accounts where tenant_id = 'tenant-a'",
        &health,
        Some(&validation_input),
    );

    let decision = plan_sharded_route(&planner, context);
    let route = decision.route().expect("selected route");

    assert_eq!(route.target().shard_id().as_str(), "tenant-a");
    assert_eq!(decision.reason(), ShardRouteReason::HashMatch);
}

#[test]
fn unknown_shard_key_follows_multi_shard_policy() {
    let planner = ShardRoutingPlanner::new(
        read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Reject,
            FreshnessPolicy::None,
        ),
        true,
        ShardRouteMapStore::new(vec![route_map("tenant-a", BackendRole::Primary)]),
    );
    let health = health(true, true);
    let validation_input = validation_input();
    let context = context("select 1", &health, Some(&validation_input));

    let target = choose_sharded_routing_target(&planner, context);

    assert!(matches!(target, RoutingTarget::Reject { .. }));
    assert_eq!(target.reason().as_str(), "fallback_reject");
}

#[test]
fn multi_shard_policy_primary_fallback_routes_to_primary() {
    let planner = ShardRoutingPlanner::new(
        read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Primary,
            FreshnessPolicy::None,
        ),
        true,
        ShardRouteMapStore::new(vec![route_map("tenant-a", BackendRole::Primary)]),
    );
    let health = health(true, true);
    let context = context("select 1", &health, None);

    let target = choose_sharded_routing_target(&planner, context);

    assert!(matches!(target, RoutingTarget::Primary { .. }));
    assert_eq!(target.reason().as_str(), "fallback_primary");
}

#[test]
fn read_routing_still_chooses_primary_or_replica_inside_selected_shard() {
    let planner = ShardRoutingPlanner::new(
        read_planner(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Primary,
            FreshnessPolicy::None,
        ),
        true,
        ShardRouteMapStore::new(vec![route_map("tenant-a", BackendRole::Primary)]),
    );
    let health = health(true, true);
    let context = context("/* pg-kinetic: shard=tenant-a */ select 1", &health, None);

    let target = choose_sharded_routing_target(&planner, context);

    assert!(matches!(target, RoutingTarget::Replica { .. }));
    assert_eq!(target.reason().as_str(), "read_candidate_query");
}

#[test]
fn fallback_reasons_are_stable() {
    let reject = apply_multi_shard_policy(FallbackPolicy::Reject);
    let primary = apply_multi_shard_policy(FallbackPolicy::Primary);

    assert_eq!(reject.reason().as_str(), "fallback_reject");
    assert_eq!(primary.reason().as_str(), "fallback_primary");
}

#[test]
fn shard_route_decision_bridges_to_read_routing_decision() {
    let planner = read_planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::None,
    );
    let health = health(true, true);
    let context = context("select 1", &health, None);
    let decision = ShardRouteDecision::new(
        Some(route("tenant-a", BackendRole::Primary)),
        ShardRouteReason::AdminOverride,
        MultiShardPolicy::FirstMatch,
    );

    let routing_decision = bridge_shard_route_decision(&decision, context.sql, &planner);

    assert_eq!(
        routing_decision.target_role,
        pg_kinetic_core::routing::BackendRole::Primary
    );
    assert_eq!(routing_decision.fallback_policy, FallbackPolicy::Primary);
}
