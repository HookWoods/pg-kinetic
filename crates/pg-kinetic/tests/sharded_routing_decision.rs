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
    pool::{
        BackendPoolRef, CheckoutMode, ReplicaSelectionStrategy, ReplicaSelector,
        ShardPoolCheckoutTarget, ShardPoolKey, ShardPools, ShardedPoolRegistry,
    },
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

fn shard_id(label: &str) -> ShardId {
    ShardId::new(label).expect("valid shard id")
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

fn test_pool(addr: std::net::SocketAddr) -> std::sync::Arc<pg_kinetic_proxy::pool::BackendPool> {
    pg_kinetic_proxy::pool::BackendPool::new(
        addr,
        pg_kinetic_proxy::config::TlsConfig::default(),
        1,
        1,
        1,
        1,
        std::time::Duration::from_millis(200),
        "DISCARD ALL",
    )
}

fn backend_ref_primary(addr: std::net::SocketAddr) -> BackendPoolRef {
    BackendPoolRef::primary(test_pool(addr))
}

fn backend_ref_replica(id: u64, weight: usize, addr: std::net::SocketAddr) -> BackendPoolRef {
    BackendPoolRef::replica(id, weight, test_pool(addr))
}

async fn backend_listener() -> (
    std::net::SocketAddr,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let addr = listener.local_addr().expect("listener addr");
    let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let accepted_probe = accepted.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept backend");
            accepted_probe.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            drop(stream);
        }
    });

    (addr, accepted)
}

async fn wait_for_accepts(accepted: &std::sync::atomic::AtomicUsize, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while accepted.load(std::sync::atomic::Ordering::Relaxed) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("backend accept observed");
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

#[tokio::test]
async fn sharded_pool_registry_uses_independent_primary_pools_per_shard() {
    let route = route_key();
    let shard_a = shard_id("tenant-a");
    let shard_b = shard_id("tenant-b");
    let (primary_a_addr, primary_a_accepts) = backend_listener().await;
    let (primary_b_addr, primary_b_accepts) = backend_listener().await;

    let registry = ShardedPoolRegistry::new();
    registry.insert(
        ShardPoolKey::new(route.clone(), shard_a.clone()),
        ShardPools::new(
            shard_a.clone(),
            backend_ref_primary(primary_a_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );
    registry.insert(
        ShardPoolKey::new(route.clone(), shard_b.clone()),
        ShardPools::new(
            shard_b.clone(),
            backend_ref_primary(primary_b_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );

    let checkout_a = registry
        .checkout_primary(&route, &shard_a, CheckoutMode::AllowConnect)
        .await
        .expect("shard-a primary checkout");
    let checkout_b = registry
        .checkout_primary(&route, &shard_b, CheckoutMode::AllowConnect)
        .await
        .expect("shard-b primary checkout");

    assert!(checkout_a.requires_startup());
    assert!(checkout_b.requires_startup());
    wait_for_accepts(&primary_a_accepts, 1).await;
    wait_for_accepts(&primary_b_accepts, 1).await;
    assert_eq!(
        primary_a_accepts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        primary_b_accepts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn sharded_pool_registry_uses_independent_replica_pools_per_shard() {
    let route = route_key();
    let shard_a = shard_id("tenant-a");
    let shard_b = shard_id("tenant-b");
    let (primary_a_addr, primary_a_accepts) = backend_listener().await;
    let (replica_a_addr, replica_a_accepts) = backend_listener().await;
    let (primary_b_addr, primary_b_accepts) = backend_listener().await;
    let (replica_b_addr, replica_b_accepts) = backend_listener().await;

    let registry = ShardedPoolRegistry::new();
    registry.insert(
        ShardPoolKey::new(route.clone(), shard_a.clone()),
        ShardPools::new(
            shard_a.clone(),
            backend_ref_primary(primary_a_addr),
            vec![backend_ref_replica(1, 1, replica_a_addr)],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );
    registry.insert(
        ShardPoolKey::new(route.clone(), shard_b.clone()),
        ShardPools::new(
            shard_b.clone(),
            backend_ref_primary(primary_b_addr),
            vec![backend_ref_replica(2, 1, replica_b_addr)],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );

    let checkout_a = registry
        .checkout_any_replica(&route, &shard_a, CheckoutMode::AllowConnect)
        .await
        .expect("shard-a replica checkout");
    let checkout_b = registry
        .checkout_any_replica(&route, &shard_b, CheckoutMode::AllowConnect)
        .await
        .expect("shard-b replica checkout");

    assert!(checkout_a.requires_startup());
    assert!(checkout_b.requires_startup());
    wait_for_accepts(&replica_a_accepts, 1).await;
    wait_for_accepts(&replica_b_accepts, 1).await;
    assert_eq!(
        primary_a_accepts.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        primary_b_accepts.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        replica_a_accepts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        replica_b_accepts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn sharded_pool_registry_requires_explicit_shard_id_for_checkout() {
    let route = route_key();
    let shard_a = shard_id("tenant-a");
    let shard_b = shard_id("tenant-b");
    let (primary_a_addr, primary_a_accepts) = backend_listener().await;
    let (primary_b_addr, primary_b_accepts) = backend_listener().await;

    let registry = ShardedPoolRegistry::new();
    registry.insert(
        ShardPoolKey::new(route.clone(), shard_a.clone()),
        ShardPools::new(
            shard_a.clone(),
            backend_ref_primary(primary_a_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );
    registry.insert(
        ShardPoolKey::new(route.clone(), shard_b.clone()),
        ShardPools::new(
            shard_b.clone(),
            backend_ref_primary(primary_b_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );

    let target = ShardPoolCheckoutTarget::new(
        route.clone(),
        shard_b.clone(),
        BackendRole::Primary,
        Some(RoutingReason::FallbackPrimary),
    );
    let checkout = registry
        .checkout_target(&target, CheckoutMode::AllowConnect)
        .await
        .expect("explicit shard checkout");

    assert_eq!(target.shard_id().as_str(), "tenant-b");
    assert_eq!(target.target_role(), BackendRole::Primary);
    assert_eq!(
        target.fallback_reason(),
        Some(RoutingReason::FallbackPrimary)
    );
    assert!(checkout.requires_startup());
    wait_for_accepts(&primary_b_accepts, 1).await;
    assert_eq!(
        primary_a_accepts.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        primary_b_accepts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn unknown_shard_id_is_rejected_before_backend_checkout() {
    let route = route_key();
    let shard_a = shard_id("tenant-a");
    let unknown_shard = shard_id("tenant-z");
    let (primary_a_addr, primary_a_accepts) = backend_listener().await;

    let registry = ShardedPoolRegistry::new();
    registry.insert(
        ShardPoolKey::new(route.clone(), shard_a.clone()),
        ShardPools::new(
            shard_a,
            backend_ref_primary(primary_a_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );

    let target = ShardPoolCheckoutTarget::new(route, unknown_shard, BackendRole::Primary, None);
    let result = registry
        .checkout_target(&target, CheckoutMode::AllowConnect)
        .await;

    assert!(matches!(
        result,
        Err(pg_kinetic_proxy::pool::PoolError::Backpressure(
            pg_kinetic_core::backpressure::BackpressureError::Closed
        ))
    ));
    assert_eq!(
        primary_a_accepts.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn shard_health_integrates_with_replica_health() {
    let route = route_key();
    let shard = shard_id("tenant-a");
    let (primary_addr, primary_accepts) = backend_listener().await;
    let (healthy_replica_addr, healthy_replica_accepts) = backend_listener().await;
    let (unhealthy_replica_addr, unhealthy_replica_accepts) = backend_listener().await;

    let healthy_replica = backend_ref_replica(1, 1, healthy_replica_addr);
    let unhealthy_replica = backend_ref_replica(2, 1, unhealthy_replica_addr);
    unhealthy_replica.set_healthy(false);

    let pools = ShardPools::new(
        shard.clone(),
        backend_ref_primary(primary_addr),
        vec![unhealthy_replica.clone(), healthy_replica.clone()],
        ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
    );

    let checkout = pools
        .checkout_target(
            &ShardPoolCheckoutTarget::new(
                route,
                shard,
                BackendRole::Replica,
                Some(RoutingReason::ReadCandidateQuery),
            ),
            CheckoutMode::AllowConnect,
        )
        .await
        .expect("healthy shard replica checkout");

    assert!(checkout.requires_startup());
    wait_for_accepts(&healthy_replica_accepts, 1).await;
    assert_eq!(
        primary_accepts.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        unhealthy_replica_accepts.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        healthy_replica_accepts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn shard_pool_metrics_use_low_cardinality_labels() {
    let shard = shard_id("tenant-a");
    let key_a = ShardPoolKey::new(route_key(), shard.clone());
    let key_b = ShardPoolKey::new(
        RouteKey::new("other", "postgres", Some("api"), None, QueryClass::Default),
        shard.clone(),
    );

    assert_eq!(key_a.metric_label(), "tenant-a");
    assert_eq!(key_b.metric_label(), "tenant-a");
}
