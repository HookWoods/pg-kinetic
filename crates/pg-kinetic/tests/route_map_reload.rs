use pg_kinetic::{
    core::{
        prepare::PreparedCatalog,
        routing::{FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
        session::TransactionState,
        sharding::{
            MultiShardPolicy, ShardId, ShardRoute, ShardRouteMap, ShardRouteReason, ShardScope,
            ShardStrategy, ShardTarget,
        },
        virtual_session::ReadAfterWriteState,
    },
    proxy_runtime::{
        routing::{ReadRoutingPlanner, RouteHealthSnapshot},
        sharding::{
            RouteMapReloadErrorCode, RouteMapReloadResult, ShardRouteMapStore, ShardRoutingContext,
            ShardRoutingPlanner,
        },
        snapshot::SnapshotStore,
    },
    route::{QueryClass, RouteKey},
};

fn route_key() -> RouteKey {
    RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        None,
        QueryClass::Default,
    )
}

fn shard_id(value: &str) -> ShardId {
    ShardId::new(value).expect("valid shard id")
}

fn route(shard_id_value: &str) -> ShardRoute {
    ShardRoute::new(
        ShardTarget::new(
            route_key(),
            pg_kinetic::core::routing::BackendRole::Primary,
            shard_id(shard_id_value),
        ),
        ShardRouteReason::HashMatch,
    )
}

fn route_map(shard_id: &str) -> ShardRouteMap {
    ShardRouteMap::new(
        ShardScope::global(),
        ShardStrategy::Hash,
        MultiShardPolicy::FirstMatch,
        vec![route(shard_id)],
    )
    .expect("valid route map")
}

fn planner(store: ShardRouteMapStore) -> ShardRoutingPlanner {
    ShardRoutingPlanner::new(
        ReadRoutingPlanner::new(
            ReadRoutingMode::PreferReplica,
            FallbackPolicy::Primary,
            FreshnessPolicy::None,
            1_000,
        ),
        true,
        store,
    )
}

fn context<'a>(sql: &'a str, health: &'a RouteHealthSnapshot) -> ShardRoutingContext<'a> {
    ShardRoutingContext::new(
        sql,
        TransactionState::Idle,
        ReadAfterWriteState::Disabled,
        health,
        None,
    )
}

fn health() -> RouteHealthSnapshot {
    RouteHealthSnapshot::new(Vec::new())
}

fn reload_result(store: &ShardRouteMapStore, result: &RouteMapReloadResult) {
    assert_eq!(store.generation_id(), result.route_map_generation_id);
}

#[test]
fn valid_route_map_reload_swaps_atomically() {
    let snapshot_store = SnapshotStore::new();
    let store = ShardRouteMapStore::new(vec![route_map("tenant-a")]);
    let planner = planner(store.clone());
    let health = health();

    let before = planner.plan_sharded_route(context(
        "/* pg-kinetic: shard=tenant-a */ select 1",
        &health,
    ));
    assert_eq!(
        before
            .route()
            .expect("selected route")
            .target()
            .shard_id()
            .as_str(),
        "tenant-a"
    );

    let result = store.reload(vec![route_map("tenant-b")], Some(&snapshot_store));
    assert!(result.success);
    reload_result(&store, &result);
    assert_eq!(
        store.route_maps()[0].routes()[0]
            .target()
            .shard_id()
            .as_str(),
        "tenant-b"
    );

    let after = planner.plan_sharded_route(context(
        "/* pg-kinetic: shard=tenant-b */ select 1",
        &health,
    ));
    assert_eq!(
        after
            .route()
            .expect("selected route")
            .target()
            .shard_id()
            .as_str(),
        "tenant-b"
    );

    let snapshots = snapshot_store.route_map_reload_snapshots();
    let snapshot = snapshots.last().expect("reload snapshot");
    assert!(snapshot.success);
    assert_eq!(snapshot.route_map_generation_id, 1);
    assert!(snapshot.error_code.is_none());
}

#[test]
fn invalid_route_map_reload_is_rejected_and_old_map_stays_active() {
    let snapshot_store = SnapshotStore::new();
    let store = ShardRouteMapStore::new(vec![route_map("tenant-a")]);

    let result = store.reload(
        vec![route_map("tenant-b"), route_map("tenant-c")],
        Some(&snapshot_store),
    );
    assert!(!result.success);
    assert_eq!(
        result.error_code,
        Some(RouteMapReloadErrorCode::ConflictingRouteScopes)
    );
    assert_eq!(result.route_map_generation_id, 0);
    assert_eq!(
        store.route_maps()[0].routes()[0]
            .target()
            .shard_id()
            .as_str(),
        "tenant-a"
    );

    let snapshots = snapshot_store.route_map_reload_snapshots();
    let snapshot = snapshots.last().expect("reload snapshot");
    assert!(!snapshot.success);
    assert_eq!(snapshot.route_map_generation_id, 0);
    assert_eq!(
        snapshot.error_code,
        Some(RouteMapReloadErrorCode::ConflictingRouteScopes)
    );
}

#[test]
fn route_map_generation_id_increments_on_successful_reload() {
    let store = ShardRouteMapStore::new(vec![route_map("tenant-a")]);

    let result = store.reload(vec![route_map("tenant-b")], None);

    assert!(result.success);
    assert_eq!(result.route_map_generation_id, 1);
    assert_eq!(store.generation_id(), 1);
}

#[test]
fn active_sessions_keep_their_current_transaction_shard_affinity() {
    let store = ShardRouteMapStore::new(vec![route_map("tenant-a")]);
    store.set_transaction_shard_affinity(41, shard_id("tenant-a"));

    let result = store.reload(vec![route_map("tenant-b")], None);

    assert!(result.success);
    assert_eq!(
        store
            .transaction_shard_affinity(41)
            .expect("transaction shard affinity")
            .as_str(),
        "tenant-a"
    );
}

#[test]
fn prepared_statements_tied_to_an_old_generation_are_revalidated_after_reload() {
    let store = ShardRouteMapStore::new(vec![route_map("tenant-a")]);
    let mut catalog = PreparedCatalog::new(7);
    catalog.upsert("stmt1", "select 1", vec![]);

    assert!(catalog.get_for_current_route_map("stmt1").is_some());

    let result = store.reload(vec![route_map("tenant-b")], None);
    assert!(result.success);
    catalog.set_route_map_generation_id(store.generation_id());

    assert!(catalog.get_for_current_route_map("stmt1").is_none());
}

#[test]
fn removed_shard_with_active_sessions_enters_draining_state() {
    let store = ShardRouteMapStore::new(vec![route_map("tenant-a")]);
    store.set_transaction_shard_affinity(88, shard_id("tenant-a"));

    let result = store.reload(vec![route_map("tenant-b")], None);

    assert!(result.success);
    assert_eq!(result.draining_shard_ids.len(), 1);
    assert_eq!(result.draining_shard_ids[0].as_str(), "tenant-a");
    assert_eq!(store.draining_shard_ids()[0].as_str(), "tenant-a");
}

#[test]
fn reload_snapshots_expose_success_failure_generation_and_error_code() {
    let snapshot_store = SnapshotStore::new();
    let store = ShardRouteMapStore::new(vec![route_map("tenant-a")]);

    let success = store.reload(vec![route_map("tenant-b")], Some(&snapshot_store));
    assert!(success.success);

    let failure = store.reload(
        vec![route_map("tenant-c"), route_map("tenant-d")],
        Some(&snapshot_store),
    );
    assert!(!failure.success);

    let snapshots = snapshot_store.route_map_reload_snapshots();
    assert_eq!(snapshots.len(), 2);

    assert!(snapshots[0].success);
    assert_eq!(snapshots[0].route_map_generation_id, 1);
    assert!(snapshots[0].error_code.is_none());

    assert!(!snapshots[1].success);
    assert_eq!(snapshots[1].route_map_generation_id, 1);
    assert_eq!(
        snapshots[1].error_code,
        Some(RouteMapReloadErrorCode::ConflictingRouteScopes)
    );
}
