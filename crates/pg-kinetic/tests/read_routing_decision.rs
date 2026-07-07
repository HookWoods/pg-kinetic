use pg_kinetic_core::{
    lsn::PgLsn,
    routing::{FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
    session::TransactionState,
};
use pg_kinetic_proxy::routing::{
    choose_routing_target, ReadRoutingPlanner, ReplicaCandidate, RouteHealthSnapshot,
    RoutingContext, RoutingReason, RoutingTarget,
};

fn planner(
    read_routing_mode: ReadRoutingMode,
    fallback_policy: FallbackPolicy,
    freshness_policy: FreshnessPolicy,
    max_replica_lag_ms: u64,
) -> ReadRoutingPlanner {
    ReadRoutingPlanner::new(
        read_routing_mode,
        fallback_policy,
        freshness_policy,
        max_replica_lag_ms,
    )
}

fn replica(
    replica_id: u64,
    healthy: bool,
    replay_lsn: Option<PgLsn>,
    lag_ms: Option<u64>,
) -> ReplicaCandidate {
    ReplicaCandidate::new(replica_id, healthy, replay_lsn, lag_ms)
}

fn snapshot(replicas: Vec<ReplicaCandidate>) -> RouteHealthSnapshot {
    RouteHealthSnapshot::new(replicas)
}

fn context<'a>(
    sql: &'a str,
    transaction_state: TransactionState,
    session_write_lsn: Option<PgLsn>,
    health: &'a RouteHealthSnapshot,
) -> RoutingContext<'a> {
    RoutingContext::new(sql, transaction_state, session_write_lsn, health)
}

fn assert_primary(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Primary { reason } => assert_eq!(reason, expected_reason),
        other => panic!("expected primary target, got {other:?}"),
    }
}

fn assert_replica(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Replica { reason, .. } => assert_eq!(reason, expected_reason),
        other => panic!("expected replica target, got {other:?}"),
    }
}

fn assert_wait(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Wait { reason } => assert_eq!(reason, expected_reason),
        other => panic!("expected wait target, got {other:?}"),
    }
}

fn assert_reject(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Reject { reason } => assert_eq!(reason, expected_reason),
        other => panic!("expected reject target, got {other:?}"),
    }
}

#[test]
fn routing_mode_off_sends_all_traffic_to_primary() {
    let planner = planner(
        ReadRoutingMode::Off,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::Off);
}

#[test]
fn writes_always_go_to_primary() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "INSERT INTO accounts VALUES (1)",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::WriteQuery);
}

#[test]
fn unknown_sql_always_goes_to_primary() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "???",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::UnknownQuery);
}

#[test]
fn explicit_primary_hint_sends_query_to_primary() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: primary */ SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::PrimaryHint);
}

#[test]
fn explicit_replica_hint_routes_eligible_query_to_replica_when_freshness_permits() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        7,
        true,
        Some(PgLsn::from_parts(1, 20)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: replica */ SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_replica(target, RoutingReason::ReplicaHint);
}

#[test]
fn stale_ok_bypasses_session_lsn_freshness_but_still_requires_healthy_replica() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(3, true, None, Some(5))]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: stale-ok */ SELECT 1",
            TransactionState::Idle,
            None,
            &health,
        ),
    );

    assert_replica(target, RoutingReason::StaleOkHint);
}

#[test]
fn strict_fresh_requires_session_lsn_and_lag_freshness() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        10,
    );
    let health = snapshot(vec![replica(
        9,
        true,
        Some(PgLsn::from_parts(2, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: strict-fresh */ SELECT 1",
            TransactionState::Idle,
            None,
            &health,
        ),
    );

    assert_primary(target, RoutingReason::FallbackPrimary);
}

#[test]
fn no_healthy_replica_follows_fallback_policy() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Wait,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        false,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_wait(target, RoutingReason::FallbackWait);
}

#[test]
fn replica_lag_beyond_limit_follows_fallback_policy() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Reject,
        FreshnessPolicy::MaxReplicaLag,
        50,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(500),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_reject(target, RoutingReason::FallbackReject);
}

#[test]
fn require_replica_rejects_when_no_replica_is_safe() {
    let planner = planner(
        ReadRoutingMode::RequireReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        10,
    );
    let health = snapshot(vec![replica(
        2,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(500),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_reject(target, RoutingReason::RequireReplicaMode);
}
