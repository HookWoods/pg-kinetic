use pg_kinetic::{
    core::routing::{BackendRole, FallbackPolicy, FreshnessPolicy, ReadRoutingMode, RoutingReason},
    core::session::{TransactionShardDecision, TransactionShardState},
    core::sharding::{MultiShardPolicy, ShardId},
    core::virtual_session::VirtualSession,
    virtual_session::PinReason,
};
use pg_kinetic_proxy::routing::{
    choose_routing_target, ReadRoutingPlanner, ReplicaCandidate, RouteHealthSnapshot,
    RoutingContext, RoutingTarget,
};

fn shard_id(label: &str) -> ShardId {
    ShardId::new(label).expect("valid shard id")
}

fn routing_planner() -> ReadRoutingPlanner {
    ReadRoutingPlanner::new(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::None,
        1_000,
    )
}

fn healthy_replica() -> RouteHealthSnapshot {
    RouteHealthSnapshot::new(vec![ReplicaCandidate::new(1, true, None, None)])
}

#[test]
fn first_routed_statement_sets_transaction_shard_affinity() {
    let mut session = VirtualSession::default();
    session.apply_transaction_sql("begin");

    let decision = session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );

    assert_eq!(decision, TransactionShardDecision::Accepted);
    assert_eq!(
        session.current_transaction_shard_id().map(ShardId::as_str),
        Some("tenant-a")
    );
    assert_eq!(
        session.current_transaction_shard_route_reason(),
        Some(RoutingReason::ReadCandidateQuery)
    );
    assert!(!session.transaction_cross_shard_violation());
}

#[test]
fn same_shard_statements_are_accepted() {
    let mut session = VirtualSession::default();
    session.apply_transaction_sql("begin");
    session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );

    let decision = session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadOnlyQuery,
        MultiShardPolicy::Reject,
    );

    assert_eq!(decision, TransactionShardDecision::Accepted);
    assert_eq!(
        session.current_transaction_shard_id().map(ShardId::as_str),
        Some("tenant-a")
    );
    assert_eq!(
        session.current_transaction_shard_route_reason(),
        Some(RoutingReason::ReadCandidateQuery)
    );
}

#[test]
fn different_shard_statements_are_rejected_by_default() {
    let mut session = VirtualSession::default();
    session.apply_transaction_sql("begin");
    session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );

    let decision = session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-b")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );

    assert_eq!(decision, TransactionShardDecision::Rejected);
    assert!(session.transaction_cross_shard_violation());
}

#[test]
fn unknown_shard_statements_follow_multi_shard_policy() {
    let mut session = VirtualSession::default();
    session.apply_transaction_sql("begin");
    session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );

    let decision = session.apply_transaction_shard_affinity(
        None,
        RoutingReason::UnknownQuery,
        MultiShardPolicy::FanOut,
    );

    assert_eq!(decision, TransactionShardDecision::FollowMultiShardPolicy);
    assert!(!session.transaction_cross_shard_violation());
}

#[test]
fn write_transactions_stay_on_primary_inside_the_selected_shard() {
    let mut session = VirtualSession::default();
    session.apply_transaction_sql("begin read write");

    let decision = session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::TransactionControl,
        MultiShardPolicy::Reject,
    );

    assert_eq!(decision, TransactionShardDecision::Accepted);
    assert_eq!(
        session.current_transaction_target_role(),
        Some(BackendRole::Primary)
    );
    assert_eq!(
        session.current_transaction_route_reason(),
        Some(RoutingReason::TransactionControl)
    );
    assert_eq!(
        session.current_transaction_shard_id().map(ShardId::as_str),
        Some("tenant-a")
    );
}

#[test]
fn read_only_transactions_can_use_replicas_inside_the_selected_shard() {
    let mut session = VirtualSession::default();
    session.apply_transaction_sql("begin read only");

    let decision = session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadOnlyQuery,
        MultiShardPolicy::Reject,
    );

    assert_eq!(decision, TransactionShardDecision::Accepted);
    assert_eq!(
        session.current_transaction_target_role(),
        Some(BackendRole::Replica)
    );
    assert_eq!(
        session.current_transaction_route_reason(),
        Some(RoutingReason::ReadOnlyQuery)
    );

    let target = choose_routing_target(
        &routing_planner(),
        RoutingContext::new(
            "select 1",
            pg_kinetic::core::session::TransactionState::Idle,
            pg_kinetic::core::virtual_session::ReadAfterWriteState::Disabled,
            &healthy_replica(),
        ),
    );
    assert!(matches!(target, RoutingTarget::Replica { .. }));
}

#[test]
fn commit_and_rollback_clear_shard_affinity() {
    let mut commit_session = VirtualSession::default();
    commit_session.apply_transaction_sql("begin");
    commit_session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );
    commit_session.apply_transaction_sql("commit");
    assert!(commit_session.transaction_shard_state().is_none());

    let mut rollback_session = VirtualSession::default();
    rollback_session.apply_transaction_sql("begin");
    rollback_session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );
    rollback_session.apply_transaction_sql("rollback");
    assert!(rollback_session.transaction_shard_state().is_none());
}

#[test]
fn existing_pinning_rules_remain_stronger_than_sharding_rules() {
    let mut session = VirtualSession::default();
    session.apply_transaction_sql("begin");

    assert_eq!(session.pin_reason(), Some(PinReason::OpenTransaction));

    let decision = session.apply_transaction_shard_affinity(
        Some(shard_id("tenant-a")),
        RoutingReason::ReadCandidateQuery,
        MultiShardPolicy::Reject,
    );

    assert_eq!(decision, TransactionShardDecision::Accepted);
    assert_eq!(session.pin_reason(), Some(PinReason::OpenTransaction));
}

#[test]
fn transaction_shard_state_tracks_route_reason_and_violation_flag() {
    let mut state =
        TransactionShardState::new(shard_id("tenant-a"), RoutingReason::ReadCandidateQuery);

    assert_eq!(state.current_shard_id().as_str(), "tenant-a");
    assert_eq!(
        state.current_shard_route_reason(),
        RoutingReason::ReadCandidateQuery
    );
    assert!(!state.cross_shard_violation());

    state.set_current_shard_route_reason(RoutingReason::ReadOnlyQuery);
    state.mark_cross_shard_violation();

    assert_eq!(
        state.current_shard_route_reason(),
        RoutingReason::ReadOnlyQuery
    );
    assert!(state.cross_shard_violation());
}
