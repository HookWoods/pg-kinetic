use pg_kinetic_core::{
    lsn::PgLsn,
    policy::PolicyAction,
    routing::{FallbackPolicy, FreshnessPolicy, QueryClass, ReadRoutingMode, RoutingHint},
    session::TransactionState,
    sharding::{ShardRouteDecision, ShardRouteReason},
    sql_classify::{classify_sql, extract_routing_hint},
    virtual_session::ReadAfterWriteState,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutingReason {
    Off,
    PrimaryOnlyMode,
    RequireReplicaMode,
    InTransaction,
    FailedTransaction,
    WriteQuery,
    UnknownQuery,
    TransactionControl,
    SessionMutation,
    CopyQuery,
    ReadCandidateQuery,
    ReadOnlyQuery,
    PrimaryHint,
    ReplicaHint,
    StaleOkHint,
    StrictFreshHint,
    FreshnessRequired,
    ReplicaStale,
    ReplicaUnavailable,
    FallbackPrimary,
    FallbackReject,
    FallbackWait,
    PolicyDenied,
    PolicyRequirePrimary,
    PolicyRequireReplica,
    PolicyRouteOverride,
    PolicyShardOverride,
}

impl RoutingReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::PrimaryOnlyMode => "primary_only_mode",
            Self::RequireReplicaMode => "require_replica_mode",
            Self::InTransaction => "in_transaction",
            Self::FailedTransaction => "failed_transaction",
            Self::WriteQuery => "write_query",
            Self::UnknownQuery => "unknown_query",
            Self::TransactionControl => "transaction_control",
            Self::SessionMutation => "session_mutation",
            Self::CopyQuery => "copy_query",
            Self::ReadCandidateQuery => "read_candidate_query",
            Self::ReadOnlyQuery => "read_only_query",
            Self::PrimaryHint => "primary_hint",
            Self::ReplicaHint => "replica_hint",
            Self::StaleOkHint => "stale_ok_hint",
            Self::StrictFreshHint => "strict_fresh_hint",
            Self::FreshnessRequired => "freshness_required",
            Self::ReplicaStale => "replica_stale",
            Self::ReplicaUnavailable => "replica_unavailable",
            Self::FallbackPrimary => "fallback_primary",
            Self::FallbackReject => "fallback_reject",
            Self::FallbackWait => "fallback_wait",
            Self::PolicyDenied => "policy_denied",
            Self::PolicyRequirePrimary => "policy_require_primary",
            Self::PolicyRequireReplica => "policy_require_replica",
            Self::PolicyRouteOverride => "policy_route_override",
            Self::PolicyShardOverride => "policy_shard_override",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRoutingPlanner {
    read_routing_mode: ReadRoutingMode,
    fallback_policy: FallbackPolicy,
    freshness_policy: FreshnessPolicy,
    max_replica_lag_ms: u64,
}

impl ReadRoutingPlanner {
    #[must_use]
    pub const fn new(
        read_routing_mode: ReadRoutingMode,
        fallback_policy: FallbackPolicy,
        freshness_policy: FreshnessPolicy,
        max_replica_lag_ms: u64,
    ) -> Self {
        Self {
            read_routing_mode,
            fallback_policy,
            freshness_policy,
            max_replica_lag_ms,
        }
    }

    #[must_use]
    pub fn choose_routing_target(&self, context: RoutingContext<'_>) -> RoutingTarget {
        choose_routing_target(self, context)
    }

    #[must_use]
    pub const fn read_routing_mode(&self) -> ReadRoutingMode {
        self.read_routing_mode
    }

    #[must_use]
    pub const fn fallback_policy(&self) -> FallbackPolicy {
        self.fallback_policy
    }

    #[must_use]
    pub const fn freshness_policy(&self) -> FreshnessPolicy {
        self.freshness_policy
    }

    #[must_use]
    pub const fn max_replica_lag_ms(&self) -> u64 {
        self.max_replica_lag_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingContext<'a> {
    pub sql: &'a str,
    pub transaction_state: TransactionState,
    pub read_after_write_state: ReadAfterWriteState,
    pub health: &'a RouteHealthSnapshot,
}

impl<'a> RoutingContext<'a> {
    #[must_use]
    pub const fn new(
        sql: &'a str,
        transaction_state: TransactionState,
        read_after_write_state: ReadAfterWriteState,
        health: &'a RouteHealthSnapshot,
    ) -> Self {
        Self {
            sql,
            transaction_state,
            read_after_write_state,
            health,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteHealthSnapshot {
    pub replicas: Vec<ReplicaCandidate>,
}

impl RouteHealthSnapshot {
    #[must_use]
    pub fn new(replicas: Vec<ReplicaCandidate>) -> Self {
        Self { replicas }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaCandidate {
    pub replica_id: u64,
    pub healthy: bool,
    pub split_brain: bool,
    pub replay_lsn: Option<PgLsn>,
    pub lag_ms: Option<u64>,
}

impl ReplicaCandidate {
    #[must_use]
    pub const fn new(
        replica_id: u64,
        healthy: bool,
        replay_lsn: Option<PgLsn>,
        lag_ms: Option<u64>,
    ) -> Self {
        Self {
            replica_id,
            healthy,
            split_brain: false,
            replay_lsn,
            lag_ms,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutingTarget {
    Primary {
        reason: RoutingReason,
    },
    Replica {
        candidate: ReplicaCandidate,
        reason: RoutingReason,
    },
    Wait {
        reason: RoutingReason,
    },
    Reject {
        reason: RoutingReason,
    },
}

impl RoutingTarget {
    #[must_use]
    pub const fn reason(&self) -> RoutingReason {
        match self {
            Self::Primary { reason }
            | Self::Replica { reason, .. }
            | Self::Wait { reason }
            | Self::Reject { reason } => *reason,
        }
    }

    #[must_use]
    pub const fn target_role(&self) -> Option<pg_kinetic_core::routing::BackendRole> {
        match self {
            Self::Primary { .. } => Some(pg_kinetic_core::routing::BackendRole::Primary),
            Self::Replica { .. } => Some(pg_kinetic_core::routing::BackendRole::Replica),
            Self::Wait { .. } | Self::Reject { .. } => None,
        }
    }

    #[must_use]
    pub fn replica(&self) -> Option<ReplicaCandidate> {
        match self {
            Self::Replica { candidate, .. } => Some(candidate.clone()),
            Self::Primary { .. } | Self::Wait { .. } | Self::Reject { .. } => None,
        }
    }
}

#[must_use]
pub fn choose_routing_target(
    planner: &ReadRoutingPlanner,
    context: RoutingContext<'_>,
) -> RoutingTarget {
    let query_class = classify_sql(context.sql);
    let routing_hint = extract_routing_hint(context.sql);

    match planner.read_routing_mode {
        ReadRoutingMode::Off => {
            return RoutingTarget::Primary {
                reason: RoutingReason::Off,
            };
        }
        ReadRoutingMode::PrimaryOnly => {
            return RoutingTarget::Primary {
                reason: RoutingReason::PrimaryOnlyMode,
            };
        }
        ReadRoutingMode::RequireReplica | ReadRoutingMode::PreferReplica => {}
    }

    match context.transaction_state {
        TransactionState::InTransaction => {
            return RoutingTarget::Primary {
                reason: RoutingReason::InTransaction,
            };
        }
        TransactionState::FailedTransaction => {
            return RoutingTarget::Primary {
                reason: RoutingReason::FailedTransaction,
            };
        }
        TransactionState::Idle => {}
    }

    if !query_class.routes_to_replica() {
        return primary_for_query_class(query_class);
    }

    if routing_hint == RoutingHint::Primary {
        return RoutingTarget::Primary {
            reason: RoutingReason::PrimaryHint,
        };
    }

    let effective_freshness_policy =
        effective_freshness_policy(planner.freshness_policy, routing_hint);

    if routing_hint == RoutingHint::StrictFresh
        && matches!(
            context.read_after_write_state,
            ReadAfterWriteState::Disabled
        )
    {
        return fallback_target(planner.fallback_policy);
    }

    let safe_replica = select_safe_replica(
        context.health,
        context.read_after_write_state,
        planner.max_replica_lag_ms,
        effective_freshness_policy,
    );

    if let Some(candidate) = safe_replica {
        return RoutingTarget::Replica {
            candidate,
            reason: success_reason(routing_hint),
        };
    }

    if planner.read_routing_mode == ReadRoutingMode::RequireReplica {
        return RoutingTarget::Reject {
            reason: RoutingReason::RequireReplicaMode,
        };
    }

    let any_healthy = context
        .health
        .replicas
        .iter()
        .any(|candidate| candidate.healthy);
    if !any_healthy {
        return fallback_target(planner.fallback_policy);
    }

    if requires_session_write_lsn(effective_freshness_policy)
        && !matches!(
            context.read_after_write_state,
            ReadAfterWriteState::Disabled
        )
    {
        let reason = match context.read_after_write_state {
            ReadAfterWriteState::Unknown => RoutingReason::FreshnessRequired,
            ReadAfterWriteState::Required(_) => RoutingReason::ReplicaStale,
            ReadAfterWriteState::Disabled => RoutingReason::FallbackPrimary,
        };

        return match planner.fallback_policy {
            FallbackPolicy::Primary => RoutingTarget::Primary {
                reason: RoutingReason::FallbackPrimary,
            },
            FallbackPolicy::Reject => RoutingTarget::Reject { reason },
            FallbackPolicy::Wait => RoutingTarget::Wait {
                reason: RoutingReason::FallbackWait,
            },
        };
    }

    fallback_target(planner.fallback_policy)
}

#[must_use]
pub fn policy_denied_target() -> RoutingTarget {
    RoutingTarget::Reject {
        reason: RoutingReason::PolicyDenied,
    }
}

#[must_use]
pub fn apply_policy_action_to_routing_target(
    planner: &ReadRoutingPlanner,
    context: RoutingContext<'_>,
    current_target: Option<RoutingTarget>,
    action: Option<&PolicyAction>,
) -> RoutingTarget {
    let routing_context = context.clone();
    let current_target =
        current_target.unwrap_or_else(|| choose_routing_target(planner, routing_context.clone()));

    let target = match action {
        None | Some(PolicyAction::Allow) => current_target,
        Some(PolicyAction::Deny { .. }) => policy_denied_target(),
        Some(PolicyAction::RequirePrimary) => RoutingTarget::Primary {
            reason: RoutingReason::PolicyRequirePrimary,
        },
        Some(PolicyAction::RequireReplica) => {
            let replica_planner = ReadRoutingPlanner::new(
                ReadRoutingMode::RequireReplica,
                planner.fallback_policy(),
                planner.freshness_policy(),
                planner.max_replica_lag_ms(),
            );
            match choose_routing_target(&replica_planner, routing_context.clone()) {
                RoutingTarget::Replica { candidate, .. } => RoutingTarget::Replica {
                    candidate,
                    reason: RoutingReason::PolicyRequireReplica,
                },
                RoutingTarget::Wait { .. } | RoutingTarget::Reject { .. } => {
                    RoutingTarget::Reject {
                        reason: RoutingReason::PolicyRequireReplica,
                    }
                }
                RoutingTarget::Primary { .. } => RoutingTarget::Primary {
                    reason: RoutingReason::PolicyRequireReplica,
                },
            }
        }
        Some(PolicyAction::RouteOverride { .. }) => {
            map_routing_target_reason(current_target, RoutingReason::PolicyRouteOverride)
        }
        Some(PolicyAction::ShardOverride { .. }) => {
            map_routing_target_reason(current_target, RoutingReason::PolicyShardOverride)
        }
    };

    ensure_policy_action_target_is_safe(planner, routing_context, target)
}

fn primary_for_query_class(query_class: QueryClass) -> RoutingTarget {
    let reason = match query_class {
        QueryClass::Write => RoutingReason::WriteQuery,
        QueryClass::ReadOnly => RoutingReason::ReadOnlyQuery,
        QueryClass::ReadCandidate => RoutingReason::ReadCandidateQuery,
        QueryClass::TransactionControl => RoutingReason::TransactionControl,
        QueryClass::SessionMutation => RoutingReason::SessionMutation,
        QueryClass::Copy => RoutingReason::CopyQuery,
        QueryClass::Unknown => RoutingReason::UnknownQuery,
    };

    RoutingTarget::Primary { reason }
}

fn success_reason(routing_hint: RoutingHint) -> RoutingReason {
    match routing_hint {
        RoutingHint::Primary => RoutingReason::PrimaryHint,
        RoutingHint::Replica => RoutingReason::ReplicaHint,
        RoutingHint::StaleOk => RoutingReason::StaleOkHint,
        RoutingHint::StrictFresh => RoutingReason::StrictFreshHint,
        RoutingHint::None => RoutingReason::ReadCandidateQuery,
    }
}

fn fallback_target(fallback_policy: FallbackPolicy) -> RoutingTarget {
    match fallback_policy {
        FallbackPolicy::Primary => RoutingTarget::Primary {
            reason: RoutingReason::FallbackPrimary,
        },
        FallbackPolicy::Reject => RoutingTarget::Reject {
            reason: RoutingReason::FallbackReject,
        },
        FallbackPolicy::Wait => RoutingTarget::Wait {
            reason: RoutingReason::FallbackWait,
        },
    }
}

fn map_routing_target_reason(target: RoutingTarget, reason: RoutingReason) -> RoutingTarget {
    match target {
        RoutingTarget::Primary { .. } => RoutingTarget::Primary { reason },
        RoutingTarget::Replica { candidate, .. } => RoutingTarget::Replica { candidate, reason },
        RoutingTarget::Wait { .. } => RoutingTarget::Wait { reason },
        RoutingTarget::Reject { .. } => RoutingTarget::Reject { reason },
    }
}

pub(crate) fn ensure_policy_action_target_is_safe(
    planner: &ReadRoutingPlanner,
    context: RoutingContext<'_>,
    target: RoutingTarget,
) -> RoutingTarget {
    match target {
        RoutingTarget::Replica { candidate, reason }
            if candidate.healthy
                && !candidate.split_brain
                && replica_satisfies_freshness(
                    &candidate,
                    planner.freshness_policy,
                    context.read_after_write_state,
                    planner.max_replica_lag_ms(),
                ) =>
        {
            RoutingTarget::Replica { candidate, reason }
        }
        RoutingTarget::Replica { .. } => choose_routing_target(planner, context),
        other => other,
    }
}

fn select_safe_replica(
    health: &RouteHealthSnapshot,
    read_after_write_state: ReadAfterWriteState,
    max_replica_lag_ms: u64,
    freshness_policy: FreshnessPolicy,
) -> Option<ReplicaCandidate> {
    let mut candidates: Vec<&ReplicaCandidate> = health
        .replicas
        .iter()
        .filter(|candidate| candidate.healthy && !candidate.split_brain)
        .filter(|candidate| {
            replica_satisfies_freshness(
                candidate,
                freshness_policy,
                read_after_write_state,
                max_replica_lag_ms,
            )
        })
        .collect();

    candidates
        .sort_by_key(|candidate| (candidate.lag_ms.unwrap_or(u64::MAX), candidate.replica_id));

    candidates.into_iter().next().cloned()
}

fn replica_satisfies_freshness(
    candidate: &ReplicaCandidate,
    freshness_policy: FreshnessPolicy,
    read_after_write_state: ReadAfterWriteState,
    max_replica_lag_ms: u64,
) -> bool {
    let effective_policy = freshness_policy;

    if requires_session_write_lsn(effective_policy) {
        match read_after_write_state {
            ReadAfterWriteState::Disabled => {}
            ReadAfterWriteState::Unknown => return false,
            ReadAfterWriteState::Required(required_session_write_lsn) => {
                let Some(replay_lsn) = candidate.replay_lsn else {
                    return false;
                };
                if replay_lsn < required_session_write_lsn {
                    return false;
                }
            }
        };
    }

    if requires_replica_lag(effective_policy) {
        let Some(lag_ms) = candidate.lag_ms else {
            return false;
        };
        if lag_ms > max_replica_lag_ms {
            return false;
        }
    }

    true
}

fn effective_freshness_policy(
    configured_policy: FreshnessPolicy,
    routing_hint: RoutingHint,
) -> FreshnessPolicy {
    match routing_hint {
        RoutingHint::StrictFresh => FreshnessPolicy::SessionWriteLsnAndMaxLag,
        RoutingHint::StaleOk => match configured_policy {
            FreshnessPolicy::SessionWriteLsnAndMaxLag => FreshnessPolicy::MaxReplicaLag,
            other => other,
        },
        RoutingHint::Primary | RoutingHint::Replica | RoutingHint::None => configured_policy,
    }
}

fn requires_session_write_lsn(policy: FreshnessPolicy) -> bool {
    matches!(
        policy,
        FreshnessPolicy::SessionWriteLsn | FreshnessPolicy::SessionWriteLsnAndMaxLag
    )
}

fn requires_replica_lag(policy: FreshnessPolicy) -> bool {
    matches!(
        policy,
        FreshnessPolicy::MaxReplicaLag | FreshnessPolicy::SessionWriteLsnAndMaxLag
    )
}

#[must_use]
pub fn bridge_shard_route_decision(
    decision: &ShardRouteDecision,
    sql: &str,
    planner: &ReadRoutingPlanner,
) -> pg_kinetic_core::routing::RoutingDecision {
    let query_class = classify_sql(sql);
    let routing_hint = extract_routing_hint(sql);
    let target_role = decision
        .route()
        .map(|route| route.target().backend_role())
        .unwrap_or(pg_kinetic_core::routing::BackendRole::Unknown);

    let reason = match decision.reason() {
        ShardRouteReason::AdminOverride => match target_role {
            pg_kinetic_core::routing::BackendRole::Primary => {
                pg_kinetic_core::routing::RoutingReason::ReadOnlyQuery
            }
            pg_kinetic_core::routing::BackendRole::Replica => {
                pg_kinetic_core::routing::RoutingReason::ReadCandidateQuery
            }
            pg_kinetic_core::routing::BackendRole::Unknown => {
                pg_kinetic_core::routing::RoutingReason::UnknownQuery
            }
        },
        ShardRouteReason::HashMatch
        | ShardRouteReason::RangeMatch
        | ShardRouteReason::ListMatch => match target_role {
            pg_kinetic_core::routing::BackendRole::Primary => {
                pg_kinetic_core::routing::RoutingReason::ReadOnlyQuery
            }
            pg_kinetic_core::routing::BackendRole::Replica => {
                pg_kinetic_core::routing::RoutingReason::ReadCandidateQuery
            }
            pg_kinetic_core::routing::BackendRole::Unknown => {
                pg_kinetic_core::routing::RoutingReason::UnknownQuery
            }
        },
        ShardRouteReason::MultiShardRejected | ShardRouteReason::ValidationFailed => {
            pg_kinetic_core::routing::RoutingReason::FallbackReject
        }
        ShardRouteReason::NoMatch => match planner.fallback_policy() {
            FallbackPolicy::Primary => pg_kinetic_core::routing::RoutingReason::FallbackPrimary,
            FallbackPolicy::Reject => pg_kinetic_core::routing::RoutingReason::FallbackReject,
            FallbackPolicy::Wait => pg_kinetic_core::routing::RoutingReason::FallbackWait,
        },
    };

    pg_kinetic_core::routing::RoutingDecision::new(
        target_role,
        query_class,
        routing_hint,
        reason,
        planner.fallback_policy(),
        planner.freshness_policy(),
    )
}
