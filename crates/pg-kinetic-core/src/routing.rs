#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendRole {
    Primary,
    Replica,
    Unknown,
}

impl BackendRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Replica => "replica",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadRoutingMode {
    #[default]
    Off,
    PreferReplica,
    RequireReplica,
    PrimaryOnly,
}

impl ReadRoutingMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::PreferReplica => "prefer_replica",
            Self::RequireReplica => "require_replica",
            Self::PrimaryOnly => "primary_only",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryClass {
    Write,
    ReadOnly,
    ReadCandidate,
    TransactionControl,
    SessionMutation,
    Copy,
    Unknown,
}

impl QueryClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::ReadOnly => "read_only",
            Self::ReadCandidate => "read_candidate",
            Self::TransactionControl => "transaction_control",
            Self::SessionMutation => "session_mutation",
            Self::Copy => "copy",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn routes_to_replica(self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadCandidate)
    }

    #[must_use]
    pub const fn target_role(self) -> BackendRole {
        if self.routes_to_replica() {
            BackendRole::Replica
        } else {
            BackendRole::Primary
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutingHint {
    Primary,
    Replica,
    StaleOk,
    StrictFresh,
    None,
}

impl RoutingHint {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Replica => "replica",
            Self::StaleOk => "stale_ok",
            Self::StrictFresh => "strict_fresh",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FallbackPolicy {
    Primary,
    Reject,
    Wait,
}

impl FallbackPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Reject => "reject",
            Self::Wait => "wait",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FreshnessPolicy {
    None,
    SessionWriteLsn,
    MaxReplicaLag,
    SessionWriteLsnAndMaxLag,
}

impl FreshnessPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SessionWriteLsn => "session_write_lsn",
            Self::MaxReplicaLag => "max_replica_lag",
            Self::SessionWriteLsnAndMaxLag => "session_write_lsn_and_max_lag",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutingReason {
    Off,
    PrimaryOnlyMode,
    PreferReplicaMode,
    RequireReplicaMode,
    WriteQuery,
    ReadOnlyQuery,
    ReadCandidateQuery,
    TransactionControl,
    SessionMutation,
    Copy,
    UnknownQuery,
    FreshnessRequired,
    ReplicaStale,
    ReplicaUnavailable,
    FallbackPrimary,
    FallbackReject,
    FallbackWait,
}

impl RoutingReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::PrimaryOnlyMode => "primary_only_mode",
            Self::PreferReplicaMode => "prefer_replica_mode",
            Self::RequireReplicaMode => "require_replica_mode",
            Self::WriteQuery => "write_query",
            Self::ReadOnlyQuery => "read_only_query",
            Self::ReadCandidateQuery => "read_candidate_query",
            Self::TransactionControl => "transaction_control",
            Self::SessionMutation => "session_mutation",
            Self::Copy => "copy",
            Self::UnknownQuery => "unknown_query",
            Self::FreshnessRequired => "freshness_required",
            Self::ReplicaStale => "replica_stale",
            Self::ReplicaUnavailable => "replica_unavailable",
            Self::FallbackPrimary => "fallback_primary",
            Self::FallbackReject => "fallback_reject",
            Self::FallbackWait => "fallback_wait",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingDecision {
    pub target_role: BackendRole,
    pub query_class: QueryClass,
    pub hint: RoutingHint,
    pub reason: RoutingReason,
    pub fallback_policy: FallbackPolicy,
    pub freshness_requirement: FreshnessPolicy,
}

impl RoutingDecision {
    #[must_use]
    pub const fn new(
        target_role: BackendRole,
        query_class: QueryClass,
        hint: RoutingHint,
        reason: RoutingReason,
        fallback_policy: FallbackPolicy,
        freshness_requirement: FreshnessPolicy,
    ) -> Self {
        Self {
            target_role,
            query_class,
            hint,
            reason,
            fallback_policy,
            freshness_requirement,
        }
    }
}
