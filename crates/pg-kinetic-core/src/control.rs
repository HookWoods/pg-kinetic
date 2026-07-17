use std::{collections::HashMap, fmt, sync::Arc};

use crate::runtime::{NodeId, ReadinessState, RuntimeLifecycleState};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ClusterViewGeneration(u64);

impl ClusterViewGeneration {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn initial() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<u64> for ClusterViewGeneration {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<ClusterViewGeneration> for u64 {
    fn from(value: ClusterViewGeneration) -> Self {
        value.as_u64()
    }
}

impl fmt::Display for ClusterViewGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum PeerHealth {
    Healthy,
    Overloaded,
    #[default]
    Unknown,
}

impl PeerHealth {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Overloaded => "overloaded",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for PeerHealth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLifecycleSummary {
    lifecycle: RuntimeLifecycleState,
    readiness: ReadinessState,
    route_map_generation: ClusterViewGeneration,
    policy_generation: ClusterViewGeneration,
    overloaded: bool,
}

impl NodeLifecycleSummary {
    #[must_use]
    pub const fn new(
        lifecycle: RuntimeLifecycleState,
        readiness: ReadinessState,
        route_map_generation: ClusterViewGeneration,
        policy_generation: ClusterViewGeneration,
        overloaded: bool,
    ) -> Self {
        Self {
            lifecycle,
            readiness,
            route_map_generation,
            policy_generation,
            overloaded,
        }
    }

    #[must_use]
    pub const fn lifecycle(&self) -> RuntimeLifecycleState {
        self.lifecycle
    }

    #[must_use]
    pub const fn readiness(&self) -> ReadinessState {
        self.readiness
    }

    #[must_use]
    pub const fn route_map_generation(&self) -> ClusterViewGeneration {
        self.route_map_generation
    }

    #[must_use]
    pub const fn policy_generation(&self) -> ClusterViewGeneration {
        self.policy_generation
    }

    #[must_use]
    pub const fn overload_state(&self) -> bool {
        self.overloaded
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStatus {
    node_id: NodeId,
    lifecycle: NodeLifecycleSummary,
    health: PeerHealth,
    metadata: Arc<str>,
    metadata_redacted: bool,
}

impl NodeStatus {
    #[must_use]
    pub fn new(
        node_id: NodeId,
        lifecycle: NodeLifecycleSummary,
        health: PeerHealth,
        metadata: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            node_id,
            lifecycle,
            health,
            metadata: metadata.into(),
            metadata_redacted: false,
        }
    }

    #[must_use]
    pub fn redacted(node_id: NodeId, lifecycle: NodeLifecycleSummary, health: PeerHealth) -> Self {
        Self {
            node_id,
            lifecycle,
            health,
            metadata: Arc::from("<redacted>"),
            metadata_redacted: true,
        }
    }

    #[must_use]
    pub fn redact(mut self) -> Self {
        self.metadata = Arc::from("<redacted>");
        self.metadata_redacted = true;
        self
    }

    #[must_use]
    pub fn with_health(mut self, health: PeerHealth) -> Self {
        self.health = health;
        self
    }

    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    #[must_use]
    pub const fn lifecycle(&self) -> &NodeLifecycleSummary {
        &self.lifecycle
    }

    #[must_use]
    pub const fn health(&self) -> PeerHealth {
        self.health
    }

    #[must_use]
    pub fn metadata(&self) -> &str {
        &self.metadata
    }

    #[must_use]
    pub const fn metadata_is_redacted(&self) -> bool {
        self.metadata_redacted
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeHeartbeat {
    status: NodeStatus,
    generation: ClusterViewGeneration,
}

impl NodeHeartbeat {
    #[must_use]
    pub fn new(status: NodeStatus, generation: ClusterViewGeneration) -> Self {
        Self { status, generation }
    }

    #[must_use]
    pub const fn status(&self) -> &NodeStatus {
        &self.status
    }

    #[must_use]
    pub const fn generation(&self) -> ClusterViewGeneration {
        self.generation
    }

    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        self.status.node_id()
    }

    #[must_use]
    pub const fn lifecycle_summary(&self) -> &NodeLifecycleSummary {
        self.status.lifecycle()
    }

    #[must_use]
    pub const fn lifecycle_state(&self) -> RuntimeLifecycleState {
        self.lifecycle_summary().lifecycle()
    }

    #[must_use]
    pub const fn readiness(&self) -> ReadinessState {
        self.lifecycle_summary().readiness()
    }

    #[must_use]
    pub const fn route_map_generation(&self) -> ClusterViewGeneration {
        self.lifecycle_summary().route_map_generation()
    }

    #[must_use]
    pub const fn policy_generation(&self) -> ClusterViewGeneration {
        self.lifecycle_summary().policy_generation()
    }

    #[must_use]
    pub const fn overload_state(&self) -> bool {
        self.lifecycle_summary().overload_state()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlEventKind {
    HeartbeatPublished,
    PeerHeartbeatObserved,
    PeerMarkedUnknown,
    ClusterViewUpdated,
    DrainRequested,
}

impl ControlEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HeartbeatPublished => "heartbeat_published",
            Self::PeerHeartbeatObserved => "peer_heartbeat_observed",
            Self::PeerMarkedUnknown => "peer_marked_unknown",
            Self::ClusterViewUpdated => "cluster_view_updated",
            Self::DrainRequested => "drain_requested",
        }
    }
}

impl fmt::Display for ControlEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlEvent {
    node_id: NodeId,
    generation: ClusterViewGeneration,
    kind: ControlEventKind,
    consensus_required: bool,
}

impl ControlEvent {
    #[must_use]
    pub fn new(node_id: NodeId, generation: ClusterViewGeneration, kind: ControlEventKind) -> Self {
        Self {
            node_id,
            generation,
            kind,
            consensus_required: false,
        }
    }

    #[must_use]
    pub fn drain_requested(node_id: NodeId, generation: ClusterViewGeneration) -> Self {
        Self::new(node_id, generation, ControlEventKind::DrainRequested)
    }

    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    #[must_use]
    pub const fn generation(&self) -> ClusterViewGeneration {
        self.generation
    }

    #[must_use]
    pub const fn kind(&self) -> ControlEventKind {
        self.kind
    }

    #[must_use]
    pub const fn requires_consensus(&self) -> bool {
        self.consensus_required
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerState {
    status: NodeStatus,
    heartbeat_generation: ClusterViewGeneration,
    unknown: bool,
}

impl PeerState {
    #[must_use]
    pub fn new(status: NodeStatus, heartbeat_generation: ClusterViewGeneration) -> Self {
        Self {
            status,
            heartbeat_generation,
            unknown: false,
        }
    }

    #[must_use]
    pub fn from_heartbeat(heartbeat: &NodeHeartbeat, stale: bool) -> Self {
        let health = if stale {
            PeerHealth::Unknown
        } else {
            heartbeat.status().health()
        };
        let status = heartbeat.status().clone().redact().with_health(health);
        Self {
            status,
            heartbeat_generation: heartbeat.generation(),
            unknown: stale,
        }
    }

    #[must_use]
    pub fn unknown(status: NodeStatus, heartbeat_generation: ClusterViewGeneration) -> Self {
        Self {
            status: status.redact().with_health(PeerHealth::Unknown),
            heartbeat_generation,
            unknown: true,
        }
    }

    #[must_use]
    pub const fn status(&self) -> &NodeStatus {
        &self.status
    }

    #[must_use]
    pub const fn health(&self) -> PeerHealth {
        self.status.health()
    }

    #[must_use]
    pub const fn heartbeat_generation(&self) -> ClusterViewGeneration {
        self.heartbeat_generation
    }

    #[must_use]
    pub const fn is_unknown(&self) -> bool {
        self.unknown || matches!(self.status.health(), PeerHealth::Unknown)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterView {
    generation: ClusterViewGeneration,
    local: NodeStatus,
    peers: HashMap<NodeId, PeerState>,
    authoritative: bool,
}

impl ClusterView {
    #[must_use]
    pub fn new(local: NodeStatus) -> Self {
        Self::with_generation(local, HashMap::new(), ClusterViewGeneration::initial())
    }

    #[must_use]
    pub fn with_peers(local: NodeStatus, peers: impl IntoIterator<Item = PeerState>) -> Self {
        let peers = peers
            .into_iter()
            .map(|peer| (peer.status().node_id().clone(), peer))
            .collect();
        Self::with_generation(local, peers, ClusterViewGeneration::initial())
    }

    #[must_use]
    pub fn with_generation(
        local: NodeStatus,
        peers: HashMap<NodeId, PeerState>,
        generation: ClusterViewGeneration,
    ) -> Self {
        Self {
            generation,
            local,
            peers,
            authoritative: false,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> ClusterViewGeneration {
        self.generation
    }

    #[must_use]
    pub const fn local(&self) -> &NodeStatus {
        &self.local
    }

    #[must_use]
    pub fn peers(&self) -> &HashMap<NodeId, PeerState> {
        &self.peers
    }

    #[must_use]
    pub const fn is_local(&self) -> bool {
        true
    }

    #[must_use]
    pub const fn is_authoritative(&self) -> bool {
        self.authoritative
    }

    #[must_use]
    pub const fn requires_consensus(&self) -> bool {
        false
    }
}
