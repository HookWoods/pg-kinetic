use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use pg_kinetic_core::{
    control::{
        ClusterView, ClusterViewGeneration, ControlEvent, ControlEventKind, NodeHeartbeat,
        NodeLifecycleSummary, NodeStatus, PeerHealth, PeerState,
    },
    runtime::NodeId,
};

#[derive(Clone, Debug)]
pub struct NodeRegistry {
    local_node_id: NodeId,
    static_peers: HashMap<NodeId, PeerState>,
}

impl NodeRegistry {
    #[must_use]
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            static_peers: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_static_peers(
        local_node_id: NodeId,
        peers: impl IntoIterator<Item = PeerState>,
    ) -> Self {
        let static_peers = peers
            .into_iter()
            .map(|peer| (peer.status().node_id().clone(), peer))
            .collect();
        Self {
            local_node_id,
            static_peers,
        }
    }

    #[must_use]
    pub const fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    #[must_use]
    pub fn node_identity(&self) -> NodeId {
        self.local_node_id.clone()
    }

    #[must_use]
    pub fn cluster_view(&self, local: NodeStatus) -> ClusterView {
        ClusterView::with_peers(local, self.static_peers.values().cloned())
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalControlEventSink {
    events: Arc<Mutex<VecDeque<ControlEvent>>>,
}

impl LocalControlEventSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&self, event: ControlEvent) {
        self.events
            .lock()
            .expect("control event sink lock")
            .push_back(event);
    }

    #[must_use]
    pub fn drain(&self) -> Vec<ControlEvent> {
        self.events
            .lock()
            .expect("control event sink lock")
            .drain(..)
            .collect()
    }

    #[must_use]
    pub const fn requires_consensus(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub struct HeartbeatPublisher {
    registry: Arc<NodeRegistry>,
    sink: LocalControlEventSink,
    generation: Arc<AtomicU64>,
}

impl HeartbeatPublisher {
    #[must_use]
    pub fn new(registry: Arc<NodeRegistry>, sink: LocalControlEventSink) -> Self {
        Self {
            registry,
            sink,
            generation: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn publish(&self, lifecycle: NodeLifecycleSummary, health: PeerHealth) -> NodeHeartbeat {
        let generation =
            ClusterViewGeneration::new(self.generation.fetch_add(1, Ordering::AcqRel) + 1);
        let heartbeat = NodeHeartbeat::new(
            NodeStatus::redacted(self.registry.node_identity(), lifecycle, health),
            generation,
        );
        self.sink.publish(ControlEvent::new(
            heartbeat.node_id().clone(),
            heartbeat.generation(),
            ControlEventKind::HeartbeatPublished,
        ));
        heartbeat
    }
}

#[derive(Clone, Debug)]
pub struct ClusterViewStore {
    inner: Arc<Mutex<ClusterViewStoreInner>>,
}

#[derive(Clone, Debug)]
struct ClusterViewStoreInner {
    generation: ClusterViewGeneration,
    view: ClusterView,
}

impl ClusterViewStore {
    #[must_use]
    pub fn new(local: NodeStatus) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClusterViewStoreInner {
                generation: ClusterViewGeneration::initial(),
                view: ClusterView::new(local),
            })),
        }
    }

    #[must_use]
    pub fn with_peers(local: NodeStatus, peers: impl IntoIterator<Item = PeerState>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClusterViewStoreInner {
                generation: ClusterViewGeneration::initial(),
                view: ClusterView::with_peers(local, peers),
            })),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ClusterView {
        self.inner
            .lock()
            .expect("cluster view store lock")
            .view
            .clone()
    }

    pub fn set_local(&self, local: NodeStatus) {
        let mut inner = self.inner.lock().expect("cluster view store lock");
        let peers = inner.view.peers().values().cloned().collect::<Vec<_>>();
        let generation = inner.generation.next();
        inner.generation = generation;
        inner.view = ClusterView::with_generation(
            local,
            peers
                .into_iter()
                .map(|peer| (peer.status().node_id().clone(), peer))
                .collect(),
            generation,
        );
    }

    pub fn observe_peer(&self, peer: PeerState) {
        let mut inner = self.inner.lock().expect("cluster view store lock");
        let mut peers = inner.view.peers().clone();
        peers.insert(peer.status().node_id().clone(), peer);
        let generation = inner.generation.next();
        inner.generation = generation;
        let local = inner.view.local().clone();
        inner.view = ClusterView::with_generation(local, peers, generation);
    }

    pub fn mark_peer_unknown(
        &self,
        node_id: &NodeId,
        heartbeat_generation: ClusterViewGeneration,
    ) -> bool {
        let mut inner = self.inner.lock().expect("cluster view store lock");
        let Some(existing) = inner.view.peers().get(node_id).cloned() else {
            return false;
        };
        let peer = PeerState::unknown(existing.status().clone(), heartbeat_generation);
        let mut peers = inner.view.peers().clone();
        peers.insert(node_id.clone(), peer);
        let generation = inner.generation.next();
        inner.generation = generation;
        let local = inner.view.local().clone();
        inner.view = ClusterView::with_generation(local, peers, generation);
        true
    }
}

#[derive(Clone, Debug)]
pub struct HeartbeatObserver {
    stale_after: Duration,
    store: ClusterViewStore,
    sink: LocalControlEventSink,
}

impl HeartbeatObserver {
    #[must_use]
    pub fn new(
        store: ClusterViewStore,
        stale_after: Duration,
        sink: LocalControlEventSink,
    ) -> Self {
        Self {
            stale_after,
            store,
            sink,
        }
    }

    #[must_use]
    pub fn observe(&self, heartbeat: NodeHeartbeat, age: Duration) -> PeerState {
        let stale = age >= self.stale_after;
        let peer = if stale {
            PeerState::unknown(heartbeat.status().clone(), heartbeat.generation())
        } else {
            PeerState::from_heartbeat(&heartbeat, false)
        };

        self.store.observe_peer(peer.clone());
        self.sink.publish(ControlEvent::new(
            heartbeat.node_id().clone(),
            heartbeat.generation(),
            if stale {
                ControlEventKind::PeerMarkedUnknown
            } else {
                ControlEventKind::PeerHeartbeatObserved
            },
        ));
        peer
    }

    #[must_use]
    pub fn mark_stale(
        &self,
        node_id: &NodeId,
        heartbeat_generation: ClusterViewGeneration,
    ) -> bool {
        let marked = self.store.mark_peer_unknown(node_id, heartbeat_generation);
        if marked {
            self.sink.publish(ControlEvent::new(
                node_id.clone(),
                heartbeat_generation,
                ControlEventKind::PeerMarkedUnknown,
            ));
        }
        marked
    }
}
