use std::{sync::Arc, time::Duration};

use pg_kinetic::{
    core::{
        control::{
            ClusterViewGeneration, ControlEvent, ControlEventKind, NodeHeartbeat,
            NodeLifecycleSummary, NodeStatus, PeerHealth, PeerState,
        },
        runtime::{NodeId, ReadinessState, RuntimeLifecycleState},
    },
    proxy_runtime::control::{
        ClusterViewStore, HeartbeatObserver, HeartbeatPublisher, LocalControlEventSink,
        NodeRegistry,
    },
};

fn summary(
    lifecycle: RuntimeLifecycleState,
    readiness: ReadinessState,
    route_map_generation: u64,
    policy_generation: u64,
    overloaded: bool,
) -> NodeLifecycleSummary {
    NodeLifecycleSummary::new(
        lifecycle,
        readiness,
        ClusterViewGeneration::new(route_map_generation),
        ClusterViewGeneration::new(policy_generation),
        overloaded,
    )
}

fn status(node_id: &str, summary: NodeLifecycleSummary, health: PeerHealth) -> NodeStatus {
    NodeStatus::new(NodeId::new(node_id).expect("node id"), summary, health, node_id)
}

#[test]
fn node_identity_is_stable_for_process_lifetime() {
    let registry = NodeRegistry::new(NodeId::new("node-a").expect("node id"));

    assert_eq!(registry.node_identity(), registry.node_identity());
    assert_eq!(registry.local_node_id().as_str(), "node-a");
}

#[test]
fn node_heartbeat_includes_lifecycle_readiness_generations_and_overload_state() {
    let heartbeat = NodeHeartbeat::new(
        status(
            "node-b",
            summary(
                RuntimeLifecycleState::Ready,
                ReadinessState::Ready,
                17,
                23,
                true,
            ),
            PeerHealth::Overloaded,
        ),
        ClusterViewGeneration::new(9),
    );

    assert_eq!(heartbeat.node_id().as_str(), "node-b");
    assert_eq!(heartbeat.lifecycle_state(), RuntimeLifecycleState::Ready);
    assert_eq!(heartbeat.readiness(), ReadinessState::Ready);
    assert_eq!(heartbeat.route_map_generation().as_u64(), 17);
    assert_eq!(heartbeat.policy_generation().as_u64(), 23);
    assert!(heartbeat.overload_state());
    assert_eq!(heartbeat.status().health(), PeerHealth::Overloaded);
}

#[test]
fn stale_heartbeat_marks_peer_unknown() {
    let sink = LocalControlEventSink::new();
    let observer = HeartbeatObserver::new(
        ClusterViewStore::new(status(
            "local",
            summary(
                RuntimeLifecycleState::Ready,
                ReadinessState::Ready,
                1,
                1,
                false,
            ),
            PeerHealth::Healthy,
        )),
        Duration::from_secs(5),
        sink,
    );
    let peer_heartbeat = NodeHeartbeat::new(
        status(
            "peer-a",
            summary(
                RuntimeLifecycleState::Ready,
                ReadinessState::Ready,
                2,
                3,
                false,
            ),
            PeerHealth::Healthy,
        ),
        ClusterViewGeneration::new(4),
    );

    let peer = observer.observe(peer_heartbeat, Duration::from_secs(10));

    assert!(peer.is_unknown());
    assert_eq!(peer.health(), PeerHealth::Unknown);
    assert!(peer.status().metadata_is_redacted());
}

#[test]
fn cluster_view_is_local_and_non_authoritative() {
    let view = ClusterViewStore::new(status(
        "local",
        summary(
            RuntimeLifecycleState::Ready,
            ReadinessState::Ready,
            4,
            5,
            false,
        ),
        PeerHealth::Healthy,
    ))
    .snapshot();

    assert!(view.is_local());
    assert!(!view.is_authoritative());
    assert!(view.requires_consensus() == false);
}

#[test]
fn drain_request_can_be_represented_as_a_control_event() {
    let event = ControlEvent::drain_requested(
        NodeId::new("node-drain").expect("node id"),
        ClusterViewGeneration::new(12),
    );

    assert_eq!(event.kind(), ControlEventKind::DrainRequested);
    assert_eq!(event.node_id().as_str(), "node-drain");
}

#[test]
fn control_plane_primitives_do_not_require_consensus() {
    let event = ControlEvent::new(
        NodeId::new("node-c").expect("node id"),
        ClusterViewGeneration::new(1),
        ControlEventKind::ClusterViewUpdated,
    );
    let sink = LocalControlEventSink::new();

    assert!(!event.requires_consensus());
    assert!(!sink.requires_consensus());
}

#[test]
fn peer_metadata_is_redacted() {
    let peer = PeerState::from_heartbeat(
        &NodeHeartbeat::new(
            status(
                "peer-secret",
                summary(
                    RuntimeLifecycleState::Ready,
                    ReadinessState::Ready,
                    7,
                    8,
                    false,
                ),
                PeerHealth::Healthy,
            ),
            ClusterViewGeneration::new(6),
        ),
        false,
    );

    assert_eq!(peer.status().metadata(), "<redacted>");
    assert!(peer.status().metadata_is_redacted());
    assert_eq!(peer.health(), PeerHealth::Healthy);
}

#[test]
fn heartbeat_publisher_and_observer_use_local_control_events() {
    let registry = Arc::new(NodeRegistry::new(NodeId::new("node-local").expect("node id")));
    let sink = LocalControlEventSink::new();
    let publisher = HeartbeatPublisher::new(Arc::clone(&registry), sink.clone());
    let store = ClusterViewStore::new(status(
        "local",
        summary(
            RuntimeLifecycleState::Starting,
            ReadinessState::NotReady,
            0,
            0,
            false,
        ),
        PeerHealth::Healthy,
    ));
    let observer = HeartbeatObserver::new(store, Duration::from_secs(1), sink.clone());

    let heartbeat = publisher.publish(
        summary(
            RuntimeLifecycleState::Ready,
            ReadinessState::Ready,
            9,
            10,
            false,
        ),
        PeerHealth::Healthy,
    );
    let peer = observer.observe(heartbeat, Duration::from_millis(0));

    assert_eq!(registry.node_identity().as_str(), "node-local");
    assert_eq!(peer.health(), PeerHealth::Healthy);
    assert_eq!(sink.drain().len(), 2);
}
