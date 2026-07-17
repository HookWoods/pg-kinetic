use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use pg_kinetic_core::{
    route::{QueryClass, RouteKey},
    routing::BackendRole,
    sharding::ShardId,
};
use pg_kinetic_proxy::{
    config::TlsConfig,
    pool::{
        BackendPool, BackendPoolRef, CheckoutMode, ReplicaSelectionStrategy, ReplicaSelector,
        RoutePoolRegistry, RoutePools, ShardPoolCheckoutTarget, ShardPoolKey, ShardPools,
        ShardedPoolRegistry,
    },
    routing::{RoutingReason, RoutingTarget},
};

fn route_key() -> RouteKey {
    RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("contention"),
        None,
        QueryClass::Default,
    )
}

fn shard_id() -> ShardId {
    ShardId::new("shard-a").expect("valid shard id")
}

fn test_pool(addr: SocketAddr) -> Arc<BackendPool> {
    BackendPool::new(
        addr,
        TlsConfig::default(),
        4,
        16,
        4,
        16,
        Duration::from_millis(200),
        "DISCARD ALL",
    )
}

fn primary(addr: SocketAddr) -> BackendPoolRef {
    BackendPoolRef::primary(test_pool(addr))
}

fn replica(id: u64, addr: SocketAddr) -> BackendPoolRef {
    BackendPoolRef::replica(id, 1, test_pool(addr))
}

async fn backend_listener() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend listener");
    let address = listener.local_addr().expect("listener address");
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_probe = accepted.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept backend connection");
            accepted_probe.fetch_add(1, Ordering::Release);
            drop(stream);
        }
    });

    (address, accepted)
}

async fn wait_for_accepts(accepted: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while accepted.load(Ordering::Acquire) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("backend accepted expected connection");
}

#[test]
fn route_pool_lookup_is_stable_under_concurrent_readers() {
    let route = route_key();
    let registry = Arc::new(RoutePoolRegistry::new());
    let backend_addr = "127.0.0.1:1".parse().expect("valid socket address");
    registry.insert(
        route.clone(),
        RoutePools::new(
            primary(backend_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );

    let readers = (0..8)
        .map(|_| {
            let registry = registry.clone();
            let route = route.clone();
            std::thread::spawn(move || {
                for _ in 0..1_000 {
                    assert!(registry.route_pools(&route).is_some());
                }
            })
        })
        .collect::<Vec<_>>();

    for reader in readers {
        reader.join().expect("registry reader succeeds");
    }
}

#[test]
fn healthy_replica_selection_uses_atomic_waiting_hints() {
    let backend_addr = "127.0.0.1:1".parse().expect("valid socket address");
    let busy = replica(1, backend_addr);
    let available = replica(2, backend_addr);
    let unhealthy = replica(3, backend_addr);
    busy.set_waiting_hint(3);
    available.set_waiting_hint(1);
    unhealthy.set_waiting_hint(0);
    unhealthy.set_healthy(false);

    let selector = ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting);
    let replicas = [busy, available, unhealthy];
    let selected = selector
        .select(&replicas)
        .expect("a healthy replica is selected");

    assert_eq!(selected.id(), 2);
}

#[test]
fn shard_checkout_target_reuses_its_precomputed_pool_key() {
    let target = ShardPoolCheckoutTarget::new(
        route_key(),
        shard_id(),
        BackendRole::Primary,
        Some(RoutingReason::Off),
    );

    let key = target.key();
    assert_eq!(key.route(), target.route_key());
    assert_eq!(key.shard_id(), target.shard_id());
    assert!(std::ptr::eq(key, target.key()));
}

#[tokio::test]
async fn primary_only_route_checkout_stays_on_the_primary_pool() {
    let (primary_addr, primary_accepted) = backend_listener().await;
    let (replica_addr, replica_accepted) = backend_listener().await;
    let route = route_key();
    let registry = RoutePoolRegistry::new();
    registry.insert(
        route.clone(),
        RoutePools::new(
            primary(primary_addr),
            vec![replica(1, replica_addr)],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );

    let backend = registry
        .checkout_target(
            &route,
            &RoutingTarget::Primary {
                reason: RoutingReason::PrimaryOnlyMode,
            },
            CheckoutMode::AllowConnect,
        )
        .await
        .expect("primary checkout succeeds");
    wait_for_accepts(&primary_accepted, 1).await;
    assert_eq!(replica_accepted.load(Ordering::Acquire), 0);
    drop(backend);
}

#[tokio::test]
async fn sharded_checkout_uses_the_target_precomputed_key() {
    let (primary_addr, primary_accepted) = backend_listener().await;
    let route = route_key();
    let shard = shard_id();
    let registry = ShardedPoolRegistry::new();
    registry.insert(
        ShardPoolKey::new(route.clone(), shard.clone()),
        ShardPools::new(
            shard.clone(),
            primary(primary_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );
    let target = ShardPoolCheckoutTarget::new(route, shard, BackendRole::Primary, None);

    let backend = registry
        .checkout_target(&target, CheckoutMode::AllowConnect)
        .await
        .expect("sharded primary checkout succeeds");
    wait_for_accepts(&primary_accepted, 1).await;
    drop(backend);
}
