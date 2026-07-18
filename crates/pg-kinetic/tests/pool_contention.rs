use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use metrics::{Counter, Gauge, Histogram, Key, Metadata, Recorder};
use pg_kinetic_core::{
    route::{QueryClass, RouteKey},
    routing::BackendRole,
    sharding::{
        MultiShardPolicy, ShardId, ShardRoute, ShardRouteMap, ShardRouteReason, ShardScope,
        ShardStrategy, ShardTarget,
    },
};
use pg_kinetic_proxy::{
    config::TlsConfig,
    pool::{
        BackendPool, BackendPoolRef, CheckoutMode, ReplicaSelectionStrategy, ReplicaSelector,
        RoutePoolRegistry, RoutePools, ShardPoolCheckoutTarget, ShardPoolKey, ShardPools,
        ShardedPoolRegistry,
    },
    routing::{RoutingReason, RoutingTarget},
    sharding::ShardRouteMapStore,
};

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();

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

fn shard_route_map(shard: &str) -> ShardRouteMap {
    ShardRouteMap::new(
        ShardScope::global(),
        ShardStrategy::Hash,
        MultiShardPolicy::FirstMatch,
        vec![ShardRoute::new(
            ShardTarget::new(
                route_key(),
                BackendRole::Primary,
                ShardId::new(shard).expect("valid shard id"),
            ),
            ShardRouteReason::HashMatch,
        )],
    )
    .expect("valid shard route map")
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

fn install_metrics_recorder() -> Arc<TestRecorder> {
    METRICS_RECORDER
        .get_or_init(|| {
            let recorder = Arc::new(TestRecorder::default());
            metrics::set_global_recorder(recorder.clone()).expect("install metrics recorder");
            recorder
        })
        .clone()
}

#[derive(Debug, Default)]
struct TestRecorder {
    registrations: Mutex<HashMap<String, usize>>,
}

impl TestRecorder {
    fn clear(&self) {
        self.registrations.lock().expect("lock recorder").clear();
    }

    fn has_metric(&self, name: &str, labels: &[(&str, &str)]) -> bool {
        self.registrations
            .lock()
            .expect("lock recorder")
            .contains_key(&metric_signature(name, labels))
    }

    fn has_metric_stage(&self, name: &str, stage: &str) -> bool {
        let prefix = format!("{name}|stage={stage},");
        self.registrations
            .lock()
            .expect("lock recorder")
            .keys()
            .any(|signature| signature.starts_with(&prefix))
    }
}

impl Recorder for TestRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.record(key);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.record(key);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.record(key);
        Histogram::noop()
    }
}

impl TestRecorder {
    fn record(&self, key: &Key) {
        self.registrations
            .lock()
            .expect("lock recorder")
            .insert(metric_signature_from_key(key), 1);
    }
}

fn metric_signature_from_key(key: &Key) -> String {
    let labels = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}|{labels}", key.name())
}

fn metric_signature(name: &str, labels: &[(&str, &str)]) -> String {
    let labels = labels
        .iter()
        .map(|(label_key, label_value)| format!("{label_key}={label_value}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}|{labels}")
}

#[test]
fn route_pool_lookup_completes_for_all_concurrent_readers() {
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

    let completed = Arc::new(AtomicUsize::new(0));
    let readers = (0..16)
        .map(|_| {
            let registry = registry.clone();
            let route = route.clone();
            let completed = completed.clone();
            std::thread::spawn(move || {
                for _ in 0..2_000 {
                    assert!(registry.route_pools(&route).is_some());
                }
                completed.fetch_add(1, Ordering::Release);
            })
        })
        .collect::<Vec<_>>();

    for reader in readers {
        reader.join().expect("registry reader succeeds");
    }
    assert_eq!(completed.load(Ordering::Acquire), 16);
}

#[tokio::test]
async fn route_gate_registry_and_checkout_waits_are_recorded_separately() {
    let recorder = install_metrics_recorder();
    recorder.clear();

    let backend_addr = "127.0.0.1:1".parse().expect("valid socket address");
    let _error = test_pool(backend_addr)
        .checkout_primary(route_key())
        .await
        .expect_err("unreachable backend rejects checkout");

    assert!(recorder.has_metric(
        "pg_kinetic_pool_checkout_wait_ms",
        &[("stage", "route_gate_registry"), ("outcome", "ok")]
    ));
    assert!(recorder.has_metric_stage("pg_kinetic_pool_checkout_wait_ms", "checkout"));
}

#[tokio::test]
async fn reusable_checkout_times_out_without_opening_another_connection() {
    let (backend_addr, accepted) = backend_listener().await;
    let pool = BackendPool::new(
        backend_addr,
        TlsConfig::default(),
        1,
        1,
        2,
        2,
        Duration::from_millis(25),
        "DISCARD ALL",
    );
    let route = route_key();
    let held = pool
        .checkout_primary(route.clone())
        .await
        .expect("first checkout succeeds");
    wait_for_accepts(&accepted, 1).await;

    let started = Instant::now();
    let error = pool
        .checkout_primary_reusable(route)
        .await
        .expect_err("reusable checkout times out while the only backend is held");

    assert!(matches!(
        error,
        pg_kinetic_proxy::pool::PoolError::Backpressure(
            pg_kinetic_core::backpressure::BackpressureError::Timeout
        )
    ));
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(accepted.load(Ordering::Acquire), 1);
    drop(held);
}

#[test]
fn route_map_generation_lookup_tracks_successful_reload() {
    let store = ShardRouteMapStore::new(vec![shard_route_map("shard-a")]);
    assert_eq!(store.generation_id(), 0);

    let reload = store.reload(vec![shard_route_map("shard-b")], None, None);

    assert!(reload.success);
    assert_eq!(reload.route_map_generation_id, 1);
    assert_eq!(store.generation_id(), reload.route_map_generation_id);
    assert_eq!(
        store.route_maps()[0].routes()[0]
            .target()
            .shard_id()
            .as_str(),
        "shard-b"
    );
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
