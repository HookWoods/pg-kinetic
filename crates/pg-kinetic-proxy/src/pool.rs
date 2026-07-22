use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, RwLock as StdRwLock,
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwapOption;
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use crate::routing::{RoutingReason, RoutingTarget};
use crate::{
    backend::Backend,
    config::{PoolLifecycleConfig, SocketConfig, TlsConfig},
    metrics::{self, RouteMetricHandles},
    snapshot::{PoolLifecycleSnapshot, PoolSnapshot, SnapshotStore},
};
use pg_kinetic_core::{
    backpressure::{BackpressureError, BackpressureGate, BackpressurePermit},
    route::{PoolKey, RouteKey},
    routing::BackendRole,
    sharding::ShardId,
};

#[derive(Debug)]
pub struct BackendPool {
    backend_addr: SocketAddr,
    tls: TlsConfig,
    socket: SocketConfig,
    reset_query: Arc<str>,
    gate: BackpressureGate,
    route_gates: StdRwLock<HashMap<PoolKey, RouteGateEntry>>,
    idle: Mutex<VecDeque<Backend>>,
    backend_lifecycle: StdMutex<HashMap<u64, BackendLifecycle>>,
    backend_global_permits: StdMutex<HashMap<u64, OwnedSemaphorePermit>>,
    backend_available: Notify,
    snapshot_store: ArcSwapOption<SnapshotStore>,
    health: Arc<AtomicBool>,
    active_backends: AtomicUsize,
    idle_backends: AtomicUsize,
    global_backend_slots: Option<Arc<Semaphore>>,
    global_backend_available: Option<Arc<Notify>>,
    max_backends: usize,
    max_waiters: usize,
    route_max_in_flight: usize,
    route_max_waiters: usize,
    checkout_timeout: Duration,
    lifecycle: PoolLifecycleConfig,
}

#[derive(Clone, Copy, Debug)]
struct BackendLifecycle {
    created_at: tokio::time::Instant,
    last_released_at: tokio::time::Instant,
}

#[derive(Clone, Debug)]
struct RouteGateEntry {
    gate: BackpressureGate,
    metrics: RouteMetricHandles,
}

#[derive(Debug)]
pub struct PooledBackend {
    backend: Option<Backend>,
    pool: Arc<BackendPool>,
    health: Arc<AtomicBool>,
    _permit: BackpressurePermit,
    route_key: RouteKey,
    route_gate: BackpressureGate,
    requires_startup: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("backend checkout rejected: {0}")]
    Backpressure(#[from] BackpressureError),

    #[error("backend connection failed: {0}")]
    Connect(#[source] anyhow::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckoutMode {
    AllowConnect,
    PreferConnect,
    ReuseOnly,
}

#[derive(Clone, Debug)]
pub struct BackendPoolRef {
    inner: Arc<BackendPoolRefInner>,
}

#[derive(Debug)]
struct BackendPoolRefInner {
    id: u64,
    role: BackendRole,
    weight: usize,
    healthy: Arc<AtomicBool>,
    waiting_hint: AtomicUsize,
    pool: Arc<BackendPool>,
}

#[derive(Clone, Debug)]
pub struct ReplicaSelector {
    strategy: ReplicaSelectionStrategy,
    cursor: Arc<AtomicUsize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaSelectionStrategy {
    LeastWaiting,
    WeightedRoundRobin,
}

#[derive(Clone, Debug)]
pub struct RoutePools {
    primary: BackendPoolRef,
    replicas: Vec<BackendPoolRef>,
    selector: ReplicaSelector,
}

#[derive(Debug, Default)]
pub struct RoutePoolRegistry {
    routes: StdRwLock<HashMap<PoolKey, RoutePools>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ShardPoolKey {
    route: RouteKey,
    shard_id: ShardId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPoolCheckoutTarget {
    key: ShardPoolKey,
    target_role: BackendRole,
    fallback_reason: Option<RoutingReason>,
}

#[derive(Clone, Debug)]
pub struct ShardPools {
    shard_id: ShardId,
    pools: RoutePools,
}

#[derive(Debug, Default)]
pub struct ShardedPoolRegistry {
    routes: StdRwLock<HashMap<ShardPoolKey, ShardPools>>,
}

impl BackendPoolRef {
    #[must_use]
    pub fn primary(pool: Arc<BackendPool>) -> Self {
        Self::new(0, BackendRole::Primary, 1, pool)
    }

    #[must_use]
    pub fn replica(id: u64, weight: usize, pool: Arc<BackendPool>) -> Self {
        Self::new(id, BackendRole::Replica, weight, pool)
    }

    #[must_use]
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    #[must_use]
    pub fn role(&self) -> BackendRole {
        self.inner.role
    }

    #[must_use]
    pub fn weight(&self) -> usize {
        self.inner.weight
    }

    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.inner.healthy.load(Ordering::Acquire)
    }

    pub fn set_healthy(&self, healthy: bool) {
        self.inner.healthy.store(healthy, Ordering::Release);
    }

    #[must_use]
    pub fn waiting_hint(&self) -> usize {
        self.inner.waiting_hint.load(Ordering::Acquire)
    }

    pub fn set_waiting_hint(&self, waiting_hint: usize) {
        self.inner
            .waiting_hint
            .store(waiting_hint, Ordering::Release);
    }

    pub fn attach_snapshot_store(&self, snapshot_store: SnapshotStore) {
        self.inner.pool.attach_snapshot_store(snapshot_store);
    }

    #[must_use]
    pub fn reset_query(&self) -> &str {
        self.inner.pool.reset_query()
    }

    #[must_use]
    pub fn backend_addr(&self) -> SocketAddr {
        self.inner.pool.backend_addr
    }

    #[must_use]
    pub fn backend_connection_settings(&self) -> (TlsConfig, SocketConfig) {
        (self.inner.pool.tls.clone(), self.inner.pool.socket.clone())
    }

    #[must_use]
    pub fn idle_backends(&self) -> usize {
        self.inner.pool.idle_backends.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn max_backends(&self) -> usize {
        self.inner.pool.max_backends()
    }

    #[must_use]
    pub fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            configured_backends: self.inner.pool.max_backends(),
            active_backends: self.inner.pool.active_count(),
            idle_backends: self.inner.pool.idle_count(),
            waiting_clients: self.inner.pool.gate.waiting(),
        }
    }

    fn new(id: u64, role: BackendRole, weight: usize, pool: Arc<BackendPool>) -> Self {
        Self {
            inner: Arc::new(BackendPoolRefInner {
                id,
                role,
                weight: weight.max(1),
                healthy: Arc::clone(&pool.health),
                waiting_hint: AtomicUsize::new(0),
                pool,
            }),
        }
    }

    async fn checkout(
        &self,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        match mode {
            CheckoutMode::AllowConnect => self.inner.pool.checkout_primary(route).await,
            CheckoutMode::PreferConnect => {
                self.inner.pool.checkout_primary_prefer_connect(route).await
            }
            CheckoutMode::ReuseOnly => self.inner.pool.checkout_primary_reusable(route).await,
        }
    }
}

impl ReplicaSelector {
    #[must_use]
    pub fn new(strategy: ReplicaSelectionStrategy) -> Self {
        Self {
            strategy,
            cursor: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[must_use]
    pub const fn strategy(&self) -> ReplicaSelectionStrategy {
        self.strategy
    }

    pub fn select<'a>(&self, replicas: &'a [BackendPoolRef]) -> Option<&'a BackendPoolRef> {
        let healthy: Vec<&BackendPoolRef> = replicas
            .iter()
            .filter(|replica| replica.is_healthy())
            .collect();
        if healthy.is_empty() {
            return None;
        }

        match self.strategy {
            ReplicaSelectionStrategy::LeastWaiting => healthy
                .into_iter()
                .min_by_key(|replica| (replica.waiting_hint(), replica.id())),
            ReplicaSelectionStrategy::WeightedRoundRobin => {
                let total_weight = healthy
                    .iter()
                    .map(|replica| replica.weight())
                    .sum::<usize>();
                if total_weight == 0 {
                    return None;
                }

                let cursor = self.cursor.fetch_add(1, Ordering::AcqRel) % total_weight;
                let mut offset = 0;
                for replica in healthy {
                    offset += replica.weight();
                    if cursor < offset {
                        return Some(replica);
                    }
                }

                None
            }
        }
    }
}

impl RoutePools {
    #[must_use]
    pub fn new(
        primary: BackendPoolRef,
        replicas: Vec<BackendPoolRef>,
        selector: ReplicaSelector,
    ) -> Self {
        assert_eq!(primary.role(), BackendRole::Primary);
        assert!(replicas
            .iter()
            .all(|replica| replica.role() == BackendRole::Replica));
        Self {
            primary,
            replicas,
            selector,
        }
    }

    #[must_use]
    pub fn primary(&self) -> &BackendPoolRef {
        &self.primary
    }

    #[must_use]
    pub fn replicas(&self) -> &[BackendPoolRef] {
        &self.replicas
    }

    #[must_use]
    pub fn replica_by_id(&self, replica_id: u64) -> Option<&BackendPoolRef> {
        self.replicas
            .iter()
            .find(|replica| replica.id() == replica_id)
    }

    #[must_use]
    pub fn selector(&self) -> &ReplicaSelector {
        &self.selector
    }

    #[must_use]
    pub fn pool_for_target(&self, target: &RoutingTarget) -> Option<&BackendPoolRef> {
        match target {
            RoutingTarget::Primary { .. } => Some(&self.primary),
            RoutingTarget::Replica { candidate, .. } => self.replica_by_id(candidate.replica_id),
            RoutingTarget::Wait { .. } | RoutingTarget::Reject { .. } => None,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> PoolSnapshot {
        let mut snapshot = self.primary.snapshot();
        for replica in &self.replicas {
            let replica = replica.snapshot();
            snapshot.configured_backends += replica.configured_backends;
            snapshot.active_backends += replica.active_backends;
            snapshot.idle_backends += replica.idle_backends;
            snapshot.waiting_clients += replica.waiting_clients;
        }
        snapshot
    }

    pub async fn checkout_primary(
        &self,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        self.primary.checkout(route, mode).await
    }

    pub async fn checkout_any_replica(
        &self,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let replica = self
            .selector
            .select(&self.replicas)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        replica.checkout(route, mode).await
    }

    pub async fn checkout_replica_by_id(
        &self,
        route: RouteKey,
        replica_id: u64,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let replica = self
            .replica_by_id(replica_id)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;

        if !replica.is_healthy() {
            return Err(PoolError::Backpressure(BackpressureError::Closed));
        }

        replica.checkout(route, mode).await
    }

    pub async fn checkout_replica_or_primary(
        &self,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        if let Some(replica) = self.selector.select(&self.replicas) {
            match replica.checkout(route.clone(), mode).await {
                Ok(backend) => return Ok(backend),
                Err(replica_error) => {
                    return match self.primary.checkout(route, mode).await {
                        Ok(backend) => Ok(backend),
                        Err(_) => Err(replica_error),
                    };
                }
            }
        }

        self.primary.checkout(route, mode).await
    }

    pub async fn checkout_target(
        &self,
        route: RouteKey,
        target: &RoutingTarget,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        match target {
            RoutingTarget::Primary { .. } => self.checkout_primary(route, mode).await,
            RoutingTarget::Replica { candidate, .. } => {
                self.checkout_replica_by_id(route, candidate.replica_id, mode)
                    .await
            }
            RoutingTarget::Wait { .. } | RoutingTarget::Reject { .. } => {
                Err(PoolError::Backpressure(BackpressureError::Closed))
            }
        }
    }

    pub async fn retire_idle_backends(&self) {
        self.primary.inner.pool.retire_idle_backends().await;
        for replica in &self.replicas {
            replica.inner.pool.retire_idle_backends().await;
        }
    }
}

impl RoutePoolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            routes: StdRwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, route: RouteKey, pools: RoutePools) {
        self.routes
            .write()
            .expect("route registry poisoned")
            .insert(route.selection_key(), pools);
    }

    #[must_use]
    pub fn route_pools(&self, route: &RouteKey) -> Option<RoutePools> {
        self.routes
            .read()
            .expect("route registry poisoned")
            .get(&route.selection_key())
            .cloned()
    }

    #[must_use]
    pub fn snapshots(&self) -> Vec<(PoolKey, PoolSnapshot)> {
        let mut snapshots = self
            .routes
            .read()
            .expect("route registry poisoned")
            .iter()
            .map(|(key, pools)| (key.clone(), pools.snapshot()))
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.0.metric_label().cmp(&right.0.metric_label()));
        snapshots
    }

    pub async fn checkout_primary(
        &self,
        route: &RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let pools = self
            .route_pools(route)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools.checkout_primary(route.clone(), mode).await
    }

    pub async fn checkout_any_replica(
        &self,
        route: &RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let pools = self
            .route_pools(route)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools.checkout_any_replica(route.clone(), mode).await
    }

    pub async fn checkout_replica_by_id(
        &self,
        route: &RouteKey,
        replica_id: u64,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let pools = self
            .route_pools(route)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools
            .checkout_replica_by_id(route.clone(), replica_id, mode)
            .await
    }

    pub async fn checkout_replica_or_primary(
        &self,
        route: &RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let pools = self
            .route_pools(route)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools.checkout_replica_or_primary(route.clone(), mode).await
    }

    pub async fn checkout_target(
        &self,
        route: &RouteKey,
        target: &RoutingTarget,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let pools = self
            .route_pools(route)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools.checkout_target(route.clone(), target, mode).await
    }
}

impl ShardPoolKey {
    #[must_use]
    pub fn new(route: RouteKey, shard_id: ShardId) -> Self {
        Self { route, shard_id }
    }

    #[must_use]
    pub fn route(&self) -> &RouteKey {
        &self.route
    }

    #[must_use]
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    #[must_use]
    pub fn metric_label(&self) -> String {
        self.shard_id.as_str().to_owned()
    }
}

impl ShardPoolCheckoutTarget {
    #[must_use]
    pub fn new(
        route: RouteKey,
        shard_id: ShardId,
        target_role: BackendRole,
        fallback_reason: Option<RoutingReason>,
    ) -> Self {
        let key = ShardPoolKey::new(route, shard_id);
        Self {
            key,
            target_role,
            fallback_reason,
        }
    }

    #[must_use]
    pub fn route_key(&self) -> &RouteKey {
        self.key.route()
    }

    #[must_use]
    pub fn shard_id(&self) -> &ShardId {
        self.key.shard_id()
    }

    #[must_use]
    pub const fn target_role(&self) -> BackendRole {
        self.target_role
    }

    #[must_use]
    pub const fn fallback_reason(&self) -> Option<RoutingReason> {
        self.fallback_reason
    }

    #[must_use]
    pub fn key(&self) -> &ShardPoolKey {
        &self.key
    }
}

impl ShardPools {
    #[must_use]
    pub fn new(
        shard_id: ShardId,
        primary: BackendPoolRef,
        replicas: Vec<BackendPoolRef>,
        selector: ReplicaSelector,
    ) -> Self {
        Self {
            shard_id,
            pools: RoutePools::new(primary, replicas, selector),
        }
    }

    #[must_use]
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    #[must_use]
    pub fn metric_label(&self) -> String {
        self.shard_id.as_str().to_owned()
    }

    #[must_use]
    pub fn primary(&self) -> &BackendPoolRef {
        self.pools.primary()
    }

    #[must_use]
    pub fn replicas(&self) -> &[BackendPoolRef] {
        self.pools.replicas()
    }

    #[must_use]
    pub fn replica_by_id(&self, replica_id: u64) -> Option<&BackendPoolRef> {
        self.pools.replica_by_id(replica_id)
    }

    #[must_use]
    pub fn selector(&self) -> &ReplicaSelector {
        self.pools.selector()
    }

    pub async fn checkout_primary(
        &self,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        self.pools.checkout_primary(route, mode).await
    }

    pub async fn checkout_any_replica(
        &self,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        self.pools.checkout_any_replica(route, mode).await
    }

    pub async fn checkout_replica_or_primary(
        &self,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        self.pools.checkout_replica_or_primary(route, mode).await
    }

    pub async fn checkout_target(
        &self,
        target: &ShardPoolCheckoutTarget,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        if target.shard_id() != self.shard_id() {
            return Err(PoolError::Backpressure(BackpressureError::Closed));
        }

        match target.target_role() {
            BackendRole::Primary => {
                self.checkout_primary(target.route_key().clone(), mode)
                    .await
            }
            BackendRole::Replica => {
                self.checkout_any_replica(target.route_key().clone(), mode)
                    .await
            }
            BackendRole::Unknown => Err(PoolError::Backpressure(BackpressureError::Closed)),
        }
    }
}

impl ShardedPoolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            routes: StdRwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, key: ShardPoolKey, pools: ShardPools) {
        assert_eq!(key.shard_id(), pools.shard_id());
        self.routes
            .write()
            .expect("sharded route registry poisoned")
            .insert(key, pools);
    }

    #[must_use]
    pub fn shard_pools(&self, key: &ShardPoolKey) -> Option<ShardPools> {
        self.routes
            .read()
            .expect("sharded route registry poisoned")
            .get(key)
            .cloned()
    }

    pub async fn checkout_target(
        &self,
        target: &ShardPoolCheckoutTarget,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let pools = self
            .shard_pools(target.key())
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools.checkout_target(target, mode).await
    }

    pub async fn checkout_primary(
        &self,
        route: &RouteKey,
        shard_id: &ShardId,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let key = ShardPoolKey::new(route.clone(), shard_id.clone());
        let pools = self
            .shard_pools(&key)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools.checkout_primary(route.clone(), mode).await
    }

    pub async fn checkout_any_replica(
        &self,
        route: &RouteKey,
        shard_id: &ShardId,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let key = ShardPoolKey::new(route.clone(), shard_id.clone());
        let pools = self
            .shard_pools(&key)
            .ok_or(PoolError::Backpressure(BackpressureError::Closed))?;
        pools.checkout_any_replica(route.clone(), mode).await
    }
}

impl BackendPool {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        max_backends: usize,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: Duration,
        reset_query: impl Into<Arc<str>>,
    ) -> Arc<Self> {
        Self::new_with_socket(
            backend_addr,
            tls,
            SocketConfig::default(),
            max_backends,
            max_waiters,
            route_max_in_flight,
            route_max_waiters,
            checkout_timeout,
            reset_query,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_socket(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
        max_backends: usize,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: Duration,
        reset_query: impl Into<Arc<str>>,
    ) -> Arc<Self> {
        let mut lifecycle = PoolLifecycleConfig::default();
        lifecycle.max_size = max_backends;
        Self::new_with_socket_and_lifecycle(
            backend_addr,
            tls,
            socket,
            max_waiters,
            route_max_in_flight,
            route_max_waiters,
            checkout_timeout,
            reset_query,
            lifecycle,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_socket_and_lifecycle(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: Duration,
        reset_query: impl Into<Arc<str>>,
        lifecycle: PoolLifecycleConfig,
    ) -> Arc<Self> {
        Self::new_with_socket_lifecycle_and_global_limit(
            backend_addr,
            tls,
            socket,
            max_waiters,
            route_max_in_flight,
            route_max_waiters,
            checkout_timeout,
            reset_query,
            lifecycle,
            None,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_socket_lifecycle_and_global_limit(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: Duration,
        reset_query: impl Into<Arc<str>>,
        lifecycle: PoolLifecycleConfig,
        global_backend_slots: Option<Arc<Semaphore>>,
    ) -> Arc<Self> {
        Self::new_with_socket_lifecycle_and_global_limit_and_notify(
            backend_addr,
            tls,
            socket,
            max_waiters,
            route_max_in_flight,
            route_max_waiters,
            checkout_timeout,
            reset_query,
            lifecycle,
            global_backend_slots,
            None,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_socket_lifecycle_and_global_limit_and_notify(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: Duration,
        reset_query: impl Into<Arc<str>>,
        lifecycle: PoolLifecycleConfig,
        global_backend_slots: Option<Arc<Semaphore>>,
        global_backend_available: Option<Arc<Notify>>,
    ) -> Arc<Self> {
        assert!(
            lifecycle.validate().is_ok(),
            "invalid pool lifecycle config"
        );
        Arc::new(Self {
            backend_addr,
            tls,
            socket,
            reset_query: reset_query.into(),
            gate: BackpressureGate::new(lifecycle.max_size, max_waiters),
            route_gates: StdRwLock::new(HashMap::new()),
            idle: Mutex::new(VecDeque::new()),
            backend_lifecycle: StdMutex::new(HashMap::new()),
            backend_global_permits: StdMutex::new(HashMap::new()),
            backend_available: Notify::new(),
            snapshot_store: ArcSwapOption::empty(),
            health: Arc::new(AtomicBool::new(true)),
            active_backends: AtomicUsize::new(0),
            idle_backends: AtomicUsize::new(0),
            global_backend_slots,
            global_backend_available,
            max_backends: lifecycle.max_size,
            max_waiters,
            route_max_in_flight,
            route_max_waiters,
            checkout_timeout,
            lifecycle,
        })
    }

    pub fn attach_snapshot_store(&self, snapshot_store: SnapshotStore) {
        self.snapshot_store
            .store(Some(Arc::new(snapshot_store.clone())));

        if let Ok(mut idle_backends) = self.idle.try_lock() {
            for backend in idle_backends.iter_mut() {
                backend.attach_snapshot_store(snapshot_store.clone());
            }
        }

        self.sync_pool_snapshot();
    }

    pub async fn checkout(self: &Arc<Self>, route: RouteKey) -> Result<PooledBackend, PoolError> {
        self.checkout_primary(route).await
    }

    pub async fn checkout_primary(
        self: &Arc<Self>,
        route: RouteKey,
    ) -> Result<PooledBackend, PoolError> {
        self.checkout_with_mode(route, CheckoutMode::AllowConnect)
            .await
    }

    pub async fn checkout_reusable(
        self: &Arc<Self>,
        route: RouteKey,
    ) -> Result<PooledBackend, PoolError> {
        self.checkout_primary_reusable(route).await
    }

    pub async fn checkout_primary_reusable(
        self: &Arc<Self>,
        route: RouteKey,
    ) -> Result<PooledBackend, PoolError> {
        self.checkout_with_mode(route, CheckoutMode::ReuseOnly)
            .await
    }

    pub async fn checkout_primary_prefer_connect(
        self: &Arc<Self>,
        route: RouteKey,
    ) -> Result<PooledBackend, PoolError> {
        self.checkout_with_mode(route, CheckoutMode::PreferConnect)
            .await
    }

    async fn checkout_with_mode(
        self: &Arc<Self>,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let route_gate_started = Instant::now();
        let route_gate = self.route_gate(&route);
        let route_gate_wait_ms = route_gate_started.elapsed().as_secs_f64() * 1_000.0;
        self.with_snapshot_store(|snapshot_store| {
            snapshot_store.record_pool_checkout_lock_wait_ms(route_gate_wait_ms);
        });
        metrics::record_pool_checkout(route_gate_wait_ms, "route_gate_registry", "ok");
        let started = Instant::now();
        let route_gate_metrics = route_gate.metrics.clone();
        let route_gate_gate = route_gate.gate.clone();
        let route_gate_gate_for_timeout = route_gate_gate.clone();
        let checkout_route = route.clone();
        let checkout = async move {
            let route_permit = match route_gate_gate.checkout(self.checkout_timeout).await {
                Ok(permit) => permit,
                Err(error) => {
                    metrics::increment_backpressure_event(
                        &checkout_route,
                        backpressure_outcome(error),
                    );
                    metrics::record_route_wait(
                        &checkout_route,
                        started.elapsed().as_secs_f64() * 1000.0,
                        backpressure_outcome(error),
                    );
                    self.record_backpressure_counts(&checkout_route, &route_gate_gate);
                    return Err(PoolError::Backpressure(error));
                }
            };
            let permit = match self.gate.checkout(self.checkout_timeout).await {
                Ok(permit) => permit,
                Err(error) => {
                    metrics::increment_backpressure_event(
                        &checkout_route,
                        backpressure_outcome(error),
                    );
                    metrics::record_route_wait(
                        &checkout_route,
                        started.elapsed().as_secs_f64() * 1000.0,
                        backpressure_outcome(error),
                    );
                    self.record_backpressure_counts(&checkout_route, &route_gate_gate);
                    return Err(PoolError::Backpressure(error));
                }
            };

            let wait_ms = started.elapsed().as_secs_f64() * 1000.0;
            route_gate_metrics.route_wait_ok.record(wait_ms);
            route_gate_metrics
                .in_flight
                .set(route_gate_gate.in_flight() as f64);
            route_gate_metrics
                .waiting
                .set(route_gate_gate.waiting() as f64);
            self.record_backpressure_counts(&checkout_route, &route_gate_gate);

            if mode == CheckoutMode::PreferConnect {
                if let Some(global_permit) = self.reserve_backend_slot() {
                    match self.connect_reserved_backend(global_permit).await {
                        Ok(mut backend) => {
                            self.attach_backend_snapshot_store(&mut backend);
                            backend.mark_checked_out(Some(checkout_route.clone()));
                            self.sync_pool_snapshot();
                            return Ok(PooledBackend {
                                backend: Some(backend),
                                pool: self.clone(),
                                health: Arc::clone(&self.health),
                                _permit: BackpressurePermit::join(route_permit, permit),
                                route_key: checkout_route.clone(),
                                route_gate: route_gate_gate.clone(),
                                requires_startup: true,
                            });
                        }
                        Err(PoolError::Connect(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
            }

            let idle_backend = match self.try_checkout_idle_backend() {
                Some(backend) => Some(backend),
                None if self.idle_backends.load(Ordering::Acquire) > 0 => {
                    self.checkout_idle_backend(false).await
                }
                None => None,
            };
            if let Some(mut backend) = idle_backend {
                self.attach_backend_snapshot_store(&mut backend);
                backend.mark_checked_out(Some(checkout_route.clone()));
                self.sync_pool_snapshot();
                return Ok(PooledBackend {
                    backend: Some(backend),
                    pool: self.clone(),
                    health: Arc::clone(&self.health),
                    _permit: BackpressurePermit::join(route_permit, permit),
                    route_key: checkout_route.clone(),
                    route_gate: route_gate_gate.clone(),
                    requires_startup: false,
                });
            }

            if mode == CheckoutMode::AllowConnect {
                if let Some(global_permit) = self.reserve_backend_slot() {
                    let mut backend = self.connect_reserved_backend(global_permit).await?;
                    self.attach_backend_snapshot_store(&mut backend);
                    backend.mark_checked_out(Some(checkout_route.clone()));
                    self.sync_pool_snapshot();

                    return Ok(PooledBackend {
                        backend: Some(backend),
                        pool: self.clone(),
                        health: Arc::clone(&self.health),
                        _permit: BackpressurePermit::join(route_permit, permit),
                        route_key: checkout_route.clone(),
                        route_gate: route_gate_gate.clone(),
                        requires_startup: true,
                    });
                }
            }

            let mut backend = loop {
                if let Some(backend) = self.checkout_idle_backend(false).await {
                    break backend;
                }

                if mode == CheckoutMode::AllowConnect {
                    if let Some(global_permit) = self.reserve_backend_slot() {
                        break self.connect_reserved_backend(global_permit).await?;
                    }
                }

                if let Some(backend) = self.checkout_idle_backend(true).await {
                    break backend;
                }
            };
            self.attach_backend_snapshot_store(&mut backend);
            backend.mark_checked_out(Some(checkout_route.clone()));
            self.sync_pool_snapshot();

            Ok(PooledBackend {
                backend: Some(backend),
                pool: self.clone(),
                health: Arc::clone(&self.health),
                _permit: BackpressurePermit::join(route_permit, permit),
                route_key: checkout_route.clone(),
                route_gate: route_gate_gate.clone(),
                requires_startup: false,
            })
        };

        let result = match timeout(self.checkout_timeout, checkout).await {
            Ok(result) => result,
            Err(_) => {
                metrics::increment_backpressure_event(&route, "timeout");
                metrics::record_route_wait(
                    &route,
                    started.elapsed().as_secs_f64() * 1000.0,
                    "timeout",
                );
                self.record_backpressure_counts(&route, &route_gate_gate_for_timeout);
                Err(PoolError::Backpressure(BackpressureError::Timeout))
            }
        };
        metrics::record_pool_checkout(
            started.elapsed().as_secs_f64() * 1_000.0,
            "checkout",
            pool_checkout_outcome(&result),
        );
        result
    }

    async fn checkout_idle_backend(&self, wait_for_backend: bool) -> Option<Backend> {
        loop {
            let notified = self.backend_available.notified();
            if let Some(backend) = self.try_checkout_idle_backend() {
                return Some(backend);
            }
            if !wait_for_backend {
                return None;
            }
            if let Some(global_backend_available) = &self.global_backend_available {
                tokio::select! {
                    _ = notified => {}
                    _ = global_backend_available.notified() => return None,
                }
            } else {
                notified.await;
            }
        }
    }

    fn try_checkout_idle_backend(&self) -> Option<Backend> {
        let mut idle_backends = self.idle.try_lock().ok()?;
        let backend = idle_backends.pop_front()?;
        self.idle_backends.fetch_sub(1, Ordering::AcqRel);
        Some(backend)
    }

    fn reserve_backend_slot(&self) -> Option<Option<OwnedSemaphorePermit>> {
        let mut active_backends = self.active_backends.load(Ordering::Acquire);
        loop {
            if active_backends >= self.max_backends {
                return None;
            }
            match self.active_backends.compare_exchange_weak(
                active_backends,
                active_backends + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => active_backends = observed,
            }
        }

        let global_permit = match &self.global_backend_slots {
            Some(slots) => match Arc::clone(slots).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    self.release_reserved_backend_slot(None);
                    return None;
                }
            },
            None => None,
        };

        Some(global_permit)
    }

    async fn connect_reserved_backend(
        &self,
        global_permit: Option<OwnedSemaphorePermit>,
    ) -> Result<Backend, PoolError> {
        match Backend::connect_with_socket(self.backend_addr, &self.tls, &self.socket).await {
            Ok(backend) => {
                self.register_backend(&backend, global_permit);
                Ok(backend)
            }
            Err(error) => {
                self.release_reserved_backend_slot(global_permit);
                Err(PoolError::Connect(error))
            }
        }
    }

    #[must_use]
    pub const fn max_backends(&self) -> usize {
        self.max_backends
    }

    #[must_use]
    pub const fn lifecycle_config(&self) -> &PoolLifecycleConfig {
        &self.lifecycle
    }

    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active_backends.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn idle_count(&self) -> usize {
        self.idle_backends.load(Ordering::Acquire)
    }

    pub async fn reap_idle(&self) {
        let now = tokio::time::Instant::now();
        let mut idle = self.idle.lock().await;
        let mut retained = VecDeque::with_capacity(idle.len());
        let mut evicted = Vec::new();
        while let Some(backend) = idle.pop_front() {
            let lifecycle = self
                .backend_lifecycle
                .lock()
                .expect("backend lifecycle poisoned")
                .get(&backend.id())
                .copied();
            let eviction_reason = lifecycle.and_then(|lifecycle| {
                (self.lifecycle.idle_timeout != Duration::ZERO
                    && now.duration_since(lifecycle.last_released_at)
                        >= self.lifecycle.idle_timeout)
                    .then_some("idle_timeout")
                    .or_else(|| {
                        (self.lifecycle.max_lifetime != Duration::ZERO
                            && now.duration_since(lifecycle.created_at)
                                >= self.lifecycle.max_lifetime)
                            .then_some("max_lifetime")
                    })
            });
            if let Some(reason) = eviction_reason {
                if retained.len() + idle.len() >= self.lifecycle.min_idle {
                    evicted.push((backend, reason));
                } else {
                    retained.push_back(backend);
                }
            } else {
                retained.push_back(backend);
            }
        }
        *idle = retained;
        drop(idle);

        for (backend, reason) in evicted {
            backend.mark_discarded();
            self.backend_lifecycle
                .lock()
                .expect("backend lifecycle poisoned")
                .remove(&backend.id());
            self.backend_global_permits
                .lock()
                .expect("backend global permits poisoned")
                .remove(&backend.id());
            self.notify_global_backend_available();
            self.active_backends.fetch_sub(1, Ordering::AcqRel);
            metrics::record_pool_eviction(reason);
        }
        self.idle_backends
            .store(self.idle.lock().await.len(), Ordering::Release);
        self.backend_available.notify_waiters();
        self.sync_pool_snapshot();
    }

    pub async fn retire_idle_backends(&self) {
        let mut idle = self.idle.lock().await;
        let retired: Vec<_> = idle.drain(..).collect();
        drop(idle);

        for backend in retired {
            backend.mark_discarded();
            self.backend_lifecycle
                .lock()
                .expect("backend lifecycle poisoned")
                .remove(&backend.id());
            self.backend_global_permits
                .lock()
                .expect("backend global permits poisoned")
                .remove(&backend.id());
            self.notify_global_backend_available();
            self.active_backends.fetch_sub(1, Ordering::AcqRel);
            metrics::record_pool_eviction("reload");
        }

        self.idle_backends.store(0, Ordering::Release);
        self.backend_available.notify_waiters();
        self.sync_pool_snapshot();
    }

    pub fn start_reaper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let pool = Arc::clone(self);
        let interval = [pool.lifecycle.idle_timeout, pool.lifecycle.max_lifetime]
            .into_iter()
            .filter(|duration| *duration != Duration::ZERO)
            .min()
            .unwrap_or(Duration::from_secs(1))
            .max(Duration::from_millis(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                pool.reap_idle().await;
            }
        })
    }

    #[must_use]
    pub const fn max_waiters(&self) -> usize {
        self.max_waiters
    }

    #[must_use]
    pub const fn route_max_in_flight(&self) -> usize {
        self.route_max_in_flight
    }

    #[must_use]
    pub const fn route_max_waiters(&self) -> usize {
        self.route_max_waiters
    }

    #[must_use]
    pub fn reset_query(&self) -> &str {
        &self.reset_query
    }

    fn route_gate(&self, route: &RouteKey) -> RouteGateEntry {
        let pool_key = route.pool_key();
        if let Some(route_gate) = self
            .route_gates
            .read()
            .expect("route gates poisoned")
            .get(&pool_key)
            .cloned()
        {
            return route_gate;
        }

        let mut route_gates = self.route_gates.write().expect("route gates poisoned");
        route_gates
            .entry(pool_key)
            .or_insert_with(|| RouteGateEntry {
                gate: BackpressureGate::new(self.route_max_in_flight, self.route_max_waiters),
                metrics: RouteMetricHandles::resolve(route),
            })
            .clone()
    }

    pub fn with_snapshot_store(&self, f: impl FnOnce(&SnapshotStore)) {
        if let Some(snapshot_store) = self.snapshot_store.load_full() {
            f(snapshot_store.as_ref());
        }
    }

    fn attach_backend_snapshot_store(&self, backend: &mut Backend) {
        self.with_snapshot_store(|snapshot_store| {
            backend.attach_snapshot_store(snapshot_store.clone());
        });
    }

    fn sync_pool_snapshot(&self) {
        let active_backends = self.active_backends.load(Ordering::Acquire);
        let idle_backends = self.idle_backends.load(Ordering::Acquire);
        metrics::record_pool_connections(active_backends, idle_backends);
        self.with_snapshot_store(|snapshot_store| {
            metrics::record_pool_snapshot(
                snapshot_store,
                PoolSnapshot {
                    configured_backends: self.max_backends,
                    active_backends,
                    idle_backends,
                    waiting_clients: self.gate.waiting(),
                },
            );
            snapshot_store.set_pool_lifecycle_snapshot(PoolLifecycleSnapshot {
                max_size: self.lifecycle.max_size,
                min_idle: self.lifecycle.min_idle,
                idle_timeout: self.lifecycle.idle_timeout,
                max_lifetime: self.lifecycle.max_lifetime,
                active_backends,
                idle_backends,
            });
        });
    }

    fn register_backend(&self, backend: &Backend, global_permit: Option<OwnedSemaphorePermit>) {
        let now = tokio::time::Instant::now();
        self.backend_lifecycle
            .lock()
            .expect("backend lifecycle poisoned")
            .insert(
                backend.id(),
                BackendLifecycle {
                    created_at: now,
                    last_released_at: now,
                },
            );
        if let Some(permit) = global_permit {
            self.backend_global_permits
                .lock()
                .expect("backend global permits poisoned")
                .insert(backend.id(), permit);
        }
    }

    fn record_backpressure_counts(&self, route: &RouteKey, gate: &BackpressureGate) {
        self.with_snapshot_store(|snapshot_store| {
            metrics::record_backpressure_snapshot(
                snapshot_store,
                route.clone(),
                gate.waiting(),
                gate.in_flight(),
            );
        });
    }

    async fn return_backend(&self, backend: Backend) {
        if let Some(lifecycle) = self
            .backend_lifecycle
            .lock()
            .expect("backend lifecycle poisoned")
            .get_mut(&backend.id())
        {
            lifecycle.last_released_at = tokio::time::Instant::now();
        }
        self.idle.lock().await.push_back(backend);
        self.idle_backends.fetch_add(1, Ordering::AcqRel);
        self.backend_available.notify_one();
        self.sync_pool_snapshot();
    }

    fn discard_backend(&self, backend_id: u64) {
        self.backend_global_permits
            .lock()
            .expect("backend global permits poisoned")
            .remove(&backend_id);
        self.notify_global_backend_available();
        self.backend_lifecycle
            .lock()
            .expect("backend lifecycle poisoned")
            .remove(&backend_id);
        self.release_reserved_backend_slot(None);
    }

    fn release_reserved_backend_slot(&self, global_permit: Option<OwnedSemaphorePermit>) {
        drop(global_permit);
        self.active_backends.fetch_sub(1, Ordering::AcqRel);
        self.backend_available.notify_waiters();
        self.notify_global_backend_available();
        self.sync_pool_snapshot();
    }

    fn notify_global_backend_available(&self) {
        if let Some(notify) = &self.global_backend_available {
            notify.notify_waiters();
        }
    }
}

fn backpressure_outcome(error: BackpressureError) -> &'static str {
    match error {
        BackpressureError::QueueFull => "queue_full",
        BackpressureError::Timeout => "timeout",
        BackpressureError::Closed => "closed",
    }
}

fn pool_checkout_outcome(result: &Result<PooledBackend, PoolError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(PoolError::Backpressure(error)) => backpressure_outcome(*error),
        Err(PoolError::Connect(_)) => "error",
    }
}

impl PooledBackend {
    #[must_use]
    pub fn backend_id(&self) -> u64 {
        self.backend
            .as_ref()
            .expect("pooled backend exists until drop")
            .id()
    }

    #[must_use]
    pub fn backend(&self) -> &Backend {
        self.backend
            .as_ref()
            .expect("pooled backend exists until release")
    }

    pub fn backend_mut(&mut self) -> &mut Backend {
        self.backend
            .as_mut()
            .expect("pooled backend exists until release")
    }

    pub fn mark_failed(&self) {
        self.health.store(false, Ordering::Release);
    }

    #[must_use]
    pub const fn requires_startup(&self) -> bool {
        self.requires_startup
    }

    pub async fn release(self) {
        let Self {
            backend,
            pool,
            _permit,
            route_key,
            route_gate,
            requires_startup: _,
            ..
        } = self;

        if let Some(backend) = backend {
            backend.mark_idle(Some(route_key.clone()));
            pool.return_backend(backend).await;
        }

        drop(_permit);
        pool.record_backpressure_counts(&route_key, &route_gate);
    }

    pub fn discard(self) {
        let Self {
            backend,
            pool,
            _permit,
            route_key,
            route_gate,
            requires_startup: _,
            ..
        } = self;

        if let Some(backend) = backend {
            backend.mark_discarded();
            pool.discard_backend(backend.id());
        }

        drop(_permit);
        pool.record_backpressure_counts(&route_key, &route_gate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_kinetic_core::route::QueryClass;
    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::net::TcpListener;

    fn route_key(label: &str) -> RouteKey {
        RouteKey::new(
            "pgkinetic",
            "postgres",
            Some(label),
            None,
            QueryClass::Default,
        )
    }

    fn test_pool(addr: SocketAddr) -> Arc<BackendPool> {
        test_pool_with_capacity(addr, 1)
    }

    fn test_pool_with_capacity(addr: SocketAddr, max_backends: usize) -> Arc<BackendPool> {
        BackendPool::new(
            addr,
            TlsConfig::default(),
            max_backends,
            1,
            1,
            1,
            Duration::from_millis(200),
            "DISCARD ALL",
        )
    }

    fn backend_ref_primary(addr: SocketAddr) -> BackendPoolRef {
        BackendPoolRef::primary(test_pool(addr))
    }

    fn backend_ref_replica(id: u64, weight: usize, addr: SocketAddr) -> BackendPoolRef {
        BackendPoolRef::replica(id, weight, test_pool(addr))
    }

    async fn backend_listener() -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind backend");
        let addr = listener.local_addr().expect("listener addr");
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_probe = accepted.clone();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept backend");
                accepted_probe.fetch_add(1, Ordering::Relaxed);
                drop(stream);
            }
        });

        (addr, accepted)
    }

    async fn wait_for_accepts(accepted: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while accepted.load(Ordering::Relaxed) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("backend accept observed");
    }

    #[tokio::test]
    async fn route_pool_registry_primary_pool_checkout_uses_primary_pool() {
        let route = route_key("primary-only");
        let (primary_addr, primary_accepts) = backend_listener().await;
        let (replica_addr, replica_accepts) = backend_listener().await;

        let pools = RoutePools::new(
            backend_ref_primary(primary_addr),
            vec![backend_ref_replica(1, 1, replica_addr)],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        );
        let registry = RoutePoolRegistry::new();
        registry.insert(route.clone(), pools.clone());

        let checkout = registry
            .checkout_primary(&route, CheckoutMode::AllowConnect)
            .await
            .expect("primary checkout");

        assert!(checkout.requires_startup());
        wait_for_accepts(&primary_accepts, 1).await;
        assert_eq!(primary_accepts.load(Ordering::Relaxed), 1);
        assert_eq!(replica_accepts.load(Ordering::Relaxed), 0);
        assert_eq!(pools.primary().role(), BackendRole::Primary);
        assert_eq!(pools.replicas().len(), 1);
    }

    #[tokio::test]
    async fn prefer_connect_opens_a_new_backend_while_capacity_remains() {
        let (addr, accepted) = backend_listener().await;
        let pool = test_pool_with_capacity(addr, 2);
        let route = route_key("startup");

        let first = pool
            .checkout_primary(route.clone())
            .await
            .expect("first checkout");
        assert!(first.requires_startup());
        first.release().await;

        let second = pool
            .checkout_primary_prefer_connect(route)
            .await
            .expect("preferred checkout");

        assert!(second.requires_startup());
        wait_for_accepts(&accepted, 2).await;
    }

    #[test]
    fn pool_reference_reports_idle_capacity() {
        let pool = BackendPool::new(
            "127.0.0.1:5432".parse().expect("backend address"),
            TlsConfig::default(),
            3,
            4,
            4,
            4,
            Duration::from_secs(1),
            "DISCARD ALL",
        );
        let pool_ref = BackendPoolRef::primary(pool);

        assert_eq!(pool_ref.idle_backends(), 0);
        assert_eq!(pool_ref.max_backends(), 3);
    }

    #[tokio::test]
    async fn route_pool_registry_primary_only_routes_continue_to_checkout_primary() {
        let route = route_key("primary-only-route");
        let (primary_addr, primary_accepts) = backend_listener().await;

        let pools = RoutePools::new(
            backend_ref_primary(primary_addr),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        );
        let registry = RoutePoolRegistry::new();
        registry.insert(route.clone(), pools.clone());

        let checkout = registry
            .checkout_replica_or_primary(&route, CheckoutMode::AllowConnect)
            .await
            .expect("primary-only checkout");

        assert!(checkout.requires_startup());
        wait_for_accepts(&primary_accepts, 1).await;
        assert!(pools.replicas().is_empty());
        assert_eq!(primary_accepts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn route_pool_registry_replica_selection_skips_unhealthy_replicas() {
        let route = route_key("least-waiting");
        let (primary_addr, primary_accepts) = backend_listener().await;
        let (replica_one_addr, replica_one_accepts) = backend_listener().await;
        let (replica_two_addr, replica_two_accepts) = backend_listener().await;

        let primary = backend_ref_primary(primary_addr);
        let replica_one = backend_ref_replica(1, 1, replica_one_addr);
        let replica_two = backend_ref_replica(2, 1, replica_two_addr);

        replica_one.set_healthy(false);
        replica_one.set_waiting_hint(0);
        replica_two.set_waiting_hint(4);

        let pools = RoutePools::new(
            primary,
            vec![replica_one.clone(), replica_two.clone()],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        );

        let checkout = pools
            .checkout_any_replica(route.clone(), CheckoutMode::AllowConnect)
            .await
            .expect("healthy replica checkout");

        assert!(checkout.requires_startup());
        wait_for_accepts(&replica_two_accepts, 1).await;
        assert_eq!(replica_one_accepts.load(Ordering::Relaxed), 0);
        assert_eq!(replica_two_accepts.load(Ordering::Relaxed), 1);
        assert_eq!(primary_accepts.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn replica_selector_weighted_round_robin_is_deterministic() {
        let first = backend_ref_replica(1, 1, "127.0.0.1:54321".parse().expect("socket"));
        let second = backend_ref_replica(2, 2, "127.0.0.1:54322".parse().expect("socket"));
        let third = backend_ref_replica(3, 1, "127.0.0.1:54323".parse().expect("socket"));
        let selector = ReplicaSelector::new(ReplicaSelectionStrategy::WeightedRoundRobin);

        let replicas = vec![first.clone(), second.clone(), third.clone()];
        let selected_ids: Vec<u64> = (0..6)
            .map(|_| selector.select(&replicas).expect("replica selection").id())
            .collect();

        assert_eq!(selected_ids, vec![1, 2, 2, 3, 1, 2]);
    }

    #[tokio::test]
    async fn route_pool_registry_checkout_replica_by_id_targets_specific_replica() {
        let route = route_key("replica-by-id");
        let (primary_addr, primary_accepts) = backend_listener().await;
        let (replica_one_addr, replica_one_accepts) = backend_listener().await;
        let (replica_two_addr, replica_two_accepts) = backend_listener().await;

        let pools = RoutePools::new(
            backend_ref_primary(primary_addr),
            vec![
                backend_ref_replica(11, 1, replica_one_addr),
                backend_ref_replica(22, 1, replica_two_addr),
            ],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        );
        let registry = RoutePoolRegistry::new();
        registry.insert(route.clone(), pools);

        let checkout = registry
            .checkout_replica_by_id(&route, 22, CheckoutMode::AllowConnect)
            .await
            .expect("specific replica checkout");

        assert!(checkout.requires_startup());
        wait_for_accepts(&replica_two_accepts, 1).await;
        assert_eq!(primary_accepts.load(Ordering::Relaxed), 0);
        assert_eq!(replica_one_accepts.load(Ordering::Relaxed), 0);
        assert_eq!(replica_two_accepts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn route_pool_registry_checkout_replica_or_primary_falls_back_to_primary() {
        let route = route_key("fallback");
        let (primary_addr, primary_accepts) = backend_listener().await;
        let primary = backend_ref_primary(primary_addr);
        let failing_replica = backend_ref_replica(77, 1, "127.0.0.1:9".parse().expect("socket"));

        let pools = RoutePools::new(
            primary,
            vec![failing_replica],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        );
        let registry = RoutePoolRegistry::new();
        registry.insert(route.clone(), pools);

        let checkout = registry
            .checkout_replica_or_primary(&route, CheckoutMode::AllowConnect)
            .await
            .expect("primary fallback checkout");

        assert!(checkout.requires_startup());
        wait_for_accepts(&primary_accepts, 1).await;
        assert_eq!(primary_accepts.load(Ordering::Relaxed), 1);
    }
}
