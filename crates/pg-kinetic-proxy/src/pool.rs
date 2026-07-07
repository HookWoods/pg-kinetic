use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;

use crate::{
    backend::Backend,
    config::{SocketConfig, TlsConfig},
    metrics,
    snapshot::{PoolSnapshot, SnapshotStore},
};
use pg_kinetic_core::{
    backpressure::{BackpressureError, BackpressureGate, BackpressurePermit},
    route::RouteKey,
};

#[derive(Debug)]
pub struct BackendPool {
    backend_addr: SocketAddr,
    tls: TlsConfig,
    socket: SocketConfig,
    reset_query: Arc<str>,
    gate: BackpressureGate,
    route_gates: StdMutex<HashMap<RouteKey, BackpressureGate>>,
    idle: Mutex<VecDeque<Backend>>,
    backend_available: Notify,
    snapshot_store: StdMutex<Option<SnapshotStore>>,
    active_backends: AtomicUsize,
    idle_backends: AtomicUsize,
    max_backends: usize,
    max_waiters: usize,
    route_max_in_flight: usize,
    route_max_waiters: usize,
    checkout_timeout: Duration,
}

#[derive(Debug)]
pub struct PooledBackend {
    backend: Option<Backend>,
    pool: Arc<BackendPool>,
    _permit: BackpressurePermit,
    route_key: RouteKey,
    requires_startup: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("backend checkout rejected: {0}")]
    Backpressure(#[from] BackpressureError),

    #[error("backend connection failed: {0}")]
    Connect(#[source] anyhow::Error),
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
        Arc::new(Self {
            backend_addr,
            tls,
            socket,
            reset_query: reset_query.into(),
            gate: BackpressureGate::new(max_backends, max_waiters),
            route_gates: StdMutex::new(HashMap::new()),
            idle: Mutex::new(VecDeque::new()),
            backend_available: Notify::new(),
            snapshot_store: StdMutex::new(None),
            active_backends: AtomicUsize::new(0),
            idle_backends: AtomicUsize::new(0),
            max_backends,
            max_waiters,
            route_max_in_flight,
            route_max_waiters,
            checkout_timeout,
        })
    }

    pub fn attach_snapshot_store(&self, snapshot_store: SnapshotStore) {
        *self
            .snapshot_store
            .lock()
            .expect("snapshot store poisoned") = Some(snapshot_store.clone());

        if let Ok(mut idle_backends) = self.idle.try_lock() {
            for backend in idle_backends.iter_mut() {
                backend.attach_snapshot_store(snapshot_store.clone());
            }
        }

        self.sync_pool_snapshot();
    }

    pub async fn checkout(self: &Arc<Self>, route: RouteKey) -> Result<PooledBackend, PoolError> {
        self.checkout_with_mode(route, CheckoutMode::AllowConnect)
            .await
    }

    pub async fn checkout_reusable(
        self: &Arc<Self>,
        route: RouteKey,
    ) -> Result<PooledBackend, PoolError> {
        self.checkout_with_mode(route, CheckoutMode::ReuseOnly)
            .await
    }

    async fn checkout_with_mode(
        self: &Arc<Self>,
        route: RouteKey,
        mode: CheckoutMode,
    ) -> Result<PooledBackend, PoolError> {
        let route_gate = self.route_gate(&route);
        let started = Instant::now();
        let checkout = async {
            let route_permit = match route_gate.checkout(self.checkout_timeout).await {
                Ok(permit) => permit,
                Err(error) => {
                    metrics::increment_backpressure_event(&route, backpressure_outcome(error));
                    metrics::record_route_wait(
                        &route,
                        started.elapsed().as_secs_f64() * 1000.0,
                        backpressure_outcome(error),
                    );
                    self.record_backpressure_counts(&route);
                    return Err(PoolError::Backpressure(error));
                }
            };
            let permit = match self.gate.checkout(self.checkout_timeout).await {
                Ok(permit) => permit,
                Err(error) => {
                    metrics::increment_backpressure_event(&route, backpressure_outcome(error));
                    metrics::record_route_wait(
                        &route,
                        started.elapsed().as_secs_f64() * 1000.0,
                        backpressure_outcome(error),
                    );
                    self.record_backpressure_counts(&route);
                    return Err(PoolError::Backpressure(error));
                }
            };

            metrics::record_route_wait(&route, started.elapsed().as_secs_f64() * 1000.0, "ok");
            metrics::record_route_in_flight(&route, route_gate.in_flight());
            metrics::record_route_waiting(&route, route_gate.waiting());
            self.record_backpressure_counts(&route);

            if let Some(mut backend) = self.checkout_idle_backend(mode).await {
                self.attach_backend_snapshot_store(&mut backend);
                backend.mark_checked_out(Some(route.clone()));
                self.sync_pool_snapshot();
                return Ok(PooledBackend {
                    backend: Some(backend),
                    pool: self.clone(),
                    _permit: BackpressurePermit::join(route_permit, permit),
                    route_key: route.clone(),
                    requires_startup: false,
                });
            }

            let mut backend = Backend::connect_with_socket(self.backend_addr, &self.tls, &self.socket)
                .await
                .map_err(PoolError::Connect)?;
            self.active_backends.fetch_add(1, Ordering::AcqRel);
            self.attach_backend_snapshot_store(&mut backend);
            backend.mark_checked_out(Some(route.clone()));
            self.sync_pool_snapshot();

            Ok(PooledBackend {
                backend: Some(backend),
                pool: self.clone(),
                _permit: BackpressurePermit::join(route_permit, permit),
                route_key: route.clone(),
                requires_startup: true,
            })
        };

        match timeout(self.checkout_timeout, checkout).await {
            Ok(result) => result,
            Err(_) => {
                metrics::increment_backpressure_event(&route, "timeout");
                metrics::record_route_wait(
                    &route,
                    started.elapsed().as_secs_f64() * 1000.0,
                    "timeout",
                );
                self.record_backpressure_counts(&route);
                Err(PoolError::Backpressure(BackpressureError::Timeout))
            }
        }
    }

    async fn checkout_idle_backend(&self, mode: CheckoutMode) -> Option<Backend> {
        loop {
            let notified = self.backend_available.notified();
            if let Some(backend) = self.idle.lock().await.pop_front() {
                self.idle_backends.fetch_sub(1, Ordering::AcqRel);
                return Some(backend);
            }
            if mode == CheckoutMode::AllowConnect {
                return None;
            }
            notified.await;
        }
    }

    #[must_use]
    pub const fn max_backends(&self) -> usize {
        self.max_backends
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

    fn route_gate(&self, route: &RouteKey) -> BackpressureGate {
        let mut route_gates = self.route_gates.lock().expect("route gates poisoned");
        route_gates
            .entry(route.clone())
            .or_insert_with(|| {
                BackpressureGate::new(self.route_max_in_flight, self.route_max_waiters)
            })
            .clone()
    }

    fn snapshot_store(&self) -> Option<SnapshotStore> {
        self.snapshot_store
            .lock()
            .expect("snapshot store poisoned")
            .clone()
    }

    fn attach_backend_snapshot_store(&self, backend: &mut Backend) {
        if let Some(snapshot_store) = self.snapshot_store() {
            backend.attach_snapshot_store(snapshot_store);
        }
    }

    fn sync_pool_snapshot(&self) {
        if let Some(snapshot_store) = self.snapshot_store() {
            metrics::record_pool_snapshot(
                &snapshot_store,
                PoolSnapshot {
                    configured_backends: self.max_backends,
                    active_backends: self.active_backends.load(Ordering::Acquire),
                    idle_backends: self.idle_backends.load(Ordering::Acquire),
                    waiting_clients: self.gate.waiting(),
                },
            );
        }
    }

    fn record_backpressure_counts(&self, route: &RouteKey) {
        if let Some(snapshot_store) = self.snapshot_store() {
            let route_gate = self.route_gate(route);
            metrics::record_backpressure_snapshot(
                &snapshot_store,
                route.clone(),
                route_gate.waiting(),
                route_gate.in_flight(),
            );
        }
    }

    async fn return_backend(&self, backend: Backend) {
        self.idle.lock().await.push_back(backend);
        self.idle_backends.fetch_add(1, Ordering::AcqRel);
        self.backend_available.notify_one();
        self.sync_pool_snapshot();
    }

    fn discard_backend(&self) {
        self.active_backends.fetch_sub(1, Ordering::AcqRel);
        self.sync_pool_snapshot();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckoutMode {
    AllowConnect,
    ReuseOnly,
}

fn backpressure_outcome(error: BackpressureError) -> &'static str {
    match error {
        BackpressureError::QueueFull => "queue_full",
        BackpressureError::Timeout => "timeout",
        BackpressureError::Closed => "closed",
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

    pub fn backend_mut(&mut self) -> &mut Backend {
        self.backend
            .as_mut()
            .expect("pooled backend exists until release")
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
            requires_startup: _,
        } = self;

        if let Some(backend) = backend {
            backend.mark_idle(Some(route_key.clone()));
            pool.return_backend(backend).await;
        }

        drop(_permit);
        pool.record_backpressure_counts(&route_key);
    }

    pub fn discard(self) {
        let Self {
            backend,
            pool,
            _permit,
            route_key,
            requires_startup: _,
        } = self;

        if let Some(backend) = backend {
            backend.mark_discarded();
            pool.discard_backend();
        }

        drop(_permit);
        pool.record_backpressure_counts(&route_key);
    }
}
