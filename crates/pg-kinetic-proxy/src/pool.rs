use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;

use crate::backend::Backend;
use crate::metrics;
use pg_kinetic_core::{
    backpressure::{BackpressureError, BackpressureGate, BackpressurePermit},
    route::RouteKey,
};

#[derive(Debug)]
pub struct BackendPool {
    backend_addr: SocketAddr,
    reset_query: Arc<str>,
    gate: BackpressureGate,
    route_gates: Mutex<HashMap<RouteKey, BackpressureGate>>,
    idle: Mutex<VecDeque<Backend>>,
    backend_available: Notify,
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
    pub fn new(
        backend_addr: SocketAddr,
        max_backends: usize,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: Duration,
        reset_query: impl Into<Arc<str>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend_addr,
            reset_query: reset_query.into(),
            gate: BackpressureGate::new(max_backends, max_waiters),
            route_gates: Mutex::new(HashMap::new()),
            idle: Mutex::new(VecDeque::new()),
            backend_available: Notify::new(),
            max_backends,
            max_waiters,
            route_max_in_flight,
            route_max_waiters,
            checkout_timeout,
        })
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
        let route_gate = self.route_gate(&route).await;
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
                    return Err(PoolError::Backpressure(error));
                }
            };

            metrics::record_route_wait(&route, started.elapsed().as_secs_f64() * 1000.0, "ok");
            metrics::record_route_in_flight(&route, route_gate.in_flight());
            metrics::record_route_waiting(&route, route_gate.waiting());

            if let Some(backend) = self.checkout_idle_backend(mode).await {
                return Ok(PooledBackend {
                    backend: Some(backend),
                    pool: self.clone(),
                    _permit: BackpressurePermit::join(route_permit, permit),
                    requires_startup: false,
                });
            }

            let backend = Backend::connect(self.backend_addr)
                .await
                .map_err(PoolError::Connect)?;

            Ok(PooledBackend {
                backend: Some(backend),
                pool: self.clone(),
                _permit: BackpressurePermit::join(route_permit, permit),
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
                Err(PoolError::Backpressure(BackpressureError::Timeout))
            }
        }
    }

    async fn checkout_idle_backend(&self, mode: CheckoutMode) -> Option<Backend> {
        loop {
            let notified = self.backend_available.notified();
            if let Some(backend) = self.idle.lock().await.pop_front() {
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

    async fn route_gate(&self, route: &RouteKey) -> BackpressureGate {
        let mut route_gates = self.route_gates.lock().await;
        route_gates
            .entry(route.clone())
            .or_insert_with(|| {
                BackpressureGate::new(self.route_max_in_flight, self.route_max_waiters)
            })
            .clone()
    }

    async fn return_backend(&self, backend: Backend) {
        self.idle.lock().await.push_back(backend);
        self.backend_available.notify_one();
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

    pub async fn release(mut self) {
        if let Some(backend) = self.backend.take() {
            self.pool.return_backend(backend).await;
        }
    }

    pub fn discard(mut self) {
        let _ = self.backend.take();
    }
}
