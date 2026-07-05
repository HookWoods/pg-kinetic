use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError},
    time,
};

use crate::route::RouteKey;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RouteBackpressureSnapshot {
    pub in_flight: usize,
    pub waiting: usize,
}

#[derive(Clone, Debug)]
pub struct BackpressureGate {
    capacity: Arc<Semaphore>,
    max_waiters: usize,
    waiters: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct BackpressurePermit {
    permits: Vec<OwnedSemaphorePermit>,
    in_flight_counters: Vec<Arc<AtomicUsize>>,
}

#[derive(Clone, Debug)]
pub struct BackpressureCoordinator {
    global: BackpressureGate,
    routes: Arc<Mutex<HashMap<RouteKey, BackpressureGate>>>,
    max_route_in_flight: usize,
    max_route_waiters: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BackpressureError {
    #[error("checkout queue is full")]
    QueueFull,

    #[error("checkout timed out")]
    Timeout,

    #[error("checkout capacity is closed")]
    Closed,
}

impl BackpressureGate {
    #[must_use]
    pub fn new(max_in_flight: usize, max_waiters: usize) -> Self {
        Self {
            capacity: Arc::new(Semaphore::new(max_in_flight)),
            max_waiters,
            waiters: Arc::new(AtomicUsize::new(0)),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[must_use]
    pub fn unbounded(max_waiters: usize) -> Self {
        Self::new(usize::MAX >> 3, max_waiters)
    }

    async fn checkout_until(
        &self,
        deadline: time::Instant,
    ) -> Result<BackpressurePermit, BackpressureError> {
        let permit = match self.capacity.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::Closed) => return Err(BackpressureError::Closed),
            Err(TryAcquireError::NoPermits) => {
                let previous = self.waiters.fetch_add(1, Ordering::AcqRel);
                if previous >= self.max_waiters {
                    self.waiters.fetch_sub(1, Ordering::AcqRel);
                    return Err(BackpressureError::QueueFull);
                }

                let permit = time::timeout_at(deadline, self.capacity.clone().acquire_owned())
                    .await
                    .map_err(|_| {
                        self.waiters.fetch_sub(1, Ordering::AcqRel);
                        BackpressureError::Timeout
                    })?
                    .map_err(|_| {
                        self.waiters.fetch_sub(1, Ordering::AcqRel);
                        BackpressureError::Closed
                    })?;

                self.waiters.fetch_sub(1, Ordering::AcqRel);
                permit
            }
        };

        self.in_flight.fetch_add(1, Ordering::AcqRel);

        Ok(BackpressurePermit::new(
            vec![permit],
            vec![self.in_flight.clone()],
        ))
    }

    pub async fn checkout(
        &self,
        timeout: Duration,
    ) -> Result<BackpressurePermit, BackpressureError> {
        let deadline = time::Instant::now() + timeout;
        self.checkout_until(deadline).await
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn waiting(&self) -> usize {
        self.waiters.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn snapshot(&self) -> RouteBackpressureSnapshot {
        RouteBackpressureSnapshot {
            in_flight: self.in_flight(),
            waiting: self.waiting(),
        }
    }
}

impl BackpressureCoordinator {
    #[must_use]
    pub fn new(max_route_in_flight: usize, max_route_waiters: usize) -> Self {
        Self {
            global: BackpressureGate::unbounded(max_route_waiters),
            routes: Arc::new(Mutex::new(HashMap::new())),
            max_route_in_flight,
            max_route_waiters,
        }
    }

    fn route_gate(&self, route: &RouteKey) -> BackpressureGate {
        let mut routes = self.routes.lock().expect("route map poisoned");
        routes
            .entry(route.clone())
            .or_insert_with(|| {
                BackpressureGate::new(self.max_route_in_flight, self.max_route_waiters)
            })
            .clone()
    }

    pub async fn checkout(
        &self,
        route: RouteKey,
        timeout: Duration,
    ) -> Result<BackpressurePermit, BackpressureError> {
        let deadline = time::Instant::now() + timeout;
        let route_gate = self.route_gate(&route);
        let route_permit = route_gate.checkout_until(deadline).await?;
        let global_permit = self.global.checkout_until(deadline).await?;

        Ok(BackpressurePermit::join(route_permit, global_permit))
    }

    #[must_use]
    pub fn route_snapshot(&self, route: &RouteKey) -> RouteBackpressureSnapshot {
        self.routes
            .lock()
            .expect("route map poisoned")
            .get(route)
            .map(BackpressureGate::snapshot)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn global_snapshot(&self) -> RouteBackpressureSnapshot {
        self.global.snapshot()
    }
}

impl BackpressurePermit {
    fn new(permits: Vec<OwnedSemaphorePermit>, in_flight_counters: Vec<Arc<AtomicUsize>>) -> Self {
        Self {
            permits,
            in_flight_counters,
        }
    }

    pub fn join(mut route: BackpressurePermit, mut global: BackpressurePermit) -> Self {
        route.permits.append(&mut global.permits);
        route
            .in_flight_counters
            .append(&mut global.in_flight_counters);
        route
    }
}

impl Drop for BackpressurePermit {
    fn drop(&mut self) {
        for counter in &self.in_flight_counters {
            counter.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
