use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time,
};

#[derive(Clone, Debug)]
pub struct BackpressureGate {
    capacity: Arc<Semaphore>,
    max_waiters: usize,
    waiters: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct BackpressurePermit {
    _permit: OwnedSemaphorePermit,
    in_flight: Arc<AtomicUsize>,
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

    pub async fn checkout(
        &self,
        timeout: Duration,
    ) -> Result<BackpressurePermit, BackpressureError> {
        if self.capacity.available_permits() == 0 {
            let previous = self.waiters.fetch_add(1, Ordering::AcqRel);
            if previous >= self.max_waiters {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                return Err(BackpressureError::QueueFull);
            }

            let permit = time::timeout(timeout, self.capacity.clone().acquire_owned())
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
            self.in_flight.fetch_add(1, Ordering::AcqRel);
            return Ok(BackpressurePermit {
                _permit: permit,
                in_flight: self.in_flight.clone(),
            });
        }

        let permit = self
            .capacity
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BackpressureError::Closed)?;
        self.in_flight.fetch_add(1, Ordering::AcqRel);

        Ok(BackpressurePermit {
            _permit: permit,
            in_flight: self.in_flight.clone(),
        })
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn waiting(&self) -> usize {
        self.waiters.load(Ordering::Acquire)
    }
}

impl Drop for BackpressurePermit {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}
