use std::{collections::VecDeque, net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::Mutex;

use crate::{
    backend::Backend,
    backpressure::{BackpressureError, BackpressureGate, BackpressurePermit},
};

#[derive(Debug)]
pub struct BackendPool {
    backend_addr: SocketAddr,
    reset_query: Arc<str>,
    gate: BackpressureGate,
    idle: Mutex<VecDeque<Backend>>,
    max_backends: usize,
    max_waiters: usize,
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
        checkout_timeout: Duration,
        reset_query: impl Into<Arc<str>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend_addr,
            reset_query: reset_query.into(),
            gate: BackpressureGate::new(max_backends, max_waiters),
            idle: Mutex::new(VecDeque::new()),
            max_backends,
            max_waiters,
            checkout_timeout,
        })
    }

    pub async fn checkout(self: &Arc<Self>) -> Result<PooledBackend, PoolError> {
        let permit = self.gate.checkout(self.checkout_timeout).await?;

        if let Some(backend) = self.idle.lock().await.pop_front() {
            return Ok(PooledBackend {
                backend: Some(backend),
                pool: self.clone(),
                _permit: permit,
                requires_startup: false,
            });
        }

        let backend = Backend::connect(self.backend_addr)
            .await
            .map_err(PoolError::Connect)?;

        Ok(PooledBackend {
            backend: Some(backend),
            pool: self.clone(),
            _permit: permit,
            requires_startup: true,
        })
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
    pub fn reset_query(&self) -> &str {
        &self.reset_query
    }

    async fn return_backend(&self, backend: Backend) {
        self.idle.lock().await.push_back(backend);
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
