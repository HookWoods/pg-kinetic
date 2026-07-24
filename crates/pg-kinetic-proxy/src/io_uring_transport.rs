use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use bytes::{BufMut, BytesMut};
use monoio::{
    io::{AsyncReadRent, AsyncWriteRent},
    net::TcpStream,
};

use crate::{
    metrics,
    pool::{PoolBackendConnector, PoolBackendTransport},
    snapshot::{ServerSnapshot, SnapshotStore},
};
use pg_kinetic_core::route::RouteKey;

static NEXT_MONOIO_BACKEND_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct MonoioTransport {
    stream: TcpStream,
    read_buf: Vec<u8>,
}

impl MonoioTransport {
    #[must_use]
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: vec![0; 16 * 1024],
        }
    }

    pub async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        let buffer = std::mem::take(&mut self.read_buf);
        let (result, buffer) = self.stream.read(buffer).await;
        self.read_buf = buffer;
        let read = result?;
        if read > 0 {
            dst.put_slice(&self.read_buf[..read]);
        }
        Ok(read)
    }

    pub async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let mut written = 0;
        while written < bytes.len() {
            let chunk = bytes[written..].to_vec();
            let (result, chunk) = self.stream.write(chunk).await;
            let n = result?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "monoio write returned zero",
                ));
            }
            written += n;
            drop(chunk);
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        self.stream.shutdown().await
    }
}

#[derive(Debug)]
pub struct MonoioBackend {
    id: u64,
    transport: MonoioTransport,
    addr: SocketAddr,
    connected_at: Instant,
    snapshot_store: Option<SnapshotStore>,
    parameter_status: Vec<(String, String)>,
    key_data: Option<(i32, i32)>,
}

impl MonoioBackend {
    #[must_use]
    pub fn new(addr: SocketAddr, transport: MonoioTransport) -> Self {
        Self {
            id: NEXT_MONOIO_BACKEND_ID.fetch_add(1, Ordering::Relaxed),
            transport,
            addr,
            connected_at: Instant::now(),
            snapshot_store: None,
            parameter_status: Vec::new(),
            key_data: None,
        }
    }

    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub const fn is_tls(&self) -> bool {
        false
    }

    #[must_use]
    pub fn parameter_status(&self) -> &[(String, String)] {
        &self.parameter_status
    }

    pub fn push_parameter_status(&mut self, name: String, value: String) {
        if let Some((_, existing_value)) = self
            .parameter_status
            .iter_mut()
            .find(|(existing_name, _)| *existing_name == name)
        {
            *existing_value = value;
        } else {
            self.parameter_status.push((name, value));
        }
    }

    #[must_use]
    pub const fn key_data(&self) -> Option<(i32, i32)> {
        self.key_data
    }

    pub fn set_key_data(&mut self, process_id: i32, secret_key: i32) {
        self.key_data = Some((process_id, secret_key));
    }

    fn publish_snapshot(&self, state: &'static str, route_key: Option<RouteKey>) {
        if let Some(snapshot_store) = self.snapshot_store.as_ref() {
            let mut snapshot = ServerSnapshot::new(self.id, state, self.connected_at.elapsed());
            snapshot.route_key = route_key;
            metrics::record_server_snapshot(snapshot_store, snapshot);
        }
    }
}

impl PoolBackendTransport for MonoioBackend {
    fn id(&self) -> u64 {
        self.id()
    }

    fn attach_snapshot_store(&mut self, snapshot_store: SnapshotStore) {
        self.snapshot_store = Some(snapshot_store);
    }

    fn mark_checked_out(&self, route_key: Option<RouteKey>) {
        self.publish_snapshot("checked_out", route_key);
    }

    fn mark_idle(&self, route_key: Option<RouteKey>) {
        self.publish_snapshot("idle", route_key);
    }

    fn mark_discarded(&self) {
        if let Some(snapshot_store) = self.snapshot_store.as_ref() {
            metrics::remove_server_snapshot(snapshot_store, self.id);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MonoioBackendConnector {
    backend_addr: SocketAddr,
}

impl MonoioBackendConnector {
    pub(crate) const fn new(backend_addr: SocketAddr) -> Self {
        Self { backend_addr }
    }

    pub(crate) const fn backend_addr(&self) -> SocketAddr {
        self.backend_addr
    }
}

impl PoolBackendConnector<MonoioBackend> for MonoioBackendConnector {
    async fn connect(&self) -> anyhow::Result<MonoioBackend> {
        let stream = TcpStream::connect_addr(self.backend_addr).await?;
        Ok(MonoioBackend::new(
            self.backend_addr,
            MonoioTransport::new(stream),
        ))
    }
}

pub(crate) type MonoioPooledBackend =
    crate::pool::PooledBackendLease<MonoioBackend, std::sync::Arc<MonoioBackendPool>>;

#[derive(Debug)]
pub(crate) struct MonoioBackendPool {
    core: std::sync::Arc<crate::pool::BackendPoolCore<MonoioBackend, MonoioBackendConnector>>,
}

impl crate::pool::BackendLeaseOwner<MonoioBackend> for std::sync::Arc<MonoioBackendPool> {
    async fn return_backend(&self, backend: MonoioBackend) {
        self.core.return_backend(backend).await;
    }

    fn discard_backend(&self, backend_id: u64) {
        self.core.discard_backend(backend_id);
    }

    fn record_backpressure_counts(
        &self,
        route: &pg_kinetic_core::route::RouteKey,
        gate: &pg_kinetic_core::backpressure::BackpressureGate,
    ) {
        self.core.record_backpressure_counts(route, gate);
    }
}

impl MonoioBackendPool {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        backend_addr: SocketAddr,
        lifecycle: crate::config::PoolLifecycleConfig,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: std::time::Duration,
        global_backend_slots: Option<std::sync::Arc<tokio::sync::Semaphore>>,
        global_backend_available: Option<std::sync::Arc<tokio::sync::Notify>>,
        route_dynamic_limit: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            core: crate::pool::BackendPoolCore::new(
                MonoioBackendConnector::new(backend_addr),
                lifecycle,
                max_waiters,
                route_max_in_flight,
                route_max_waiters,
                checkout_timeout,
                global_backend_slots,
                global_backend_available,
                route_dynamic_limit,
            ),
        })
    }

    pub(crate) async fn checkout(
        self: &std::sync::Arc<Self>,
        route: pg_kinetic_core::route::RouteKey,
        mode: crate::pool::CheckoutMode,
    ) -> Result<MonoioPooledBackend, crate::pool::PoolError> {
        self.core
            .checkout_with_mode(std::sync::Arc::clone(self), route, mode)
            .await
    }
}

impl crate::proxy::SharedBackendPool<MonoioBackend, std::sync::Arc<MonoioBackendPool>>
    for std::sync::Arc<MonoioBackendPool>
{
    async fn checkout_shared(
        &self,
        route: pg_kinetic_core::route::RouteKey,
    ) -> Result<MonoioPooledBackend, crate::pool::PoolError> {
        self.checkout(route, crate::pool::CheckoutMode::AllowConnect)
            .await
    }
}

impl crate::proxy::BackendStartupMetadata for MonoioBackend {
    fn is_tls(&self) -> bool {
        MonoioBackend::is_tls(self)
    }

    fn parameter_status(&self) -> &[(String, String)] {
        MonoioBackend::parameter_status(self)
    }

    fn push_parameter_status(&mut self, name: String, value: String) {
        MonoioBackend::push_parameter_status(self, name, value);
    }

    fn set_key_data(&mut self, process_id: i32, secret_key: i32) {
        MonoioBackend::set_key_data(self, process_id, secret_key);
    }
}

impl crate::io_runtime::RuntimeByteStream for MonoioBackend {
    async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        crate::io_runtime::read_from(&mut self.transport, dst).await
    }

    async fn write_all_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        crate::io_runtime::write_all_to(&mut self.transport, bytes).await
    }

    async fn shutdown_stream(&mut self) -> std::io::Result<()> {
        crate::io_runtime::shutdown(&mut self.transport).await
    }
}

impl crate::io_runtime::RuntimeByteStream for MonoioTransport {
    async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        MonoioTransport::read_into(self, dst).await
    }

    async fn write_all_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        MonoioTransport::write_all(self, bytes).await
    }

    async fn shutdown_stream(&mut self) -> std::io::Result<()> {
        MonoioTransport::shutdown(self).await
    }
}
