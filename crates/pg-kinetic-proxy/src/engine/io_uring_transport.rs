use std::{
    collections::HashMap,
    net::SocketAddr,
    pin::pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use bytes::{BufMut, BytesMut};
use monoio::{
    io::{AsyncReadRent, AsyncWriteRent, CancelableAsyncReadRent},
    net::TcpStream,
};
use rustls::{pki_types::ServerName, ClientConfig, ServerConfig};

use crate::{
    config::{BackendTlsMode, SocketConfig, TlsConfig},
    net::socket,
    observe::metrics,
    observe::snapshot::{ServerSnapshot, SnapshotStore},
    pool::{PoolBackendConnector, PoolBackendTransport, RoutePools},
    routing::RoutingTarget,
};
use pg_kinetic_core::traffic::route::{PoolKey, RouteKey};
use pg_kinetic_wire::tls::{ssl_request_packet, SslResponse};

static NEXT_MONOIO_BACKEND_ID: AtomicU64 = AtomicU64::new(1);
const MONOIO_READ_BUFFER_BYTES: usize = 16 * 1024;
const ECANCELED: i32 = 125;

#[derive(Debug)]
pub struct MonoioTransport {
    stream: Option<MonoioStream>,
    read_buf: Vec<u8>,
    peer_certificates_present: bool,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum MonoioStream {
    Plain(TcpStream),
    ClientTls(monoio_rustls::ClientTlsStream<TcpStream>),
    ServerTls(monoio_rustls::ServerTlsStream<TcpStream>),
}

impl MonoioTransport {
    #[must_use]
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Some(MonoioStream::Plain(stream)),
            read_buf: vec![0; MONOIO_READ_BUFFER_BYTES],
            peer_certificates_present: false,
        }
    }

    #[must_use]
    fn from_client_tls(stream: monoio_rustls::ClientTlsStream<TcpStream>) -> Self {
        Self {
            stream: Some(MonoioStream::ClientTls(stream)),
            read_buf: vec![0; MONOIO_READ_BUFFER_BYTES],
            peer_certificates_present: false,
        }
    }

    #[must_use]
    pub fn is_tls(&self) -> bool {
        matches!(
            self.stream,
            Some(MonoioStream::ClientTls(_) | MonoioStream::ServerTls(_))
        )
    }

    #[must_use]
    pub const fn has_peer_certificates(&self) -> bool {
        self.peer_certificates_present
    }

    pub async fn start_tls(
        &mut self,
        server_config: &Arc<ServerConfig>,
        peer_certificates_present: bool,
    ) -> anyhow::Result<()> {
        let Some(MonoioStream::Plain(stream)) = self.stream.take() else {
            anyhow::bail!("client TLS is already active");
        };
        let tls_stream = monoio_rustls::TlsAcceptor::from(Arc::clone(server_config))
            .accept(stream)
            .await
            .context("complete io_uring client TLS handshake")?;
        self.stream = Some(MonoioStream::ServerTls(tls_stream));
        self.peer_certificates_present = peer_certificates_present;
        Ok(())
    }

    pub async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        if self.read_buf.is_empty() {
            self.read_buf.resize(MONOIO_READ_BUFFER_BYTES, 0);
        }
        let buffer = std::mem::take(&mut self.read_buf);
        let (result, buffer) = match self.stream.as_mut().expect("monoio stream present") {
            MonoioStream::Plain(stream) => stream.read(buffer).await,
            MonoioStream::ClientTls(stream) => stream.read(buffer).await,
            MonoioStream::ServerTls(stream) => stream.read(buffer).await,
        };
        self.read_buf = buffer;
        let read = result?;
        if read > 0 {
            dst.put_slice(&self.read_buf[..read]);
        }
        Ok(read)
    }

    pub async fn read_into_timeout(
        &mut self,
        dst: &mut BytesMut,
        duration: Duration,
    ) -> Result<std::io::Result<usize>, ()> {
        if self.read_buf.is_empty() {
            self.read_buf.resize(MONOIO_READ_BUFFER_BYTES, 0);
        }

        let canceler = monoio::io::Canceller::new();
        let handle = canceler.handle();
        let buffer = std::mem::take(&mut self.read_buf);
        let mut timer = pin!(monoio::time::sleep(duration));
        if !matches!(self.stream, Some(MonoioStream::Plain(_))) {
            let (result, buffer) =
                match monoio::time::timeout(duration, self.read_tls_like(buffer)).await {
                    Ok((result, buffer)) => (result, buffer),
                    Err(_) => return Err(()),
                };
            self.read_buf = buffer;
            match result {
                Ok(read) => {
                    if read > 0 {
                        dst.put_slice(&self.read_buf[..read]);
                    }
                    return Ok(Ok(read));
                }
                Err(error) => return Ok(Err(error)),
            }
        }

        let Some(MonoioStream::Plain(stream)) = &mut self.stream else {
            unreachable!("TLS streams returned above");
        };
        let mut read = pin!(stream.cancelable_read(buffer, handle));

        monoio::select! {
            _ = &mut timer => {
                canceler.cancel();
                let (result, buffer) = read.await;
                self.read_buf = buffer;
                match result {
                    Ok(read) => {
                        if read > 0 {
                            dst.put_slice(&self.read_buf[..read]);
                        }
                        Ok(Ok(read))
                    }
                    Err(error) if error.raw_os_error() == Some(ECANCELED) => Err(()),
                    Err(error) => Ok(Err(error)),
                }
            }
            (result, buffer) = &mut read => {
                self.read_buf = buffer;
                match result {
                    Ok(read) => {
                        if read > 0 {
                            dst.put_slice(&self.read_buf[..read]);
                        }
                        Ok(Ok(read))
                    }
                    Err(error) => Ok(Err(error)),
                }
            }
        }
    }

    pub async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let mut written = 0;
        while written < bytes.len() {
            let chunk = bytes[written..].to_vec();
            let (result, chunk) = match self.stream.as_mut().expect("monoio stream present") {
                MonoioStream::Plain(stream) => stream.write(chunk).await,
                MonoioStream::ClientTls(stream) => stream.write(chunk).await,
                MonoioStream::ServerTls(stream) => stream.write(chunk).await,
            };
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
        match self.stream.as_mut().expect("monoio stream present") {
            MonoioStream::Plain(stream) => stream.shutdown().await,
            MonoioStream::ClientTls(stream) => stream.shutdown().await,
            MonoioStream::ServerTls(stream) => stream.shutdown().await,
        }
    }

    async fn read_tls_like(&mut self, buffer: Vec<u8>) -> monoio::BufResult<usize, Vec<u8>> {
        match self.stream.as_mut().expect("monoio stream present") {
            MonoioStream::Plain(stream) => stream.read(buffer).await,
            MonoioStream::ClientTls(stream) => stream.read(buffer).await,
            MonoioStream::ServerTls(stream) => stream.read(buffer).await,
        }
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
    pub fn is_tls(&self) -> bool {
        self.transport.is_tls()
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
    tls: TlsConfig,
    socket: SocketConfig,
}

impl MonoioBackendConnector {
    pub(crate) const fn new(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
    ) -> Self {
        Self {
            backend_addr,
            tls,
            socket,
        }
    }
}

impl PoolBackendConnector<MonoioBackend> for MonoioBackendConnector {
    async fn connect(&self) -> anyhow::Result<MonoioBackend> {
        let stream = TcpStream::connect_addr(self.backend_addr).await?;
        let socket_options = socket::SocketOptions::from(&self.socket);
        socket::apply_monoio_socket_options(&stream, &socket_options, "backend")
            .context("apply io_uring backend socket options")?;
        if self.tls.backend_tls_mode != BackendTlsMode::Disable {
            return connect_backend_tls(stream, self.backend_addr, &self.tls).await;
        }
        Ok(MonoioBackend::new(
            self.backend_addr,
            MonoioTransport::new(stream),
        ))
    }
}

async fn connect_backend_tls(
    mut stream: TcpStream,
    backend_addr: SocketAddr,
    tls: &TlsConfig,
) -> anyhow::Result<MonoioBackend> {
    let tls_settings = crate::net::tls::backend_tls_settings(tls)?;
    write_all_raw(&mut stream, &ssl_request_packet())
        .await
        .context("send io_uring backend SSLRequest")?;
    let response = vec![0_u8; 1];
    let (read_result, response) = stream.read(response).await;
    let read = read_result.context("read io_uring backend SSLResponse")?;
    anyhow::ensure!(read == 1, "backend closed during TLS negotiation");
    match response[0] {
        value if value == u8::from(SslResponse::Accept) => {
            let (client_config, server_name) = tls_settings;
            let tls_stream = connect_monoio_tls(stream, client_config, server_name).await?;
            Ok(MonoioBackend::new(
                backend_addr,
                MonoioTransport::from_client_tls(tls_stream),
            ))
        }
        value if value == u8::from(SslResponse::Deny) => match tls.backend_tls_mode {
            BackendTlsMode::Prefer => Ok(MonoioBackend::new(
                backend_addr,
                MonoioTransport::new(stream),
            )),
            BackendTlsMode::Require | BackendTlsMode::VerifyCa | BackendTlsMode::VerifyFull => {
                anyhow::bail!("backend denied TLS negotiation")
            }
            BackendTlsMode::Disable => unreachable!(),
        },
        other => anyhow::bail!("unexpected backend SSLResponse byte {other:#04x}"),
    }
}

async fn write_all_raw(stream: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    let mut written = 0;
    while written < bytes.len() {
        let chunk = bytes[written..].to_vec();
        let (result, chunk) = stream.write(chunk).await;
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

async fn connect_monoio_tls(
    stream: TcpStream,
    client_config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
) -> anyhow::Result<monoio_rustls::ClientTlsStream<TcpStream>> {
    monoio_rustls::TlsConnector::from(client_config)
        .connect(server_name, stream)
        .await
        .context("complete io_uring backend TLS handshake")
}

pub(crate) type MonoioPooledBackend =
    crate::pool::PooledBackendLease<MonoioBackend, std::sync::Arc<MonoioBackendPool>>;

#[derive(Debug)]
pub(crate) struct MonoioBackendPool {
    #[cfg(test)]
    backend_addr: SocketAddr,
    #[cfg(test)]
    connection_settings: BackendConnectionKey,
    core: std::sync::Arc<
        crate::pool::BackendPoolCore<
            MonoioBackend,
            MonoioBackendConnector,
            crate::engine::io_runtime::MonoioTimeout,
        >,
    >,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BackendConnectionKey {
    backend_addr: SocketAddr,
    tls: TlsConfig,
    socket: SocketConfig,
}

impl BackendConnectionKey {
    const fn new(backend_addr: SocketAddr, tls: TlsConfig, socket: SocketConfig) -> Self {
        Self {
            backend_addr,
            tls,
            socket,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MonoioBackendPoolSelector {
    default_connection: BackendConnectionKey,
    backend_slots: Arc<tokio::sync::Semaphore>,
    default_pool: Arc<MonoioBackendPool>,
    route_pools: RwLock<HashMap<(PoolKey, BackendConnectionKey), Arc<MonoioBackendPool>>>,
}

impl MonoioBackendPoolSelector {
    pub(crate) fn new(
        default_backend_addr: SocketAddr,
        default_tls: TlsConfig,
        default_socket: SocketConfig,
        backend_slots: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let default_pool = MonoioBackendPool::new(
            default_backend_addr,
            default_tls.clone(),
            default_socket.clone(),
            crate::config::PoolLifecycleConfig::default(),
            128,
            128,
            128,
            std::time::Duration::from_millis(500),
            Some(backend_slots.clone()),
            None,
            None,
        );
        let default_connection =
            BackendConnectionKey::new(default_backend_addr, default_tls, default_socket);
        Self {
            default_connection,
            backend_slots,
            default_pool,
            route_pools: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn default_pool(&self) -> Arc<MonoioBackendPool> {
        Arc::clone(&self.default_pool)
    }

    pub(crate) fn pool_for_route(
        &self,
        route: &RouteKey,
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
    ) -> Arc<MonoioBackendPool> {
        let connection = BackendConnectionKey::new(backend_addr, tls, socket);
        if connection == self.default_connection {
            return self.default_pool();
        }

        let key = (route.selection_key(), connection.clone());
        if let Some(pool) = self
            .route_pools
            .read()
            .expect("io_uring route pool selector poisoned")
            .get(&key)
        {
            return Arc::clone(pool);
        }

        let pool = MonoioBackendPool::new(
            backend_addr,
            connection.tls.clone(),
            connection.socket.clone(),
            crate::config::PoolLifecycleConfig::default(),
            128,
            128,
            128,
            std::time::Duration::from_millis(500),
            Some(self.backend_slots.clone()),
            None,
            None,
        );
        let mut route_pools = self
            .route_pools
            .write()
            .expect("io_uring route pool selector poisoned");
        Arc::clone(route_pools.entry(key).or_insert_with(|| Arc::clone(&pool)))
    }

    pub(crate) fn pool_for_target(
        &self,
        route: &RouteKey,
        route_pools: &RoutePools,
        target: &RoutingTarget,
    ) -> Option<Arc<MonoioBackendPool>> {
        route_pools.pool_for_target(target).map(|pool| {
            let (tls, socket) = pool.backend_connection_settings();
            self.pool_for_route(route, pool.backend_addr(), tls, socket)
        })
    }
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
        route: &pg_kinetic_core::traffic::route::RouteKey,
        gate: &pg_kinetic_core::traffic::backpressure::BackpressureGate,
    ) {
        self.core.record_backpressure_counts(route, gate);
    }
}

impl MonoioBackendPool {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
        lifecycle: crate::config::PoolLifecycleConfig,
        max_waiters: usize,
        route_max_in_flight: usize,
        route_max_waiters: usize,
        checkout_timeout: std::time::Duration,
        global_backend_slots: Option<std::sync::Arc<tokio::sync::Semaphore>>,
        global_backend_available: Option<std::sync::Arc<tokio::sync::Notify>>,
        route_dynamic_limit: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    ) -> std::sync::Arc<Self> {
        #[cfg(test)]
        let connection_settings =
            BackendConnectionKey::new(backend_addr, tls.clone(), socket.clone());
        std::sync::Arc::new(Self {
            #[cfg(test)]
            backend_addr,
            #[cfg(test)]
            connection_settings,
            core: crate::pool::BackendPoolCore::new(
                MonoioBackendConnector::new(backend_addr, tls, socket),
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
        route: pg_kinetic_core::traffic::route::RouteKey,
        mode: crate::pool::CheckoutMode,
    ) -> Result<MonoioPooledBackend, crate::pool::PoolError> {
        self.core
            .checkout_with_mode(std::sync::Arc::clone(self), route, mode)
            .await
    }

    #[cfg(test)]
    pub(crate) fn backend_addr(&self) -> SocketAddr {
        self.backend_addr
    }

    #[cfg(test)]
    fn connection_settings(&self) -> BackendConnectionKey {
        self.connection_settings.clone()
    }
}

impl crate::proxy::SharedBackendPool<MonoioBackend, std::sync::Arc<MonoioBackendPool>>
    for std::sync::Arc<MonoioBackendPoolSelector>
{
    async fn checkout_shared(
        &self,
        route: pg_kinetic_core::traffic::route::RouteKey,
        route_pools: &RoutePools,
    ) -> Result<MonoioPooledBackend, crate::pool::PoolError> {
        let primary = route_pools.primary();
        let (tls, socket) = primary.backend_connection_settings();
        self.pool_for_route(&route, primary.backend_addr(), tls, socket)
            .checkout(route, crate::pool::CheckoutMode::AllowConnect)
            .await
    }

    async fn checkout_shared_target(
        &self,
        route: pg_kinetic_core::traffic::route::RouteKey,
        route_pools: &RoutePools,
        target: &RoutingTarget,
    ) -> Result<MonoioPooledBackend, crate::pool::PoolError> {
        let Some(pool) = self.pool_for_target(&route, route_pools, target) else {
            return Err(crate::pool::PoolError::Backpressure(
                pg_kinetic_core::traffic::backpressure::BackpressureError::Closed,
            ));
        };
        pool.checkout(route, crate::pool::CheckoutMode::AllowConnect)
            .await
    }
}

impl crate::proxy::BackendStartupMetadata for MonoioBackend {
    fn is_tls(&self) -> bool {
        MonoioBackend::is_tls(self)
    }

    fn addr(&self) -> std::net::SocketAddr {
        MonoioBackend::addr(self)
    }

    fn key_data(&self) -> Option<(i32, i32)> {
        MonoioBackend::key_data(self)
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

impl crate::engine::io_runtime::RuntimeByteStream for MonoioBackend {
    async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        crate::engine::io_runtime::read_from(&mut self.transport, dst).await
    }

    async fn write_all_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        crate::engine::io_runtime::write_all_to(&mut self.transport, bytes).await
    }

    async fn shutdown_stream(&mut self) -> std::io::Result<()> {
        crate::engine::io_runtime::shutdown(&mut self.transport).await
    }
}

impl crate::engine::io_runtime::RuntimeByteStream for MonoioTransport {
    async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        MonoioTransport::read_into(self, dst).await
    }

    async fn read_into_timeout(
        &mut self,
        dst: &mut BytesMut,
        duration: Duration,
    ) -> Result<std::io::Result<usize>, ()> {
        MonoioTransport::read_into_timeout(self, dst, duration).await
    }

    async fn write_all_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        MonoioTransport::write_all(self, bytes).await
    }

    async fn shutdown_stream(&mut self) -> std::io::Result<()> {
        MonoioTransport::shutdown(self).await
    }
}

impl crate::proxy::ClientTlsIo for MonoioTransport {
    fn is_tls(&self) -> bool {
        MonoioTransport::is_tls(self)
    }

    fn has_peer_certificates(&self) -> bool {
        MonoioTransport::has_peer_certificates(self)
    }

    async fn read_with_idle_timeout(
        &mut self,
        buffer: &mut BytesMut,
        idle_timeout: Duration,
    ) -> Result<std::io::Result<usize>, ()> {
        crate::engine::io_runtime::read_from_timeout(self, buffer, idle_timeout).await
    }

    async fn start_tls(
        &mut self,
        server_config: &Arc<ServerConfig>,
        require_client_certificate: bool,
    ) -> anyhow::Result<()> {
        MonoioTransport::start_tls(self, server_config, require_client_certificate).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::TlsConfig,
        pool::{BackendPool, BackendPoolRef, ReplicaSelectionStrategy, ReplicaSelector},
        routing::{ReplicaCandidate, RoutingReason},
    };
    use pg_kinetic_core::traffic::route::{QueryClass, RouteKey};

    fn route_pools(primary_addr: SocketAddr, replica_addr: SocketAddr) -> RoutePools {
        let primary = BackendPoolRef::primary(BackendPool::new(
            primary_addr,
            TlsConfig::default(),
            1,
            1,
            1,
            1,
            std::time::Duration::from_millis(500),
            "DISCARD ALL",
        ));
        let replica = BackendPoolRef::replica(
            7,
            1,
            BackendPool::new(
                replica_addr,
                TlsConfig::default(),
                1,
                1,
                1,
                1,
                std::time::Duration::from_millis(500),
                "DISCARD ALL",
            ),
        );
        RoutePools::new(
            primary,
            vec![replica],
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        )
    }

    #[test]
    fn selector_reuses_default_pool_and_keeps_route_pools_separate() {
        let default_addr = "127.0.0.1:5432".parse().expect("default address");
        let alternate_addr = "127.0.0.1:5433".parse().expect("alternate address");
        let alternate_addr_again = "127.0.0.1:5434".parse().expect("second alternate address");
        let selector = MonoioBackendPoolSelector::new(
            default_addr,
            TlsConfig::default(),
            SocketConfig::default(),
            Arc::new(tokio::sync::Semaphore::new(4)),
        );
        let route = RouteKey::new("app", "app", None, None, QueryClass::Default);

        let default_pool = selector.pool_for_route(
            &route,
            default_addr,
            TlsConfig::default(),
            SocketConfig::default(),
        );
        let alternate_pool = selector.pool_for_route(
            &route,
            alternate_addr,
            TlsConfig::default(),
            SocketConfig::default(),
        );
        let alternate_pool_again = selector.pool_for_route(
            &route,
            alternate_addr,
            TlsConfig::default(),
            SocketConfig::default(),
        );
        let alternate_pool_for_new_endpoint = selector.pool_for_route(
            &route,
            alternate_addr_again,
            TlsConfig::default(),
            SocketConfig::default(),
        );

        assert_eq!(default_pool.backend_addr(), default_addr);
        assert_eq!(alternate_pool.backend_addr(), alternate_addr);
        assert!(Arc::ptr_eq(&alternate_pool, &alternate_pool_again));
        assert_eq!(
            alternate_pool_for_new_endpoint.backend_addr(),
            alternate_addr_again
        );
        assert!(!Arc::ptr_eq(
            &alternate_pool,
            &alternate_pool_for_new_endpoint
        ));
        assert!(!Arc::ptr_eq(&default_pool, &alternate_pool));
    }

    #[test]
    fn selector_keeps_backend_tls_and_socket_settings_separate() {
        let default_addr = "127.0.0.1:5432".parse().expect("default address");
        let route = RouteKey::new("app", "app", None, None, QueryClass::Default);
        let selector = MonoioBackendPoolSelector::new(
            default_addr,
            TlsConfig::default(),
            SocketConfig::default(),
            Arc::new(tokio::sync::Semaphore::new(4)),
        );
        let mut require_tls = TlsConfig::default();
        require_tls.backend_tls_mode = BackendTlsMode::Require;
        let mut tuned_socket = SocketConfig::default();
        tuned_socket.tcp_recv_buffer_bytes = Some(65_536);

        let default_pool = selector.pool_for_route(
            &route,
            default_addr,
            TlsConfig::default(),
            SocketConfig::default(),
        );
        let tls_pool = selector.pool_for_route(
            &route,
            default_addr,
            require_tls.clone(),
            SocketConfig::default(),
        );
        let socket_pool = selector.pool_for_route(
            &route,
            default_addr,
            TlsConfig::default(),
            tuned_socket.clone(),
        );
        let tls_pool_again = selector.pool_for_route(
            &route,
            default_addr,
            require_tls.clone(),
            SocketConfig::default(),
        );

        assert!(Arc::ptr_eq(&tls_pool, &tls_pool_again));
        assert!(!Arc::ptr_eq(&default_pool, &tls_pool));
        assert!(!Arc::ptr_eq(&default_pool, &socket_pool));
        assert_eq!(tls_pool.connection_settings().tls, require_tls);
        assert_eq!(socket_pool.connection_settings().socket, tuned_socket);
    }

    #[test]
    fn selector_maps_primary_and_replica_targets_to_reusable_endpoint_pools() {
        let default_addr = "127.0.0.1:5432".parse().expect("default address");
        let replica_addr = "127.0.0.1:5433".parse().expect("replica address");
        let selector = MonoioBackendPoolSelector::new(
            default_addr,
            TlsConfig::default(),
            SocketConfig::default(),
            Arc::new(tokio::sync::Semaphore::new(4)),
        );
        let route = RouteKey::new("app", "app", None, None, QueryClass::Default);
        let route_pools = route_pools(default_addr, replica_addr);
        let primary_target = RoutingTarget::Primary {
            reason: RoutingReason::Off,
        };
        let replica_target = RoutingTarget::Replica {
            candidate: ReplicaCandidate::new(7, true, None, None),
            reason: RoutingReason::ReadCandidateQuery,
        };

        let primary_pool = selector
            .pool_for_target(&route, &route_pools, &primary_target)
            .expect("primary target pool");
        let primary_pool_again = selector
            .pool_for_target(&route, &route_pools, &primary_target)
            .expect("primary target pool reuse");
        let replica_pool = selector
            .pool_for_target(&route, &route_pools, &replica_target)
            .expect("replica target pool");
        let replica_pool_again = selector
            .pool_for_target(&route, &route_pools, &replica_target)
            .expect("replica target pool reuse");

        assert_eq!(primary_pool.backend_addr(), default_addr);
        assert!(Arc::ptr_eq(&primary_pool, &primary_pool_again));
        assert_eq!(replica_pool.backend_addr(), replica_addr);
        assert!(Arc::ptr_eq(&replica_pool, &replica_pool_again));
        assert!(!Arc::ptr_eq(&primary_pool, &replica_pool));
    }

    #[test]
    fn selector_does_not_create_pool_for_missing_or_non_checkout_targets() {
        let primary_addr = "127.0.0.1:5432".parse().expect("primary address");
        let replica_addr = "127.0.0.1:5433".parse().expect("replica address");
        let selector = MonoioBackendPoolSelector::new(
            primary_addr,
            TlsConfig::default(),
            SocketConfig::default(),
            Arc::new(tokio::sync::Semaphore::new(4)),
        );
        let route = RouteKey::new("app", "app", None, None, QueryClass::Default);
        let route_pools = route_pools(primary_addr, replica_addr);

        let unknown_replica = RoutingTarget::Replica {
            candidate: ReplicaCandidate::new(99, true, None, None),
            reason: RoutingReason::ReplicaUnavailable,
        };
        let wait_target = RoutingTarget::Wait {
            reason: RoutingReason::FallbackWait,
        };
        let reject_target = RoutingTarget::Reject {
            reason: RoutingReason::FallbackReject,
        };

        assert!(selector
            .pool_for_target(&route, &route_pools, &unknown_replica)
            .is_none());
        assert!(selector
            .pool_for_target(&route, &route_pools, &wait_target)
            .is_none());
        assert!(selector
            .pool_for_target(&route, &route_pools, &reject_target)
            .is_none());
    }
}
