use std::{
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Instant,
};

use anyhow::Context as AnyhowContext;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::rustls::{pki_types::ServerName, ClientConfig};

use crate::{
    config::{BackendTlsMode, SocketConfig, TlsConfig},
    metrics,
    snapshot::{ServerSnapshot, SnapshotStore},
    socket, tls,
};
use pg_kinetic_core::route::RouteKey;
use pg_kinetic_wire::tls::{ssl_request_packet, SslResponse};

static NEXT_BACKEND_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Backend {
    id: u64,
    stream: BackendStream,
    addr: SocketAddr,
    connected_at: Instant,
    snapshot_store: Option<SnapshotStore>,
    parameter_status: Vec<(String, String)>,
}

impl Backend {
    pub async fn connect(addr: SocketAddr, tls_config: &TlsConfig) -> anyhow::Result<Self> {
        Self::connect_with_socket(addr, tls_config, &SocketConfig::default()).await
    }

    pub async fn connect_with_socket(
        addr: SocketAddr,
        tls_config: &TlsConfig,
        socket_config: &SocketConfig,
    ) -> anyhow::Result<Self> {
        let tls_settings = if matches!(tls_config.backend_tls_mode, BackendTlsMode::Disable) {
            None
        } else {
            Some(tls::backend_tls_settings(tls_config)?)
        };

        let socket_options = socket::SocketOptions::from(socket_config);

        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect backend {addr}"))?;
        socket::apply_socket_options(&stream, &socket_options, "backend")
            .context("apply backend socket options")?;

        if matches!(tls_config.backend_tls_mode, BackendTlsMode::Disable) {
            return Ok(Self {
                id: NEXT_BACKEND_ID.fetch_add(1, Ordering::Relaxed),
                stream: BackendStream::Plain(stream),
                addr,
                connected_at: Instant::now(),
                snapshot_store: None,
                parameter_status: Vec::new(),
            });
        }

        let stream = match tls_config.backend_tls_mode {
            BackendTlsMode::Disable => unreachable!(),
            BackendTlsMode::Prefer | BackendTlsMode::Require => {
                match negotiate_backend_tls(
                    stream,
                    tls_config.backend_tls_mode,
                    tls_settings.expect("backend TLS settings are present"),
                )
                .await?
                {
                    BackendConnectStream::Plain(stream) => BackendStream::Plain(stream),
                    BackendConnectStream::Tls(stream) => BackendStream::Tls(stream),
                }
            }
            BackendTlsMode::VerifyCa | BackendTlsMode::VerifyFull => {
                match negotiate_backend_tls(
                    stream,
                    tls_config.backend_tls_mode,
                    tls_settings.expect("backend TLS settings are present"),
                )
                .await?
                {
                    BackendConnectStream::Tls(stream) => BackendStream::Tls(stream),
                    BackendConnectStream::Plain(_) => {
                        anyhow::bail!("backend TLS was disabled by the server")
                    }
                }
            }
        };

        Ok(Self {
            id: NEXT_BACKEND_ID.fetch_add(1, Ordering::Relaxed),
            stream,
            addr,
            connected_at: Instant::now(),
            snapshot_store: None,
            parameter_status: Vec::new(),
        })
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
        self.stream.is_tls()
    }

    pub fn attach_snapshot_store(&mut self, snapshot_store: SnapshotStore) {
        self.snapshot_store = Some(snapshot_store);
    }

    pub fn mark_checked_out(&self, route_key: Option<RouteKey>) {
        self.publish_snapshot("checked_out", route_key);
    }

    pub fn mark_idle(&self, route_key: Option<RouteKey>) {
        self.publish_snapshot("idle", route_key);
    }

    pub fn mark_discarded(&self) {
        if let Some(snapshot_store) = self.snapshot_store.as_ref() {
            metrics::remove_server_snapshot(snapshot_store, self.id);
        }
    }

    pub fn stream_mut(&mut self) -> &mut BackendStream {
        &mut self.stream
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

    fn publish_snapshot(&self, state: &'static str, route_key: Option<RouteKey>) {
        if let Some(snapshot_store) = self.snapshot_store.as_ref() {
            let mut snapshot = ServerSnapshot::new(self.id, state, self.connected_at.elapsed());
            snapshot.route_key = route_key;
            metrics::record_server_snapshot(snapshot_store, snapshot);
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum BackendConnectStream {
    Plain(TcpStream),
    Tls(ClientTlsStream<TcpStream>),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum BackendStream {
    Plain(TcpStream),
    Tls(ClientTlsStream<TcpStream>),
}

impl BackendStream {
    #[must_use]
    pub const fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }
}

impl AsyncRead for BackendStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::Tls(stream) => Pin::new(stream).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for BackendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(context, buffer),
            Self::Tls(stream) => Pin::new(stream).poll_write(context, buffer),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(context),
            Self::Tls(stream) => Pin::new(stream).poll_flush(context),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(context),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(context),
        }
    }
}

async fn negotiate_backend_tls(
    mut stream: TcpStream,
    tls_mode: BackendTlsMode,
    tls_settings: (Arc<ClientConfig>, ServerName<'static>),
) -> anyhow::Result<BackendConnectStream> {
    let (client_config, server_name) = tls_settings;

    stream
        .write_all(&ssl_request_packet())
        .await
        .context("send backend SSLRequest")?;

    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .context("read backend SSLResponse")?;

    match response[0] {
        value if value == u8::from(SslResponse::Accept) => {
            let tls_stream = tls::connect_backend_tls(stream, client_config, server_name).await?;
            Ok(BackendConnectStream::Tls(tls_stream))
        }
        value if value == u8::from(SslResponse::Deny) => match tls_mode {
            BackendTlsMode::Prefer => Ok(BackendConnectStream::Plain(stream)),
            BackendTlsMode::Require | BackendTlsMode::VerifyCa | BackendTlsMode::VerifyFull => {
                anyhow::bail!("backend denied TLS negotiation")
            }
            BackendTlsMode::Disable => unreachable!(),
        },
        other => anyhow::bail!("unexpected backend SSLResponse byte {other:#04x}"),
    }
}
