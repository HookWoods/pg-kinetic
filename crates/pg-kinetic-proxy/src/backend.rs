use std::{
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

use anyhow::Context as AnyhowContext;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::rustls::{pki_types::ServerName, ClientConfig};

use crate::{
    config::{BackendTlsMode, TlsConfig},
    tls,
};
use pg_kinetic_wire::tls::{ssl_request_packet, SslResponse};

static NEXT_BACKEND_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Backend {
    id: u64,
    stream: BackendStream,
    addr: SocketAddr,
}

impl Backend {
    pub async fn connect(addr: SocketAddr, tls_config: &TlsConfig) -> anyhow::Result<Self> {
        let tls_settings = if matches!(tls_config.backend_tls_mode, BackendTlsMode::Disable) {
            None
        } else {
            Some(tls::backend_tls_settings(tls_config)?)
        };

        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect backend {addr}"))?;
        stream
            .set_nodelay(true)
            .context("set backend TCP_NODELAY")?;

        if matches!(tls_config.backend_tls_mode, BackendTlsMode::Disable) {
            return Ok(Self {
                id: NEXT_BACKEND_ID.fetch_add(1, Ordering::Relaxed),
                stream: BackendStream::Plain(stream),
                addr,
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

    pub fn stream_mut(&mut self) -> &mut BackendStream {
        &mut self.stream
    }
}

#[derive(Debug)]
enum BackendConnectStream {
    Plain(TcpStream),
    Tls(ClientTlsStream<TcpStream>),
}

#[derive(Debug)]
pub enum BackendStream {
    Plain(TcpStream),
    Tls(ClientTlsStream<TcpStream>),
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
