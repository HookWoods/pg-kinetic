use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use crate::{
    backend::Backend,
    config::{SocketConfig, TlsConfig},
    drain::DrainController,
};
use pg_kinetic_core::security::{DrainState, HealthStatus};

#[derive(Debug)]
pub struct HealthState {
    drain: Arc<DrainController>,
    backend: Arc<BackendHealthProbe>,
}

#[derive(Debug)]
pub struct BackendHealthProbe {
    backend_addr: SocketAddr,
    tls_config: TlsConfig,
    socket_config: SocketConfig,
    readiness_timeout: Duration,
    readiness_backend_check_interval: Duration,
    status: AtomicU8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    pub process: HealthStatus,
    pub ready: HealthStatus,
    pub drain_state: DrainState,
    pub active_clients: usize,
    pub backend_health: HealthStatus,
}

impl HealthState {
    #[must_use]
    pub fn new(drain: Arc<DrainController>, backend: Arc<BackendHealthProbe>) -> Arc<Self> {
        Arc::new(Self { drain, backend })
    }

    #[must_use]
    pub fn snapshot(&self) -> HealthSnapshot {
        let backend_health = self.backend.status();
        let ready = if self.is_ready() {
            HealthStatus::Ready
        } else {
            HealthStatus::NotReady
        };

        HealthSnapshot {
            process: HealthStatus::Live,
            ready,
            drain_state: self.drain.state(),
            active_clients: self.drain.active_clients(),
            backend_health,
        }
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.drain.is_accepting() && self.backend.is_ready()
    }
}

impl BackendHealthProbe {
    #[must_use]
    pub fn new(
        backend_addr: SocketAddr,
        tls_config: TlsConfig,
        socket_config: SocketConfig,
        readiness_timeout: Duration,
        readiness_backend_check_interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend_addr,
            tls_config,
            socket_config,
            readiness_timeout,
            readiness_backend_check_interval,
            status: AtomicU8::new(Self::status_to_u8(HealthStatus::NotReady)),
        })
    }

    #[must_use]
    pub fn status(&self) -> HealthStatus {
        Self::status_from_u8(self.status.load(Ordering::Acquire))
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.status(), HealthStatus::Ready)
    }

    pub fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let probe = Arc::clone(self);
        tokio::spawn(async move {
            probe.run().await;
        })
    }

    async fn run(self: Arc<Self>) {
        loop {
            self.refresh_once().await;
            if self.readiness_backend_check_interval.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(self.readiness_backend_check_interval).await;
            }
        }
    }

    async fn refresh_once(&self) {
        let status = match timeout(
            self.readiness_timeout,
            Backend::connect_with_socket(self.backend_addr, &self.tls_config, &self.socket_config),
        )
        .await
        {
            Ok(Ok(backend)) => {
                drop(backend);
                HealthStatus::Ready
            }
            Ok(Err(error)) => {
                tracing::debug!(backend_addr = %self.backend_addr, error = %error, "backend health probe failed");
                HealthStatus::NotReady
            }
            Err(_) => {
                tracing::debug!(backend_addr = %self.backend_addr, "backend health probe timed out");
                HealthStatus::NotReady
            }
        };

        self.status
            .store(Self::status_to_u8(status), Ordering::Release);
    }

    const fn status_to_u8(status: HealthStatus) -> u8 {
        match status {
            HealthStatus::Ready => 0,
            HealthStatus::NotReady => 1,
            HealthStatus::Live => 2,
            HealthStatus::Degraded => 3,
        }
    }

    const fn status_from_u8(status: u8) -> HealthStatus {
        match status {
            0 => HealthStatus::Ready,
            1 => HealthStatus::NotReady,
            2 => HealthStatus::Live,
            _ => HealthStatus::Degraded,
        }
    }
}

pub async fn spawn(
    listen_addr: SocketAddr,
    drain: Arc<DrainController>,
    backend_addr: SocketAddr,
    tls_config: TlsConfig,
    socket_config: SocketConfig,
    readiness_timeout: Duration,
    readiness_backend_check_interval: Duration,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("bind health listener {listen_addr}"))?;
    tracing::info!(%listen_addr, "health listener enabled");

    let backend = BackendHealthProbe::new(
        backend_addr,
        tls_config,
        socket_config,
        readiness_timeout,
        readiness_backend_check_interval,
    );
    let _probe_handle = backend.start();
    let state = HealthState::new(drain, backend);

    Ok(tokio::spawn(async move {
        run_server(listener, state).await;
    }))
}

async fn run_server(listener: TcpListener, state: Arc<HealthState>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(error = %error, "health listener accept failed");
                continue;
            }
        };

        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, state).await {
                tracing::debug!(error = %error, "health request failed");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, state: Arc<HealthState>) -> anyhow::Result<()> {
    let request = read_request(&mut stream).await?;
    let path = request_path(&request).unwrap_or("/");
    let response = match path {
        "/healthz" => text_response(200, state.snapshot().process.as_str()),
        "/readyz" => {
            let snapshot = state.snapshot();
            if snapshot.ready == HealthStatus::Ready {
                text_response(200, snapshot.ready.as_str())
            } else {
                text_response(503, snapshot.ready.as_str())
            }
        }
        "/state" => json_response(200, &snapshot_body(&state.snapshot())),
        _ => text_response(404, "not_found"),
    };

    stream
        .write_all(&response)
        .await
        .context("write health response")?;
    stream.shutdown().await.context("shutdown health stream")?;
    Ok(())
}

async fn read_request(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .context("read health request")?;
        if read == 0 {
            break;
        }

        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() >= 4 && buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if buffer.len() > 8 * 1024 {
            break;
        }
    }

    Ok(buffer)
}

fn request_path(request: &[u8]) -> Option<&str> {
    let request = std::str::from_utf8(request).ok()?;
    let line = request.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;

    if method == "GET" {
        Some(path)
    } else {
        None
    }
}

fn text_response(status: u16, body: &str) -> Vec<u8> {
    response(status, "text/plain; charset=utf-8", body)
}

fn json_response(status: u16, body: &str) -> Vec<u8> {
    response(status, "application/json; charset=utf-8", body)
}

fn response(status: u16, content_type: &str, body: &str) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "OK",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn snapshot_body(snapshot: &HealthSnapshot) -> String {
    format!(
        r#"{{"process":"{}","ready":"{}","drain_state":"{}","active_clients":{},"backend_health":"{}"}}"#,
        snapshot.process.as_str(),
        snapshot.ready.as_str(),
        snapshot.drain_state.as_str(),
        snapshot.active_clients,
        snapshot.backend_health.as_str()
    )
}
