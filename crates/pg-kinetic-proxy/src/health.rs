use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use anyhow::Context;
use bytes::{Buf, BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{sleep, timeout},
};

use crate::{
    backend::Backend,
    config::{SocketConfig, TlsConfig},
    drain::DrainController,
};
use pg_kinetic_core::security::{DrainState, HealthStatus};
use pg_kinetic_core::{
    ha::{
        EndpointHealth as EndpointHealthState, EndpointRoleState, HealthProbeOutcome,
        ReplicaLagState, RoleProbeOutcome, SplitBrainWarning,
    },
    routing::BackendRole,
};
use pg_kinetic_wire::{
    backend::parse_backend_frame,
    protocol::{BackendTag, FrontendTag, ProtocolVersion},
};

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

const UNAVAILABLE_FAILURE_THRESHOLD: u32 = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointHealthSnapshot {
    pub endpoint_id: u64,
    pub endpoint_addr: SocketAddr,
    pub expected_role: BackendRole,
    pub health: HealthProbeOutcome,
    pub role: RoleProbeOutcome,
    pub lag_state: ReplicaLagState,
    pub last_error: Option<String>,
}

impl EndpointHealthSnapshot {
    #[must_use]
    pub fn new(endpoint_id: u64, endpoint_addr: SocketAddr, expected_role: BackendRole) -> Self {
        Self {
            endpoint_id,
            endpoint_addr,
            expected_role,
            health: HealthProbeOutcome::new(EndpointHealthState::Unhealthy, false, 0),
            role: RoleProbeOutcome::new(EndpointRoleState::Unknown, None),
            lag_state: ReplicaLagState::Unknown,
            last_error: None,
        }
    }
}

#[derive(Debug)]
pub struct EndpointHealthProbe {
    endpoint_addr: SocketAddr,
    expected_role: BackendRole,
    probe_user: String,
    probe_database: String,
    tls_config: TlsConfig,
    socket_config: SocketConfig,
    probe_interval: Duration,
    probe_timeout: Duration,
    snapshot: StdMutex<EndpointHealthSnapshot>,
    publisher: watch::Sender<EndpointHealthSnapshot>,
}

impl EndpointHealthProbe {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_id: u64,
        endpoint_addr: SocketAddr,
        expected_role: BackendRole,
        probe_user: impl Into<String>,
        probe_database: impl Into<String>,
        tls_config: TlsConfig,
        socket_config: SocketConfig,
        probe_interval: Duration,
        probe_timeout: Duration,
    ) -> Arc<Self> {
        let snapshot = EndpointHealthSnapshot::new(endpoint_id, endpoint_addr, expected_role);
        let (publisher, _receiver) = watch::channel(snapshot.clone());
        Arc::new(Self {
            endpoint_addr,
            expected_role,
            probe_user: probe_user.into(),
            probe_database: probe_database.into(),
            tls_config,
            socket_config,
            probe_interval,
            probe_timeout,
            snapshot: StdMutex::new(snapshot),
            publisher,
        })
    }

    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<EndpointHealthSnapshot> {
        self.publisher.subscribe()
    }

    #[must_use]
    pub fn snapshot(&self) -> EndpointHealthSnapshot {
        self.snapshot
            .lock()
            .expect("endpoint health snapshot poisoned")
            .clone()
    }

    pub fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let probe = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                probe.probe_once().await;
                let delay = probe.backoff_delay();
                if delay.is_zero() {
                    tokio::task::yield_now().await;
                } else {
                    sleep(delay).await;
                }
            }
        })
    }

    pub async fn probe_once(&self) -> EndpointHealthSnapshot {
        let previous = self.snapshot();
        let outcome = timeout(self.probe_timeout, self.run_probe()).await;
        let mut snapshot = previous.clone();

        match outcome {
            Ok(Ok(observed_role)) => {
                let recovered = !previous.health.state.is_healthy();
                snapshot.health =
                    HealthProbeOutcome::new(EndpointHealthState::Healthy, recovered, 0);
                snapshot.role = role_outcome(self.expected_role, observed_role);
                snapshot.lag_state = ReplicaLagState::Unknown;
                snapshot.last_error = None;
            }
            Ok(Err(error)) => {
                let failure_count = previous.health.consecutive_failures.saturating_add(1);
                let health = if failure_count >= UNAVAILABLE_FAILURE_THRESHOLD {
                    EndpointHealthState::Unavailable
                } else {
                    EndpointHealthState::Unhealthy
                };
                snapshot.health = HealthProbeOutcome::new(health, false, failure_count);
                snapshot.role = RoleProbeOutcome::new(EndpointRoleState::Unknown, None);
                snapshot.lag_state = ReplicaLagState::Unknown;
                snapshot.last_error = Some(error.to_string());
            }
            Err(_) => {
                let failure_count = previous.health.consecutive_failures.saturating_add(1);
                let health = if failure_count >= UNAVAILABLE_FAILURE_THRESHOLD {
                    EndpointHealthState::Unavailable
                } else {
                    EndpointHealthState::Degraded
                };
                snapshot.health = HealthProbeOutcome::new(health, false, failure_count);
                snapshot.role = RoleProbeOutcome::new(EndpointRoleState::Unknown, None);
                snapshot.lag_state = ReplicaLagState::Unknown;
                snapshot.last_error = Some(String::from("probe timed out"));
            }
        }

        self.publish(snapshot.clone());
        snapshot
    }

    fn backoff_delay(&self) -> Duration {
        let snapshot = self.snapshot();
        if snapshot.health.state.is_healthy() {
            return self.probe_interval;
        }

        let exponent = snapshot
            .health
            .consecutive_failures
            .saturating_sub(1)
            .min(4);
        let multiplier = 1_u32 << exponent;
        self.probe_interval.saturating_mul(multiplier)
    }

    fn publish(&self, snapshot: EndpointHealthSnapshot) {
        *self
            .snapshot
            .lock()
            .expect("endpoint health snapshot poisoned") = snapshot.clone();
        let _ = self.publisher.send(snapshot);
    }

    async fn run_probe(&self) -> anyhow::Result<BackendRole> {
        let mut backend =
            Backend::connect_with_socket(self.endpoint_addr, &self.tls_config, &self.socket_config)
                .await
                .with_context(|| format!("connect endpoint {}", self.endpoint_addr))?;

        backend
            .stream_mut()
            .write_all(&startup_packet(&self.probe_user, &self.probe_database))
            .await
            .context("write probe startup packet")?;

        execute_probe_query(&mut backend, "SELECT 1").await?;
        let role = execute_probe_query(&mut backend, "SELECT pg_is_in_recovery()").await?;

        match role.as_deref() {
            Some("t") | Some("true") | Some("1") => Ok(BackendRole::Replica),
            Some("f") | Some("false") | Some("0") => Ok(BackendRole::Primary),
            Some(other) => anyhow::bail!("unexpected pg_is_in_recovery() result: {other}"),
            None => anyhow::bail!("pg_is_in_recovery() returned no rows"),
        }
    }
}

fn role_outcome(expected_role: BackendRole, observed_role: BackendRole) -> RoleProbeOutcome {
    if expected_role == observed_role {
        let state = match observed_role {
            BackendRole::Primary => EndpointRoleState::Primary,
            BackendRole::Replica => EndpointRoleState::Replica,
            BackendRole::Unknown => EndpointRoleState::Unknown,
        };
        return RoleProbeOutcome::new(state, None);
    }

    let warning = SplitBrainWarning::new(expected_role, observed_role);
    RoleProbeOutcome::new(EndpointRoleState::Warning, Some(warning))
}

async fn execute_probe_query(backend: &mut Backend, sql: &str) -> anyhow::Result<Option<String>> {
    backend
        .stream_mut()
        .write_all(&simple_query_packet(sql))
        .await
        .with_context(|| format!("write probe query {sql}"))?;

    let mut buffer = BytesMut::with_capacity(8 * 1024);
    let mut row_value = None;

    loop {
        let read = backend
            .stream_mut()
            .read_buf(&mut buffer)
            .await
            .with_context(|| format!("read probe response for {sql}"))?;
        if read == 0 {
            anyhow::bail!("endpoint disconnected during probe");
        }

        while let Some(frame) = parse_backend_frame(&mut buffer)? {
            match frame.tag {
                tag if tag == u8::from(BackendTag::DataRow) => {
                    if row_value.is_none() {
                        row_value = parse_probe_row(&frame.payload)?;
                    }
                }
                tag if tag == u8::from(BackendTag::ErrorResponse) => {
                    anyhow::bail!("endpoint returned error during probe");
                }
                tag if tag == u8::from(BackendTag::ReadyForQuery) => {
                    return Ok(row_value);
                }
                _ => {}
            }
        }
    }
}

fn parse_probe_row(payload: &[u8]) -> anyhow::Result<Option<String>> {
    let mut payload = payload;
    if payload.remaining() < 2 {
        return Ok(None);
    }

    let columns = payload.get_i16();
    if columns <= 0 {
        return Ok(None);
    }

    let length = payload.get_i32();
    if length < 0 {
        return Ok(None);
    }

    let length = length as usize;
    if payload.remaining() < length {
        return Ok(None);
    }

    let mut value = vec![0_u8; length];
    payload.copy_to_slice(&mut value);
    let value = String::from_utf8(value).context("probe row is not utf8")?;
    Ok(Some(value))
}

fn startup_packet(user: &str, database: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"application_name\0pg-kinetic-health\0");
    body.put_u8(0);

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet
}

fn simple_query_packet(sql: &str) -> BytesMut {
    let mut packet = BytesMut::new();
    packet.put_u8(u8::from(FrontendTag::Query));
    packet.put_i32((sql.len() + 5) as i32);
    packet.extend_from_slice(sql.as_bytes());
    packet.put_u8(0);
    packet
}
