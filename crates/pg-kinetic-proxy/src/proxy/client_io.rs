use super::*;

pub(super) async fn next_client_cycle<C>(
    client: &mut C,
    client_buffer: &mut BytesMut,
    idle_timeout: Option<Duration>,
    idle_timeout_kind: IdleTimeoutKind,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<Option<ClientCycle>>
where
    C: crate::engine::io_runtime::RuntimeByteStream + ?Sized,
{
    loop {
        match crate::engine::io_runtime::take_frontend_cycle_bytes(
            client_buffer,
            max_client_buffer_bytes,
        )? {
            crate::engine::io_runtime::FrontendCycleRead::Complete { bytes, .. } => {
                return Ok(Some(ClientCycle::Frames(
                    crate::engine::io_runtime::parse_frontend_cycle_frames(bytes)?,
                )));
            }
            crate::engine::io_runtime::FrontendCycleRead::Terminate { .. } => {
                return Ok(Some(ClientCycle::Terminate));
            }
            crate::engine::io_runtime::FrontendCycleRead::BufferLimitExceeded => {
                return Ok(Some(ClientCycle::BufferLimitExceeded));
            }
            crate::engine::io_runtime::FrontendCycleRead::NeedMoreBytes => {
                if client_buffer.len() >= max_client_buffer_bytes {
                    return Ok(Some(ClientCycle::BufferLimitExceeded));
                }

                match idle_timeout {
                    Some(duration) => match crate::engine::io_runtime::tokio_timeout(
                        duration,
                        crate::engine::io_runtime::read_from(client, client_buffer),
                    )
                    .await
                    {
                        Ok(Ok(0)) => return Ok(Some(ClientCycle::Terminate)),
                        Ok(Ok(_)) => {
                            if client_buffer.len() > max_client_buffer_bytes {
                                return Ok(Some(ClientCycle::BufferLimitExceeded));
                            }
                            continue;
                        }
                        Ok(Err(error)) => return Err(error).context("read client"),
                        Err(_) => return Ok(Some(ClientCycle::IdleTimeout(idle_timeout_kind))),
                    },
                    None => {
                        if crate::engine::io_runtime::read_from(client, client_buffer)
                            .await
                            .context("read client")?
                            == 0
                        {
                            return Ok(Some(ClientCycle::Terminate));
                        }

                        if client_buffer.len() > max_client_buffer_bytes {
                            return Ok(Some(ClientCycle::BufferLimitExceeded));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub(crate) async fn handle_startup_or_cancel<C>(
    client: &mut C,
    client_buffer: &mut BytesMut,
    client_tls_mode: crate::config::ClientTlsMode,
    client_tls_server_config: Option<&Arc<ServerConfig>>,
    idle_timeout: Duration,
    max_client_buffer_bytes: usize,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<StartupOrCancel>
where
    C: ClientTlsIo + ?Sized,
{
    match read_startup_packet_with_buffer(
        client,
        client_tls_mode,
        client_tls_server_config,
        idle_timeout,
        max_client_buffer_bytes,
        client_buffer,
        phase_recorder,
    )
    .await?
    {
        StartupRead::Packet(bytes) => Ok(StartupOrCancel::Startup(bytes)),
        StartupRead::Cancel {
            process_id,
            secret_key,
        } => Ok(StartupOrCancel::Cancel {
            process_id,
            secret_key,
        }),
        StartupRead::ClientClosed => Ok(StartupOrCancel::Finished),
        StartupRead::TimedOut => {
            let error =
                build_error_response(SqlState::OperatorIntervention.as_str(), "startup timed out");
            let mut response = BytesMut::with_capacity(error.len() + 6);
            response.extend_from_slice(&error);
            response.extend_from_slice(&ready_for_query(ReadyStatus::Idle));
            crate::engine::io_runtime::write_all_to(client, &response)
                .await
                .context("write startup timeout response")?;
            Ok(StartupOrCancel::Finished)
        }
        StartupRead::BufferLimitExceeded => Err(buffer_limit_exceeded(BufferBudgetKind::Client)),
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
#[derive(Debug)]
pub(crate) enum StartupOrCancel {
    Startup(BytesMut),
    Cancel { process_id: i32, secret_key: i32 },
    Finished,
}

pub(crate) trait ClientTlsIo: crate::engine::io_runtime::RuntimeByteStream {
    fn is_tls(&self) -> bool;

    fn has_peer_certificates(&self) -> bool;

    async fn read_with_idle_timeout(
        &mut self,
        buffer: &mut BytesMut,
        idle_timeout: Duration,
    ) -> Result<std::io::Result<usize>, ()>;

    async fn start_tls(
        &mut self,
        server_config: &Arc<ServerConfig>,
        require_client_certificate: bool,
    ) -> anyhow::Result<()>;
}

impl ClientTlsIo for ClientConnection {
    fn is_tls(&self) -> bool {
        ClientConnection::is_tls(self)
    }

    fn has_peer_certificates(&self) -> bool {
        ClientConnection::has_peer_certificates(self)
    }

    async fn read_with_idle_timeout(
        &mut self,
        buffer: &mut BytesMut,
        idle_timeout: Duration,
    ) -> Result<std::io::Result<usize>, ()> {
        crate::engine::io_runtime::tokio_timeout(
            idle_timeout,
            crate::engine::io_runtime::read_from(self, buffer),
        )
        .await
    }

    async fn start_tls(
        &mut self,
        server_config: &Arc<ServerConfig>,
        _require_client_certificate: bool,
    ) -> anyhow::Result<()> {
        ClientConnection::start_tls(self, server_config).await
    }
}

#[derive(Debug)]
pub(super) enum ClientCycle {
    Frames(Vec<FrontendFrame>),
    Terminate,
    IdleTimeout(IdleTimeoutKind),
    BufferLimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IdleTimeoutKind {
    Client,
    Transaction,
}

#[derive(Default)]
pub(super) struct QueryProgress {
    pub(super) response_started: bool,
    pub(super) rows: u64,
    pub(super) error: bool,
}

pub(super) struct CancelSessionGuard {
    registry: Arc<cancel::CancelRegistry>,
    key: (i32, i32),
}

impl CancelSessionGuard {
    pub(super) fn new(registry: Arc<cancel::CancelRegistry>, key: (i32, i32)) -> Self {
        Self { registry, key }
    }
}

impl Drop for CancelSessionGuard {
    fn drop(&mut self) {
        self.registry.remove_session(self.key);
    }
}

pub(super) fn bind_cancel_target(
    registry: &cancel::CancelRegistry,
    client_key: (i32, i32),
    backend: &PooledBackend,
) {
    bind_cancel_target_for_backend(registry, client_key, backend);
}

pub(super) fn bind_cancel_target_for_backend<B, O>(
    registry: &cancel::CancelRegistry,
    client_key: (i32, i32),
    backend: &crate::pool::PooledBackendLease<B, O>,
) where
    B: crate::pool::PoolBackendTransport + crate::proxy::BackendStartupMetadata,
    O: crate::pool::BackendLeaseOwner<B>,
{
    if let Some((process_id, secret_key)) = backend.backend().key_data() {
        registry.bind(
            client_key,
            cancel::CancelTarget {
                backend_addr: backend.backend().addr(),
                process_id,
                secret_key,
            },
        );
    }
}

pub(super) async fn release_backend_with_cancel_unbind<B, O>(
    registry: &cancel::CancelRegistry,
    client_key: (i32, i32),
    backend: crate::pool::PooledBackendLease<B, O>,
) where
    B: crate::pool::PoolBackendTransport,
    O: crate::pool::BackendLeaseOwner<B>,
{
    registry.unbind(client_key).await;
    backend.release().await;
}

pub(super) async fn discard_backend_with_cancel_unbind<B, O>(
    registry: &cancel::CancelRegistry,
    client_key: (i32, i32),
    backend: crate::pool::PooledBackendLease<B, O>,
) where
    B: crate::pool::PoolBackendTransport,
    O: crate::pool::BackendLeaseOwner<B>,
{
    registry.unbind(client_key).await;
    backend.discard();
}

pub(crate) async fn read_startup_packet(
    client: &mut ClientConnection,
    client_tls_mode: crate::config::ClientTlsMode,
    client_tls_server_config: Option<&Arc<ServerConfig>>,
    idle_timeout: Duration,
    max_client_buffer_bytes: usize,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<StartupRead> {
    let mut buffer = BytesMut::with_capacity(8192);
    read_startup_packet_with_buffer(
        client,
        client_tls_mode,
        client_tls_server_config,
        idle_timeout,
        max_client_buffer_bytes,
        &mut buffer,
        phase_recorder,
    )
    .await
}

pub(super) async fn read_startup_packet_with_buffer<C>(
    client: &mut C,
    client_tls_mode: crate::config::ClientTlsMode,
    client_tls_server_config: Option<&Arc<ServerConfig>>,
    idle_timeout: Duration,
    max_client_buffer_bytes: usize,
    buffer: &mut BytesMut,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<StartupRead>
where
    C: ClientTlsIo + ?Sized,
{
    let client_tls_required = matches!(
        client_tls_mode,
        crate::config::ClientTlsMode::Require | crate::config::ClientTlsMode::VerifyClient
    );
    loop {
        match crate::engine::io_runtime::take_startup_packet_bytes(buffer, max_client_buffer_bytes)?
        {
            crate::engine::io_runtime::StartupPacketRead::Packet(packet) => {
                if client_tls_required && !client.is_tls() {
                    anyhow::bail!("client TLS is required");
                }
                return Ok(StartupRead::Packet(packet));
            }
            crate::engine::io_runtime::StartupPacketRead::Cancel {
                process_id,
                secret_key,
                ..
            } => {
                return Ok(StartupRead::Cancel {
                    process_id,
                    secret_key,
                });
            }
            crate::engine::io_runtime::StartupPacketRead::EncryptionRequest(
                crate::engine::io_runtime::StartupEncryptionRequest::Ssl,
            ) => {
                match client_tls_mode {
                    crate::config::ClientTlsMode::Disable => {
                        reject_startup_encryption_request(client).await?;
                    }
                    crate::config::ClientTlsMode::Allow
                    | crate::config::ClientTlsMode::Require
                    | crate::config::ClientTlsMode::VerifyClient => {
                        crate::engine::io_runtime::write_all_to(client, b"S")
                            .await
                            .context("accept startup encryption request")?;
                        let server_config = client_tls_server_config
                            .context("client TLS server config is unavailable")?;
                        let tls_timer =
                            PhaseTimer::start(ProtocolPhase::TlsHandshake, phase_recorder);
                        let require_client_certificate =
                            matches!(client_tls_mode, crate::config::ClientTlsMode::VerifyClient);
                        let tls_result = client
                            .start_tls(server_config, require_client_certificate)
                            .await;
                        let tls_outcome = match &tls_result {
                            Ok(())
                                if matches!(
                                    client_tls_mode,
                                    crate::config::ClientTlsMode::VerifyClient
                                ) && !client.has_peer_certificates() =>
                            {
                                MetricOutcome::Rejected
                            }
                            Ok(()) => MetricOutcome::Ok,
                            Err(_) => MetricOutcome::Error,
                        };
                        tls_timer.finish(tls_outcome);
                        tls_result?;
                        if matches!(client_tls_mode, crate::config::ClientTlsMode::VerifyClient)
                            && !client.has_peer_certificates()
                        {
                            anyhow::bail!("client certificate is required");
                        }
                        buffer.clear();
                    }
                }
                continue;
            }
            crate::engine::io_runtime::StartupPacketRead::EncryptionRequest(
                crate::engine::io_runtime::StartupEncryptionRequest::Gss,
            ) => {
                reject_startup_encryption_request(client).await?;
                continue;
            }
            crate::engine::io_runtime::StartupPacketRead::BufferLimitExceeded => {
                return Ok(StartupRead::BufferLimitExceeded);
            }
            crate::engine::io_runtime::StartupPacketRead::NeedMoreBytes => {
                if buffer.len() >= max_client_buffer_bytes {
                    return Ok(StartupRead::BufferLimitExceeded);
                }

                match client.read_with_idle_timeout(buffer, idle_timeout).await {
                    Ok(Ok(0)) => return Ok(StartupRead::ClientClosed),
                    Ok(Ok(_)) => {
                        if buffer.len() > max_client_buffer_bytes {
                            return Ok(StartupRead::BufferLimitExceeded);
                        }
                        continue;
                    }
                    Ok(Err(error)) => return Err(error).context("read startup"),
                    Err(_) => return Ok(StartupRead::TimedOut),
                }
            }
        }
    }
}

pub(super) async fn reject_startup_encryption_request<C>(client: &mut C) -> anyhow::Result<()>
where
    C: crate::engine::io_runtime::RuntimeByteStream + ?Sized,
{
    crate::engine::io_runtime::write_all_to(client, b"N")
        .await
        .context("reject startup encryption request")
}

#[derive(Debug)]
pub(crate) enum StartupRead {
    Packet(BytesMut),
    Cancel { process_id: i32, secret_key: i32 },
    ClientClosed,
    TimedOut,
    BufferLimitExceeded,
}
