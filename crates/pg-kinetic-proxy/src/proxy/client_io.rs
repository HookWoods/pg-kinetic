use super::*;

pub(super) async fn next_client_cycle(
    client: &mut ClientConnection,
    client_buffer: &mut BytesMut,
    idle_timeout: Option<Duration>,
    idle_timeout_kind: IdleTimeoutKind,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<Option<ClientCycle>> {
    let first = loop {
        if let Some(frame) = parse_frontend_frame(client_buffer)? {
            break frame;
        }

        if client_buffer.len() >= max_client_buffer_bytes {
            return Ok(Some(ClientCycle::BufferLimitExceeded));
        }

        match idle_timeout {
            Some(duration) => match timeout(duration, client.read_buf(client_buffer)).await {
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
                if client
                    .read_buf(client_buffer)
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
    };

    if first.tag == u8::from(FrontendTag::Terminate) {
        return Ok(Some(ClientCycle::Terminate));
    }

    if first.tag == u8::from(FrontendTag::Query) {
        return Ok(Some(ClientCycle::Frames(vec![first])));
    }

    let mut frames = vec![first];
    while !frames
        .iter()
        .any(|frame| frame.tag == u8::from(FrontendTag::Sync))
    {
        if let Some(frame) = parse_frontend_frame(client_buffer)? {
            frames.push(frame);
            continue;
        }

        if client_buffer.len() >= max_client_buffer_bytes {
            return Ok(Some(ClientCycle::BufferLimitExceeded));
        }

        match idle_timeout {
            Some(duration) => match timeout(duration, client.read_buf(client_buffer)).await {
                Ok(Ok(0)) => return Ok(Some(ClientCycle::Terminate)),
                Ok(Ok(_)) => {
                    if client_buffer.len() > max_client_buffer_bytes {
                        return Ok(Some(ClientCycle::BufferLimitExceeded));
                    }
                    continue;
                }
                Ok(Err(error)) => return Err(error).context("read extended query frame"),
                Err(_) => return Ok(Some(ClientCycle::IdleTimeout(idle_timeout_kind))),
            },
            None => {
                if client
                    .read_buf(client_buffer)
                    .await
                    .context("read extended query frame")?
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

    Ok(Some(ClientCycle::Frames(frames)))
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

pub(super) async fn release_backend_with_cancel_unbind(
    registry: &cancel::CancelRegistry,
    client_key: (i32, i32),
    backend: PooledBackend,
) {
    registry.unbind(client_key).await;
    backend.release().await;
}

pub(super) async fn discard_backend_with_cancel_unbind(
    registry: &cancel::CancelRegistry,
    client_key: (i32, i32),
    backend: PooledBackend,
) {
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

pub(super) async fn read_startup_packet_with_buffer(
    client: &mut ClientConnection,
    client_tls_mode: crate::config::ClientTlsMode,
    client_tls_server_config: Option<&Arc<ServerConfig>>,
    idle_timeout: Duration,
    max_client_buffer_bytes: usize,
    buffer: &mut BytesMut,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<StartupRead> {
    let client_tls_required = matches!(
        client_tls_mode,
        crate::config::ClientTlsMode::Require | crate::config::ClientTlsMode::VerifyClient
    );
    loop {
        while let Some(packet) = next_startup_packet(buffer)? {
            match parse_startup_packet(&packet) {
                Ok(StartupPacket::SslRequest) => {
                    match client_tls_mode {
                        crate::config::ClientTlsMode::Disable => {
                            reject_startup_encryption_request(client).await?;
                        }
                        crate::config::ClientTlsMode::Allow
                        | crate::config::ClientTlsMode::Require
                        | crate::config::ClientTlsMode::VerifyClient => {
                            client
                                .write_all(b"S")
                                .await
                                .context("accept startup encryption request")?;
                            let server_config = client_tls_server_config
                                .context("client TLS server config is unavailable")?;
                            let tls_timer =
                                PhaseTimer::start(ProtocolPhase::TlsHandshake, phase_recorder);
                            let tls_result = client.start_tls(server_config).await;
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
                Ok(StartupPacket::GssEncRequest) => {
                    reject_startup_encryption_request(client).await?;
                    continue;
                }
                Ok(StartupPacket::Startup { .. }) if client_tls_required && !client.is_tls() => {
                    anyhow::bail!("client TLS is required");
                }
                Ok(StartupPacket::CancelRequest {
                    process_id,
                    secret_key,
                }) => {
                    return Ok(StartupRead::Cancel {
                        process_id,
                        secret_key,
                    });
                }
                Ok(StartupPacket::Startup { .. }) => return Ok(StartupRead::Packet(packet)),
                Err(error) => return Err(error).context("parse startup packet"),
            }
        }

        if buffer.len() >= max_client_buffer_bytes {
            return Ok(StartupRead::BufferLimitExceeded);
        }

        match timeout(idle_timeout, client.read_buf(buffer)).await {
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

pub(super) fn next_startup_packet(buffer: &mut BytesMut) -> anyhow::Result<Option<BytesMut>> {
    if buffer.len() < 4 {
        return Ok(None);
    }

    let len = i32::from_be_bytes(
        buffer[..4]
            .try_into()
            .expect("four startup length bytes are present"),
    );
    if len < 8 {
        return Err(WireError::InvalidStartupLength(len)).context("parse startup packet");
    }

    let len = len as usize;
    if buffer.len() < len {
        return Ok(None);
    }

    Ok(Some(buffer.split_to(len)))
}

pub(super) async fn reject_startup_encryption_request(
    client: &mut ClientConnection,
) -> anyhow::Result<()> {
    client
        .write_all(b"N")
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
