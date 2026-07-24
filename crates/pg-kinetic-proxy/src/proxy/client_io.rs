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
        let mut frames = vec![first];
        drain_buffered_simple_queries(client_buffer, &mut frames)?;
        return Ok(Some(ClientCycle::Frames(frames)));
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

fn drain_buffered_simple_queries(
    client_buffer: &mut BytesMut,
    frames: &mut Vec<FrontendFrame>,
) -> anyhow::Result<()> {
    while next_complete_frontend_tag(client_buffer) == Some(u8::from(FrontendTag::Query)) {
        let Some(frame) = parse_frontend_frame(client_buffer)? else {
            break;
        };
        frames.push(frame);
    }

    Ok(())
}

fn next_complete_frontend_tag(buffer: &[u8]) -> Option<u8> {
    if buffer.len() < 5 {
        return None;
    }

    let len = i32::from_be_bytes(
        buffer[1..5]
            .try_into()
            .expect("frontend frame length header is present"),
    );
    if len < 4 {
        return Some(buffer[0]);
    }

    (buffer.len() >= len as usize + 1).then_some(buffer[0])
}

#[derive(Debug)]
pub(super) enum ClientCycle {
    Frames(Vec<FrontendFrame>),
    Terminate,
    IdleTimeout(IdleTimeoutKind),
    BufferLimitExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontend_frame(tag: FrontendTag, payload: &[u8]) -> BytesMut {
        let mut frame = BytesMut::with_capacity(payload.len() + 5);
        frame.put_u8(u8::from(tag));
        frame.put_i32((payload.len() + 4) as i32);
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn drains_pipelined_simple_queries_already_in_one_read_buffer() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&frontend_frame(FrontendTag::Query, b"select 1\0"));
        buffer.extend_from_slice(&frontend_frame(FrontendTag::Query, b"select 2\0"));
        buffer.extend_from_slice(&frontend_frame(FrontendTag::Query, b"select 3\0"));
        let first = parse_frontend_frame(&mut buffer)
            .expect("first query parses")
            .expect("first query is complete");
        let mut frames = vec![first];

        drain_buffered_simple_queries(&mut buffer, &mut frames).expect("drain queries");

        assert_eq!(frames.len(), 3);
        assert!(buffer.is_empty());
    }

    #[test]
    fn simple_query_drain_preserves_partial_followup_frame() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&frontend_frame(FrontendTag::Query, b"select 1\0"));
        buffer.extend_from_slice(&frontend_frame(FrontendTag::Query, b"select 2\0")[..3]);
        let first = parse_frontend_frame(&mut buffer)
            .expect("first query parses")
            .expect("first query is complete");
        let mut frames = vec![first];

        drain_buffered_simple_queries(&mut buffer, &mut frames).expect("drain queries");

        assert_eq!(frames.len(), 1);
        assert_eq!(buffer.len(), 3);
    }
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
        match crate::io_runtime::take_startup_packet_bytes(buffer, max_client_buffer_bytes)? {
            crate::io_runtime::StartupPacketRead::Packet(packet) => {
                if client_tls_required && !client.is_tls() {
                    anyhow::bail!("client TLS is required");
                }
                return Ok(StartupRead::Packet(packet));
            }
            crate::io_runtime::StartupPacketRead::Cancel {
                process_id,
                secret_key,
                ..
            } => {
                return Ok(StartupRead::Cancel {
                    process_id,
                    secret_key,
                });
            }
            crate::io_runtime::StartupPacketRead::EncryptionRequest(
                crate::io_runtime::StartupEncryptionRequest::Ssl,
            ) => {
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
            crate::io_runtime::StartupPacketRead::EncryptionRequest(
                crate::io_runtime::StartupEncryptionRequest::Gss,
            ) => {
                reject_startup_encryption_request(client).await?;
                continue;
            }
            crate::io_runtime::StartupPacketRead::BufferLimitExceeded => {
                return Ok(StartupRead::BufferLimitExceeded);
            }
            crate::io_runtime::StartupPacketRead::NeedMoreBytes => {
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
    }
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
