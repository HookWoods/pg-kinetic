use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn proxy_startup(
    client: &mut ClientConnection,
    backend: &mut PooledBackend,
    startup_packet: &[u8],
    max_client_buffer_bytes: usize,
    max_backend_buffer_bytes: usize,
    forward_backend_auth_requests_to_client: bool,
    emit_auth_ok_when_backend_requires_no_startup: bool,
    backend_credentials: Option<&auth::BackendCredentials>,
    buffers: &mut SessionBufferSet,
    _phase_recorder: &dyn telemetry::PhaseTimingRecorder,
    client_key: (i32, i32),
) -> anyhow::Result<()> {
    if !backend.requires_startup() {
        let startup_response = synthetic_startup_ready(
            emit_auth_ok_when_backend_requires_no_startup,
            backend.backend_mut().parameter_status(),
            client_key,
        );
        client
            .write_all(&startup_response)
            .await
            .context("write synthetic startup response")?;
        return Ok(());
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(startup_packet)
        .await
        .context("forward startup")?;

    buffers.client_read_mut().clear();
    buffers.backend_read_mut().clear();
    let mut backend_auth = backend_credentials
        .cloned()
        .map(auth::BackendAuthSession::new)
        .transpose()?;
    let mut sent_backend_key_data = false;
    loop {
        if buffers.backend_read_mut().len() >= max_backend_buffer_bytes {
            return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
        }

        backend
            .backend_mut()
            .stream_mut()
            .read_buf(buffers.backend_read_mut())
            .await
            .context("read startup response")?;
        buffers.observe_backend_read();
        if buffers.backend_read_mut().len() > max_backend_buffer_bytes {
            return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
        }

        while let Some(frame) = parse_backend_frame(buffers.backend_read_mut())? {
            if frame.tag == u8::from(BackendTag::Authentication) {
                let code = auth_request_code(&frame.payload)?;
                if let Some(backend_auth) = backend_auth.as_mut() {
                    if let Some(response) =
                        backend_auth.respond(&frame.payload, backend.backend_mut().is_tls())?
                    {
                        backend
                            .backend_mut()
                            .stream_mut()
                            .write_all(&response)
                            .await
                            .context("respond to backend authentication request")?;
                    }
                    continue;
                }
                if code == 0 {
                    if forward_backend_auth_requests_to_client {
                        client
                            .write_all(&encode_backend_frame(&frame))
                            .await
                            .context("forward startup response")?;
                    }
                    continue;
                }

                if forward_backend_auth_requests_to_client {
                    client
                        .write_all(&encode_backend_frame(&frame))
                        .await
                        .context("forward startup response")?;

                    if auth_request_expects_client_response(&frame.payload)? {
                        if buffers.client_read_mut().len() >= max_client_buffer_bytes {
                            return Err(buffer_limit_exceeded(BufferBudgetKind::Client));
                        }

                        buffers.client_read_mut().clear();
                        let read = client
                            .read_buf(buffers.client_read_mut())
                            .await
                            .context("read startup auth response")?;
                        anyhow::ensure!(read > 0, "client disconnected during startup auth");
                        buffers.observe_client_read();
                        if buffers.client_read_mut().len() > max_client_buffer_bytes {
                            return Err(buffer_limit_exceeded(BufferBudgetKind::Client));
                        }
                        backend
                            .backend_mut()
                            .stream_mut()
                            .write_all(buffers.client_read_mut())
                            .await
                            .context("forward startup auth response")?;
                        buffers.client_read_mut().clear();
                    }
                } else {
                    anyhow::bail!(
                        "backend authentication exchange is not supported after local auth"
                    );
                }
            } else {
                capture_backend_parameter_status(backend, &frame);
                if capture_backend_key_data(backend, &frame) {
                    client
                        .write_all(&encode_backend_key_data(client_key.0, client_key.1))
                        .await
                        .context("write synthetic backend key data")?;
                    sent_backend_key_data = true;
                    continue;
                }
                if frame.ready_status().is_some() && !sent_backend_key_data {
                    client
                        .write_all(&encode_backend_key_data(client_key.0, client_key.1))
                        .await
                        .context("write synthetic backend key data")?;
                    sent_backend_key_data = true;
                }
                client
                    .write_all(&encode_backend_frame(&frame))
                    .await
                    .context("forward startup response")?;
            }

            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return Ok(());
            }
        }
    }
}

pub(super) async fn bootstrap_backend(
    backend: &mut PooledBackend,
    startup_packet: &[u8],
    backend_credentials: Option<&auth::BackendCredentials>,
) -> anyhow::Result<()> {
    if !backend.requires_startup() {
        return Ok(());
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(startup_packet)
        .await
        .context("forward backend startup")?;

    let mut backend_buffer = BytesMut::with_capacity(8192);
    let mut backend_auth = backend_credentials
        .cloned()
        .map(auth::BackendAuthSession::new)
        .transpose()?;
    loop {
        backend
            .backend_mut()
            .stream_mut()
            .read_buf(&mut backend_buffer)
            .await
            .context("read backend startup response")?;

        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            if frame.tag == u8::from(BackendTag::Authentication) {
                let code = auth_request_code(&frame.payload)?;
                if let Some(backend_auth) = backend_auth.as_mut() {
                    if let Some(response) =
                        backend_auth.respond(&frame.payload, backend.backend_mut().is_tls())?
                    {
                        backend
                            .backend_mut()
                            .stream_mut()
                            .write_all(&response)
                            .await
                            .context("respond to backend bootstrap authentication request")?;
                    }
                } else if code != 0 && auth_request_expects_client_response(&frame.payload)? {
                    anyhow::bail!("backend authentication exchange requires client response");
                }
            } else {
                capture_backend_parameter_status(backend, &frame);
                capture_backend_key_data(backend, &frame);
            }

            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return Ok(());
            }
        }
    }
}

pub(super) fn auth_request_code(payload: &[u8]) -> anyhow::Result<i32> {
    anyhow::ensure!(payload.len() >= 4, "authentication request missing code");
    Ok(i32::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

pub(super) fn encode_backend_frame(frame: &BackendFrame) -> BytesMut {
    let mut encoded = BytesMut::with_capacity(frame.payload.len() + 5);
    encoded.put_u8(frame.tag);
    encoded.put_i32((frame.payload.len() + 4) as i32);
    encoded.extend_from_slice(&frame.payload);
    encoded
}

pub(super) fn synthetic_startup_ready(
    include_authentication_ok: bool,
    parameter_status: &[(String, String)],
    client_key: (i32, i32),
) -> BytesMut {
    let mut bytes = BytesMut::new();
    if include_authentication_ok {
        bytes.put_u8(u8::from(BackendTag::Authentication));
        bytes.put_i32(8);
        bytes.put_i32(0);
    }
    for (name, value) in parameter_status {
        bytes.extend_from_slice(&encode_parameter_status(name, value));
    }
    bytes.extend_from_slice(&encode_backend_key_data(client_key.0, client_key.1));
    let ready = ready_for_query_idle();
    bytes.extend_from_slice(&ready);
    bytes
}

pub(super) fn capture_backend_parameter_status(backend: &mut PooledBackend, frame: &BackendFrame) {
    if frame.tag != u8::from(BackendTag::ParameterStatus) {
        return;
    }

    if let Some((name, value)) = parse_parameter_status(&frame.payload) {
        backend.backend_mut().push_parameter_status(name, value);
    }
}

pub(super) fn capture_backend_key_data(backend: &mut PooledBackend, frame: &BackendFrame) -> bool {
    if frame.tag != u8::from(BackendTag::BackendKeyData) {
        return false;
    }

    if frame.payload.len() == 8 {
        let process_id =
            i32::from_be_bytes(frame.payload[0..4].try_into().expect("process id bytes"));
        let secret_key =
            i32::from_be_bytes(frame.payload[4..8].try_into().expect("secret key bytes"));
        backend.backend_mut().set_key_data(process_id, secret_key);
    }

    true
}

pub(super) fn ready_for_query_idle() -> BytesMut {
    ready_for_query(ReadyStatus::Idle)
}

pub(super) fn ready_for_query(status: ReadyStatus) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(match status {
        ReadyStatus::Idle => u8::from(ReadyStatusByte::Idle),
        ReadyStatus::InTransaction => u8::from(ReadyStatusByte::InTransaction),
        ReadyStatus::FailedTransaction => u8::from(ReadyStatusByte::FailedTransaction),
    });
    bytes
}

pub(super) fn auth_request_expects_client_response(payload: &[u8]) -> anyhow::Result<bool> {
    let code = auth_request_code(payload)?;
    Ok(matches!(code, 3 | 5 | 6 | 7 | 8 | 9 | 10 | 11))
}
