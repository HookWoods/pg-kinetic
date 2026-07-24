use super::*;

pub(crate) trait BackendStartupMetadata {
    fn is_tls(&self) -> bool;

    fn addr(&self) -> std::net::SocketAddr;

    fn key_data(&self) -> Option<(i32, i32)>;

    fn parameter_status(&self) -> &[(String, String)];

    fn push_parameter_status(&mut self, name: String, value: String);

    fn set_key_data(&mut self, process_id: i32, secret_key: i32);
}

impl BackendStartupMetadata for crate::backend::Backend {
    fn is_tls(&self) -> bool {
        self.is_tls()
    }

    fn addr(&self) -> std::net::SocketAddr {
        self.addr()
    }

    fn key_data(&self) -> Option<(i32, i32)> {
        self.key_data()
    }

    fn parameter_status(&self) -> &[(String, String)] {
        self.parameter_status()
    }

    fn push_parameter_status(&mut self, name: String, value: String) {
        self.push_parameter_status(name, value);
    }

    fn set_key_data(&mut self, process_id: i32, secret_key: i32) {
        self.set_key_data(process_id, secret_key);
    }
}

struct PooledBackendStartup<'a> {
    backend: &'a mut crate::backend::Backend,
}

impl BackendStartupMetadata for PooledBackendStartup<'_> {
    fn is_tls(&self) -> bool {
        self.backend.is_tls()
    }

    fn addr(&self) -> std::net::SocketAddr {
        self.backend.addr()
    }

    fn key_data(&self) -> Option<(i32, i32)> {
        self.backend.key_data()
    }

    fn parameter_status(&self) -> &[(String, String)] {
        self.backend.parameter_status()
    }

    fn push_parameter_status(&mut self, name: String, value: String) {
        self.backend.push_parameter_status(name, value);
    }

    fn set_key_data(&mut self, process_id: i32, secret_key: i32) {
        self.backend.set_key_data(process_id, secret_key);
    }
}

impl crate::io_runtime::RuntimeByteStream for PooledBackendStartup<'_> {
    async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        crate::io_runtime::read_from(self.backend.stream_mut(), dst).await
    }

    async fn write_all_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        crate::io_runtime::write_all_to(self.backend.stream_mut(), bytes).await
    }

    async fn shutdown_stream(&mut self) -> std::io::Result<()> {
        crate::io_runtime::shutdown(self.backend.stream_mut()).await
    }
}

impl BackendStartupMetadata for PooledBackend {
    fn is_tls(&self) -> bool {
        self.backend().is_tls()
    }

    fn addr(&self) -> std::net::SocketAddr {
        self.backend().addr()
    }

    fn key_data(&self) -> Option<(i32, i32)> {
        self.backend().key_data()
    }

    fn parameter_status(&self) -> &[(String, String)] {
        self.backend().parameter_status()
    }

    fn push_parameter_status(&mut self, name: String, value: String) {
        self.backend_mut().push_parameter_status(name, value);
    }

    fn set_key_data(&mut self, process_id: i32, secret_key: i32) {
        self.backend_mut().set_key_data(process_id, secret_key);
    }
}

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
    let requires_startup = backend.requires_startup();
    let mut startup_backend = PooledBackendStartup {
        backend: backend.backend_mut(),
    };
    proxy_startup_streams(
        client,
        &mut startup_backend,
        requires_startup,
        startup_packet,
        max_client_buffer_bytes,
        max_backend_buffer_bytes,
        forward_backend_auth_requests_to_client,
        emit_auth_ok_when_backend_requires_no_startup,
        backend_credentials,
        buffers,
        Some(client_key),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn proxy_startup_streams<C, B>(
    client: &mut C,
    backend: &mut B,
    requires_startup: bool,
    startup_packet: &[u8],
    max_client_buffer_bytes: usize,
    max_backend_buffer_bytes: usize,
    forward_backend_auth_requests_to_client: bool,
    emit_auth_ok_when_backend_requires_no_startup: bool,
    backend_credentials: Option<&auth::BackendCredentials>,
    buffers: &mut SessionBufferSet,
    client_key: Option<(i32, i32)>,
) -> anyhow::Result<()>
where
    C: crate::io_runtime::RuntimeByteStream + ?Sized,
    B: crate::io_runtime::RuntimeByteStream + BackendStartupMetadata,
{
    if !requires_startup {
        let client_key =
            client_key.context("client key is required for synthetic startup ready")?;
        let startup_response = synthetic_startup_ready(
            emit_auth_ok_when_backend_requires_no_startup,
            backend.parameter_status(),
            client_key,
        );
        crate::io_runtime::write_all_to(client, &startup_response)
            .await
            .context("write synthetic startup response")?;
        return Ok(());
    }

    crate::io_runtime::write_all_to(backend, startup_packet)
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

        crate::io_runtime::read_from(backend, buffers.backend_read_mut())
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
                        backend_auth.respond(&frame.payload, backend.is_tls())?
                    {
                        crate::io_runtime::write_all_to(backend, &response)
                            .await
                            .context("respond to backend authentication request")?;
                    }
                    continue;
                }
                if code == 0 {
                    if forward_backend_auth_requests_to_client {
                        crate::io_runtime::write_all_to(client, &encode_backend_frame(&frame))
                            .await
                            .context("forward startup response")?;
                    }
                    continue;
                }

                if forward_backend_auth_requests_to_client {
                    crate::io_runtime::write_all_to(client, &encode_backend_frame(&frame))
                        .await
                        .context("forward startup response")?;

                    if auth_request_expects_client_response(&frame.payload)? {
                        if buffers.client_read_mut().len() >= max_client_buffer_bytes {
                            return Err(buffer_limit_exceeded(BufferBudgetKind::Client));
                        }

                        buffers.client_read_mut().clear();
                        let read = crate::io_runtime::read_from(client, buffers.client_read_mut())
                            .await
                            .context("read startup auth response")?;
                        anyhow::ensure!(read > 0, "client disconnected during startup auth");
                        buffers.observe_client_read();
                        if buffers.client_read_mut().len() > max_client_buffer_bytes {
                            return Err(buffer_limit_exceeded(BufferBudgetKind::Client));
                        }
                        crate::io_runtime::write_all_to(backend, buffers.client_read_mut())
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
                    if let Some(client_key) = client_key {
                        crate::io_runtime::write_all_to(
                            client,
                            &encode_backend_key_data(client_key.0, client_key.1),
                        )
                        .await
                        .context("write synthetic backend key data")?;
                        sent_backend_key_data = true;
                        continue;
                    }
                }
                if frame.ready_status().is_some() && !sent_backend_key_data {
                    if let Some(client_key) = client_key {
                        crate::io_runtime::write_all_to(
                            client,
                            &encode_backend_key_data(client_key.0, client_key.1),
                        )
                        .await
                        .context("write synthetic backend key data")?;
                        sent_backend_key_data = true;
                    }
                }
                crate::io_runtime::write_all_to(client, &encode_backend_frame(&frame))
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
    let requires_startup = backend.requires_startup();
    let mut startup_backend = PooledBackendStartup {
        backend: backend.backend_mut(),
    };
    bootstrap_backend_streams(
        &mut startup_backend,
        requires_startup,
        startup_packet,
        backend_credentials,
    )
    .await
}

pub(crate) async fn bootstrap_backend_streams<B>(
    backend: &mut B,
    requires_startup: bool,
    startup_packet: &[u8],
    backend_credentials: Option<&auth::BackendCredentials>,
) -> anyhow::Result<()>
where
    B: crate::io_runtime::RuntimeByteStream + BackendStartupMetadata,
{
    if !requires_startup {
        return Ok(());
    }

    crate::io_runtime::write_all_to(backend, startup_packet)
        .await
        .context("forward backend startup")?;

    let mut backend_buffer = BytesMut::with_capacity(8192);
    let mut backend_auth = backend_credentials
        .cloned()
        .map(auth::BackendAuthSession::new)
        .transpose()?;
    loop {
        crate::io_runtime::read_from(backend, &mut backend_buffer)
            .await
            .context("read backend startup response")?;

        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            if frame.tag == u8::from(BackendTag::Authentication) {
                let code = auth_request_code(&frame.payload)?;
                if let Some(backend_auth) = backend_auth.as_mut() {
                    if let Some(response) =
                        backend_auth.respond(&frame.payload, backend.is_tls())?
                    {
                        crate::io_runtime::write_all_to(backend, &response)
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

pub(super) fn capture_backend_parameter_status(
    backend: &mut impl BackendStartupMetadata,
    frame: &BackendFrame,
) {
    if frame.tag != u8::from(BackendTag::ParameterStatus) {
        return;
    }

    if let Some((name, value)) = parse_parameter_status(&frame.payload) {
        backend.push_parameter_status(name, value);
    }
}

pub(super) fn capture_backend_key_data(
    backend: &mut impl BackendStartupMetadata,
    frame: &BackendFrame,
) -> bool {
    if frame.tag != u8::from(BackendTag::BackendKeyData) {
        return false;
    }

    if frame.payload.len() == 8 {
        let process_id =
            i32::from_be_bytes(frame.payload[0..4].try_into().expect("process id bytes"));
        let secret_key =
            i32::from_be_bytes(frame.payload[4..8].try_into().expect("secret key bytes"));
        backend.set_key_data(process_id, secret_key);
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

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io};

    use super::*;
    use crate::io_runtime::RuntimeByteStream;

    #[tokio::test]
    async fn startup_stream_helper_forwards_backend_ready_without_pooled_backend() {
        let startup_packet = BytesMut::from(&b"startup"[..]);
        let backend_frames = encoded_auth_ok_ready();
        let mut client = MemoryStream::default();
        let mut backend = MemoryStream::with_reads([backend_frames]);
        let pool = ProxyBufferPool::new(
            BufferReusePolicy::default(),
            OversizedBufferPolicy::default(),
        );
        let mut lease = pool.acquire();
        let buffers = lease.buffers_mut();

        proxy_startup_streams(
            &mut client,
            &mut backend,
            true,
            &startup_packet,
            1024,
            1024,
            true,
            false,
            None,
            buffers,
            Some((12, 34)),
        )
        .await
        .expect("startup succeeds");

        assert_eq!(backend.written, startup_packet);
        assert!(client.written.starts_with(&encoded_auth_ok()));
        assert!(client.written.ends_with(&ready_for_query_idle()));
    }

    #[tokio::test]
    async fn startup_stream_helper_synthesizes_ready_for_reused_backend() {
        let startup_packet = BytesMut::from(&b"startup"[..]);
        let mut client = MemoryStream::default();
        let mut backend = MemoryStream::default();
        let pool = ProxyBufferPool::new(
            BufferReusePolicy::default(),
            OversizedBufferPolicy::default(),
        );
        let mut lease = pool.acquire();

        proxy_startup_streams(
            &mut client,
            &mut backend,
            false,
            &startup_packet,
            1024,
            1024,
            true,
            true,
            None,
            lease.buffers_mut(),
            Some((12, 34)),
        )
        .await
        .expect("reused backend startup succeeds");

        assert!(backend.written.is_empty());
        assert_eq!(client.written, synthetic_startup_ready(true, &[], (12, 34)));
    }

    #[derive(Default)]
    struct MemoryStream {
        reads: VecDeque<BytesMut>,
        written: BytesMut,
        parameter_status: Vec<(String, String)>,
        key_data: Option<(i32, i32)>,
    }

    impl MemoryStream {
        fn with_reads(reads: impl IntoIterator<Item = BytesMut>) -> Self {
            Self {
                reads: reads.into_iter().collect(),
                written: BytesMut::new(),
                parameter_status: Vec::new(),
                key_data: None,
            }
        }
    }

    impl BackendStartupMetadata for MemoryStream {
        fn is_tls(&self) -> bool {
            false
        }

        fn addr(&self) -> std::net::SocketAddr {
            "127.0.0.1:5432".parse().expect("test address")
        }

        fn key_data(&self) -> Option<(i32, i32)> {
            self.key_data
        }

        fn parameter_status(&self) -> &[(String, String)] {
            &self.parameter_status
        }

        fn push_parameter_status(&mut self, name: String, value: String) {
            self.parameter_status.push((name, value));
        }

        fn set_key_data(&mut self, process_id: i32, secret_key: i32) {
            self.key_data = Some((process_id, secret_key));
        }
    }

    impl RuntimeByteStream for MemoryStream {
        async fn read_into(&mut self, dst: &mut BytesMut) -> io::Result<usize> {
            let Some(mut next) = self.reads.pop_front() else {
                return Ok(0);
            };
            let len = next.len();
            dst.extend_from_slice(&next.split());
            Ok(len)
        }

        async fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.written.extend_from_slice(bytes);
            Ok(())
        }

        async fn shutdown_stream(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn encoded_auth_ok_ready() -> BytesMut {
        let mut bytes = encoded_auth_ok();
        bytes.extend_from_slice(&ready_for_query_idle());
        bytes
    }

    fn encoded_auth_ok() -> BytesMut {
        let mut bytes = BytesMut::new();
        bytes.put_u8(u8::from(BackendTag::Authentication));
        bytes.put_i32(8);
        bytes.put_i32(0);
        bytes
    }
}
