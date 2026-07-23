use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendFailureKind {
    Connect,
    Read,
    Write,
    Authentication,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDisposition {
    Never,
    RetryBeforeResponse,
}

#[must_use]
pub const fn retry_disposition(
    kind: BackendFailureKind,
    response_started: bool,
    request_is_safe_to_replay: bool,
) -> RetryDisposition {
    if matches!(kind, BackendFailureKind::Read) && !response_started && request_is_safe_to_replay {
        RetryDisposition::RetryBeforeResponse
    } else {
        RetryDisposition::Never
    }
}

#[derive(Debug)]
pub(super) struct BackendFailure {
    pub(super) kind: BackendFailureKind,
    pub(super) response_started: bool,
    source: anyhow::Error,
}

impl std::fmt::Display for BackendFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "backend {:?} failure: {}",
            self.kind, self.source
        )
    }
}

impl std::error::Error for BackendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

pub(super) fn backend_failure(
    kind: BackendFailureKind,
    response_started: bool,
    source: impl Into<anyhow::Error>,
) -> anyhow::Error {
    BackendFailure {
        kind,
        response_started,
        source: source.into(),
    }
    .into()
}

#[derive(Debug)]
pub(crate) struct ClientConnection {
    inner: Option<ClientTransport>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(super) enum ClientTransport {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl ClientConnection {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self {
            inner: Some(ClientTransport::Plain(stream)),
        }
    }

    pub(crate) fn is_tls(&self) -> bool {
        matches!(self.inner, Some(ClientTransport::Tls(_)))
    }

    pub(crate) fn has_peer_certificates(&self) -> bool {
        match self.inner.as_ref().expect("client stream present") {
            ClientTransport::Plain(_) => false,
            ClientTransport::Tls(stream) => stream.get_ref().1.peer_certificates().is_some(),
        }
    }

    pub(crate) async fn read_buf(&mut self, buffer: &mut BytesMut) -> std::io::Result<usize> {
        match self.inner.as_mut().expect("client stream present") {
            ClientTransport::Plain(stream) => stream.read_buf(buffer).await,
            ClientTransport::Tls(stream) => stream.read_buf(buffer).await,
        }
    }

    pub(crate) async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self.inner.as_mut().expect("client stream present") {
            ClientTransport::Plain(stream) => stream.write_all(bytes).await,
            ClientTransport::Tls(stream) => stream.write_all(bytes).await,
        }
    }

    pub(crate) async fn write_all_vectored(
        &mut self,
        slices: &[IoSlice<'_>],
    ) -> std::io::Result<()> {
        if slices.is_empty() {
            return Ok(());
        }

        let mut slice_index = 0;
        let mut slice_offset = 0;

        while slice_index < slices.len() {
            skip_empty_vectored_slices(slices, &mut slice_index, &mut slice_offset);
            if slice_index >= slices.len() {
                return Ok(());
            }

            let mut remaining_slices = Vec::with_capacity(slices.len() - slice_index);
            let first_slice = &slices[slice_index][slice_offset..];
            remaining_slices.push(IoSlice::new(first_slice));
            for slice in &slices[slice_index + 1..] {
                if !slice.is_empty() {
                    remaining_slices.push(IoSlice::new(slice));
                }
            }

            let written = match self.inner.as_mut().expect("client stream present") {
                ClientTransport::Plain(stream) => stream.write_vectored(&remaining_slices).await?,
                ClientTransport::Tls(stream) => stream.write_vectored(&remaining_slices).await?,
            };
            if written == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write vectored bytes",
                ));
            }

            let mut remaining = written;
            while remaining > 0 {
                let current_slice_len = slices[slice_index].len() - slice_offset;
                if current_slice_len == 0 {
                    slice_index += 1;
                    slice_offset = 0;
                    if slice_index >= slices.len() {
                        return Ok(());
                    }
                    continue;
                }
                if remaining < current_slice_len {
                    slice_offset += remaining;
                    remaining = 0;
                } else {
                    remaining -= current_slice_len;
                    slice_index += 1;
                    slice_offset = 0;
                    if slice_index >= slices.len() {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn shutdown(&mut self) -> std::io::Result<()> {
        match self.inner.as_mut().expect("client stream present") {
            ClientTransport::Plain(stream) => stream.shutdown().await,
            ClientTransport::Tls(stream) => stream.shutdown().await,
        }
    }

    pub(crate) async fn start_tls(
        &mut self,
        server_config: &Arc<ServerConfig>,
    ) -> anyhow::Result<()> {
        let plain = match self.inner.take().context("client stream missing")? {
            ClientTransport::Plain(stream) => stream,
            ClientTransport::Tls(stream) => {
                self.inner = Some(ClientTransport::Tls(stream));
                anyhow::bail!("client TLS is already active");
            }
        };

        let tls = tls::accept_client_tls(plain, server_config).await?;
        self.inner = Some(ClientTransport::Tls(tls));
        Ok(())
    }
}

pub(super) fn skip_empty_vectored_slices(
    slices: &[IoSlice<'_>],
    slice_index: &mut usize,
    slice_offset: &mut usize,
) {
    while *slice_index < slices.len() && *slice_offset >= slices[*slice_index].len() {
        *slice_index += 1;
        *slice_offset = 0;
    }
}
