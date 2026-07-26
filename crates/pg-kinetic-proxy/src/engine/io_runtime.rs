use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{future::Future, io, time::Duration};

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use pg_kinetic_wire::backend::{parse_backend_frame, BackendFrame, ReadyStatus};
use pg_kinetic_wire::frame::parse_frontend_frame;
use pg_kinetic_wire::{
    frame::FrontendFrame,
    protocol::{BackendTag, FrontendTag},
    startup::{parse_startup_packet, StartupPacket},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FrontendCycleShape {
    expected_ready_count: usize,
    expected_completion_count: usize,
    needs_sync: bool,
}

/// A frontend message that ends the extended-protocol cycle the proxy is reading.
fn is_extended_cycle_boundary(tag: u8) -> bool {
    tag == u8::from(FrontendTag::Sync) || tag == u8::from(FrontendTag::Flush)
}

/// Frontend messages that each elicit exactly one terminating backend reply.
///
/// Only used for `Flush`-delimited cycles, which have no `ReadyForQuery` to mark
/// the end of the response. See `is_backend_cycle_completion`.
fn expects_backend_completion(tag: u8) -> bool {
    tag == u8::from(FrontendTag::Parse)
        || tag == u8::from(FrontendTag::Bind)
        || tag == u8::from(FrontendTag::Describe)
        || tag == u8::from(FrontendTag::Execute)
        || tag == u8::from(FrontendTag::Close)
}

/// Backend messages that terminate the reply to a single frontend request.
///
/// `ParameterDescription` and `DataRow` are deliberately absent: they precede a
/// terminating `RowDescription`/`NoData` and `CommandComplete` respectively, so
/// counting them would overshoot the expected completion count.
fn is_backend_cycle_completion(tag: u8) -> bool {
    tag == u8::from(BackendTag::ParseComplete)
        || tag == u8::from(BackendTag::BindComplete)
        || tag == u8::from(BackendTag::CloseComplete)
        || tag == u8::from(BackendTag::RowDescription)
        || tag == u8::from(BackendTag::NoData)
        || tag == u8::from(BackendTag::CommandComplete)
        || tag == u8::from(BackendTag::EmptyQueryResponse)
        || tag == u8::from(BackendTag::PortalSuspended)
}

#[derive(Debug, Eq, PartialEq)]
pub enum FrontendCycleRead {
    Complete {
        bytes: BytesMut,
        shape: FrontendCycleShape,
    },
    Terminate {
        bytes: BytesMut,
    },
    BufferLimitExceeded,
    NeedMoreBytes,
}

#[derive(Debug, Eq, PartialEq)]
pub enum StartupPacketRead {
    Packet(BytesMut),
    Cancel {
        bytes: BytesMut,
        process_id: i32,
        secret_key: i32,
    },
    EncryptionRequest(StartupEncryptionRequest),
    BufferLimitExceeded,
    NeedMoreBytes,
}

pub(crate) trait RuntimeByteStream {
    async fn read_into(&mut self, dst: &mut BytesMut) -> io::Result<usize>;

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    async fn read_into_timeout(
        &mut self,
        dst: &mut BytesMut,
        duration: Duration,
    ) -> Result<io::Result<usize>, ()> {
        monoio_timeout(duration, self.read_into(dst)).await
    }

    async fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()>;

    async fn shutdown_stream(&mut self) -> io::Result<()>;
}

pub(crate) async fn read_from<S: RuntimeByteStream + ?Sized>(
    stream: &mut S,
    dst: &mut BytesMut,
) -> io::Result<usize> {
    stream.read_into(dst).await
}

pub(crate) async fn write_all_to<S: RuntimeByteStream + ?Sized>(
    stream: &mut S,
    bytes: &[u8],
) -> io::Result<()> {
    stream.write_all_bytes(bytes).await
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub(crate) async fn read_from_timeout<S: RuntimeByteStream + ?Sized>(
    stream: &mut S,
    dst: &mut BytesMut,
    duration: Duration,
) -> Result<io::Result<usize>, ()> {
    stream.read_into_timeout(dst, duration).await
}

pub(crate) async fn shutdown<S: RuntimeByteStream + ?Sized>(stream: &mut S) -> io::Result<()> {
    stream.shutdown_stream().await
}

pub(crate) trait TimeoutRuntime {
    async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, ()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TokioTimeout;

impl TimeoutRuntime for TokioTimeout {
    async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, ()> {
        tokio::time::timeout(duration, future).await.map_err(|_| ())
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MonoioTimeout;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
impl TimeoutRuntime for MonoioTimeout {
    async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, ()> {
        monoio::time::timeout(duration, future)
            .await
            .map_err(|_| ())
    }
}

pub(crate) async fn tokio_timeout<F: Future>(
    duration: Duration,
    future: F,
) -> Result<F::Output, ()> {
    TokioTimeout::timeout(duration, future).await
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub(crate) async fn monoio_timeout<F: Future>(
    duration: Duration,
    future: F,
) -> Result<F::Output, ()> {
    MonoioTimeout::timeout(duration, future).await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupEncryptionRequest {
    Ssl,
    Gss,
}

#[derive(Debug)]
pub struct CapacityGuard {
    active: Arc<AtomicUsize>,
}

pub type ClientCapacityGuard = CapacityGuard;
pub type BackendCapacityGuard = CapacityGuard;

impl Drop for CapacityGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn try_enter_client_capacity(
    active: &Arc<AtomicUsize>,
    max_clients: usize,
) -> Option<ClientCapacityGuard> {
    try_enter_capacity(active, max_clients)
}

pub fn try_enter_backend_capacity(
    active: &Arc<AtomicUsize>,
    max_backends: usize,
) -> Option<BackendCapacityGuard> {
    try_enter_capacity(active, max_backends)
}

fn try_enter_capacity(active: &Arc<AtomicUsize>, limit: usize) -> Option<CapacityGuard> {
    let mut current = active.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return None;
        }
        match active.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                return Some(CapacityGuard {
                    active: Arc::clone(active),
                });
            }
            Err(actual) => current = actual,
        }
    }
}

impl FrontendCycleShape {
    #[must_use]
    pub fn from_frames(frames: &[FrontendFrame]) -> Self {
        let query_count = frames
            .iter()
            .filter(|frame| frame.tag == u8::from(FrontendTag::Query))
            .count();
        let needs_sync = frames
            .iter()
            .any(|frame| frame.tag != u8::from(FrontendTag::Query));
        let ends_with_sync = frames
            .last()
            .is_some_and(|frame| frame.tag == u8::from(FrontendTag::Sync));
        // Every simple query answers with its own ReadyForQuery, and a trailing
        // Sync adds one more for the extended-protocol part of the cycle. Summing
        // both keeps mixed cycles correct instead of collapsing them to one.
        let expected_ready_count = query_count + usize::from(ends_with_sync);
        // Without a Sync there is no ReadyForQuery to wait for, so the end of the
        // response is defined by one terminating reply per pending request.
        let expected_completion_count = if expected_ready_count == 0 {
            frames
                .iter()
                .filter(|frame| expects_backend_completion(frame.tag))
                .count()
        } else {
            0
        };

        Self {
            expected_ready_count,
            expected_completion_count,
            needs_sync,
        }
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> anyhow::Result<Option<Self>> {
        let mut buffer = BytesMut::from(bytes);
        let mut frames = Vec::new();
        while let Some(frame) = parse_frontend_frame(&mut buffer)? {
            frames.push(frame);
        }

        if frames.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Self::from_frames(&frames)))
        }
    }

    #[must_use]
    pub const fn expected_ready_count(self) -> usize {
        self.expected_ready_count
    }

    #[must_use]
    pub const fn expects_ready(self) -> bool {
        self.expected_ready_count > 0
    }

    /// Terminating backend replies expected when the cycle has no `Sync`.
    #[must_use]
    pub const fn expected_completion_count(self) -> usize {
        self.expected_completion_count
    }

    /// A cycle the backend never answers, such as a lone `Flush` or `Sync`-less
    /// batch of no-reply frames. Waiting on the backend for one would hang.
    #[must_use]
    pub const fn expects_no_response(self) -> bool {
        self.expected_ready_count == 0 && self.expected_completion_count == 0
    }

    #[must_use]
    pub const fn needs_sync(self) -> bool {
        self.needs_sync
    }
}

pub fn parse_frontend_cycle_frames(mut bytes: BytesMut) -> anyhow::Result<Vec<FrontendFrame>> {
    let mut frames = Vec::new();
    while let Some(frame) = parse_frontend_frame(&mut bytes)? {
        frames.push(frame);
    }
    Ok(frames)
}

pub fn take_frontend_cycle_bytes(
    buffer: &mut BytesMut,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<FrontendCycleRead> {
    if buffer.len() > max_client_buffer_bytes {
        return Ok(FrontendCycleRead::BufferLimitExceeded);
    }

    let Some(first_len) = complete_frontend_frame_len(buffer)? else {
        return Ok(FrontendCycleRead::NeedMoreBytes);
    };

    let first_tag = buffer[0];
    let mut cycle_len = first_len;
    if first_tag == u8::from(FrontendTag::Terminate) {
        return Ok(FrontendCycleRead::Terminate {
            bytes: buffer.split_to(cycle_len),
        });
    } else if first_tag == u8::from(FrontendTag::Query) {
        while let Some(next_len) = complete_frontend_frame_len(&buffer[cycle_len..])? {
            if buffer[cycle_len] != u8::from(FrontendTag::Query) {
                break;
            }
            cycle_len += next_len;
        }
    } else {
        // Walk forward tracking the tag at each frame offset. Re-scanning the
        // accumulated prefix on every iteration would copy and re-parse it once
        // per frame, which is quadratic for pipelined batches.
        let mut last_tag = first_tag;
        while !is_extended_cycle_boundary(last_tag) {
            let Some(next_len) = complete_frontend_frame_len(&buffer[cycle_len..])? else {
                return Ok(FrontendCycleRead::NeedMoreBytes);
            };
            last_tag = buffer[cycle_len];
            cycle_len += next_len;
        }
    }

    let bytes = buffer.split_to(cycle_len);
    let shape = FrontendCycleShape::from_wire_bytes(&bytes)?.expect("complete cycle has frames");
    Ok(FrontendCycleRead::Complete { bytes, shape })
}

pub fn take_startup_packet_bytes(
    buffer: &mut BytesMut,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<StartupPacketRead> {
    if buffer.len() > max_client_buffer_bytes {
        return Ok(StartupPacketRead::BufferLimitExceeded);
    }
    if buffer.len() < 4 {
        return Ok(StartupPacketRead::NeedMoreBytes);
    }

    let length = i32::from_be_bytes(
        buffer[..4]
            .try_into()
            .expect("four startup length bytes are present"),
    );
    if length < 8 {
        let packet = BytesMut::from(&buffer[..]);
        parse_startup_packet(&packet).map_err(anyhow::Error::from)?;
        unreachable!("invalid startup length should be rejected by parser");
    }

    let length = length as usize;
    if buffer.len() < length {
        return Ok(StartupPacketRead::NeedMoreBytes);
    }

    let packet = buffer.split_to(length);
    match parse_startup_packet(&packet).map_err(anyhow::Error::from)? {
        StartupPacket::Startup { .. } => Ok(StartupPacketRead::Packet(packet)),
        StartupPacket::CancelRequest {
            process_id,
            secret_key,
        } => Ok(StartupPacketRead::Cancel {
            bytes: packet,
            process_id,
            secret_key,
        }),
        StartupPacket::SslRequest => Ok(StartupPacketRead::EncryptionRequest(
            StartupEncryptionRequest::Ssl,
        )),
        StartupPacket::GssEncRequest => Ok(StartupPacketRead::EncryptionRequest(
            StartupEncryptionRequest::Gss,
        )),
    }
}

fn complete_frontend_frame_len(bytes: &[u8]) -> anyhow::Result<Option<usize>> {
    const HEADER_LEN: usize = 5;
    if bytes.len() < HEADER_LEN {
        return Ok(None);
    }

    let length = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    if length < 4 {
        let mut invalid = BytesMut::from(bytes);
        parse_frontend_frame(&mut invalid)?;
        unreachable!("invalid frontend length should be rejected by parser");
    }

    let total_len = 1 + length as usize;
    Ok((bytes.len() >= total_len).then_some(total_len))
}

#[derive(Debug, Default)]
pub struct PlannedFrontendCycle {
    pub backend_bytes: BytesMut,
    pub injected_parse_completes: usize,
    pub needs_sync: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseDrainEvent {
    Frames {
        ready: Option<ReadyStatus>,
        response_started: bool,
    },
    BufferLimitExceeded,
    NeedMoreBytes,
}

#[derive(Debug, Eq, PartialEq)]
pub enum BackendBytesDrainEvent {
    Bytes {
        bytes: BytesMut,
        ready: Option<ReadyStatus>,
    },
    BufferLimitExceeded,
    NeedMoreBytes,
}

/// Counters carried across the reads that make up one backend response cycle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendCycleProgress {
    pub ready_count: usize,
    pub injected_parse_completes: usize,
    pub completion_count: usize,
    pub saw_error: bool,
    pub response_started: bool,
}

#[derive(Debug)]
pub struct BackendResponseDrain {
    injected_parse_completes: usize,
    ready_count: usize,
    expected_ready_count: usize,
    completion_count: usize,
    expected_completion_count: usize,
    saw_error: bool,
    response_started: bool,
}

impl BackendResponseDrain {
    #[must_use]
    pub const fn new(expected_ready_count: usize, injected_parse_completes: usize) -> Self {
        Self {
            injected_parse_completes,
            ready_count: 0,
            expected_ready_count,
            completion_count: 0,
            expected_completion_count: 0,
            saw_error: false,
            response_started: false,
        }
    }

    /// Drain for one frontend cycle, carrying the cycle's completion budget so
    /// `Flush`-delimited responses know when they are finished.
    #[must_use]
    pub const fn for_cycle(shape: FrontendCycleShape, injected_parse_completes: usize) -> Self {
        Self {
            injected_parse_completes,
            ready_count: 0,
            expected_ready_count: shape.expected_ready_count,
            completion_count: 0,
            expected_completion_count: shape.expected_completion_count,
            saw_error: false,
            response_started: false,
        }
    }

    #[must_use]
    pub const fn resume(shape: FrontendCycleShape, progress: BackendCycleProgress) -> Self {
        Self {
            injected_parse_completes: progress.injected_parse_completes,
            ready_count: progress.ready_count,
            expected_ready_count: shape.expected_ready_count,
            completion_count: progress.completion_count,
            expected_completion_count: shape.expected_completion_count,
            saw_error: progress.saw_error,
            response_started: progress.response_started,
        }
    }

    #[must_use]
    pub const fn progress(&self) -> BackendCycleProgress {
        BackendCycleProgress {
            ready_count: self.ready_count,
            injected_parse_completes: self.injected_parse_completes,
            completion_count: self.completion_count,
            saw_error: self.saw_error,
            response_started: self.response_started,
        }
    }

    #[must_use]
    pub const fn response_started(&self) -> bool {
        self.response_started
    }

    #[must_use]
    pub const fn ready_count(&self) -> usize {
        self.ready_count
    }

    #[must_use]
    pub const fn injected_parse_completes(&self) -> usize {
        self.injected_parse_completes
    }

    #[must_use]
    pub const fn expects_ready(&self) -> bool {
        self.expected_ready_count > 0
    }

    /// True once a `Sync`-less cycle has been fully answered.
    ///
    /// An `ErrorResponse` ends it early: the backend discards every remaining
    /// message in the cycle until it sees a `Sync`, so the outstanding
    /// completions will never arrive.
    #[must_use]
    pub const fn flush_cycle_complete(&self) -> bool {
        self.saw_error || self.completion_count >= self.expected_completion_count
    }

    pub fn drain(
        &mut self,
        backend_buffer: &mut BytesMut,
        forwarded_frames: &mut Vec<([u8; 5], Bytes)>,
    ) -> anyhow::Result<ResponseDrainEvent> {
        self.drain_with(backend_buffer, forwarded_frames, |_| Ok(()))
    }

    pub fn drain_with_limit(
        &mut self,
        backend_buffer: &mut BytesMut,
        forwarded_frames: &mut Vec<([u8; 5], Bytes)>,
        max_backend_buffer_bytes: usize,
    ) -> anyhow::Result<ResponseDrainEvent> {
        if backend_buffer.len() > max_backend_buffer_bytes {
            return Ok(ResponseDrainEvent::BufferLimitExceeded);
        }

        self.drain(backend_buffer, forwarded_frames)
    }

    pub fn drain_with(
        &mut self,
        backend_buffer: &mut BytesMut,
        forwarded_frames: &mut Vec<([u8; 5], Bytes)>,
        mut on_frame: impl FnMut(&BackendFrame) -> anyhow::Result<()>,
    ) -> anyhow::Result<ResponseDrainEvent> {
        let mut ready = None;
        while let Some(frame) = parse_backend_frame(backend_buffer)? {
            if self.injected_parse_completes > 0 && frame.tag == u8::from(BackendTag::ParseComplete)
            {
                // Reply to a Parse the proxy injected, not one the client sent:
                // swallow it and leave the completion count untouched.
                self.injected_parse_completes -= 1;
                continue;
            }

            self.response_started = true;
            on_frame(&frame)?;
            if let Some(status) = frame.ready_status() {
                self.ready_count += 1;
                ready = Some(status);
            }
            if is_backend_cycle_completion(frame.tag) {
                self.completion_count += 1;
            }
            if frame.tag == u8::from(BackendTag::ErrorResponse) {
                self.saw_error = true;
            }

            let mut header = [0_u8; 5];
            header[0] = frame.tag;
            header[1..].copy_from_slice(&((frame.payload.len() + 4) as i32).to_be_bytes());
            forwarded_frames.push((header, frame.payload));
        }

        if ready.is_some() && self.ready_count >= self.expected_ready_count {
            Ok(ResponseDrainEvent::Frames {
                ready,
                response_started: self.response_started,
            })
        } else if forwarded_frames.is_empty() {
            Ok(ResponseDrainEvent::NeedMoreBytes)
        } else {
            Ok(ResponseDrainEvent::Frames {
                ready: None,
                response_started: self.response_started,
            })
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum BackendForwardOutcome {
    Ready(ReadyStatus),
    Flushed,
}

pub fn drain_backend_response_bytes(
    backend_buffer: &mut BytesMut,
    drain: &mut BackendResponseDrain,
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<BackendBytesDrainEvent> {
    let mut forwarded_frames = Vec::new();
    match drain.drain_with_limit(
        backend_buffer,
        &mut forwarded_frames,
        max_backend_buffer_bytes,
    )? {
        ResponseDrainEvent::Frames { ready, .. } => {
            let total_len = forwarded_frames
                .iter()
                .map(|(header, payload)| header.len() + payload.len())
                .sum();
            let mut bytes = BytesMut::with_capacity(total_len);
            for (header, payload) in forwarded_frames {
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(&payload);
            }
            Ok(BackendBytesDrainEvent::Bytes { bytes, ready })
        }
        ResponseDrainEvent::BufferLimitExceeded => Ok(BackendBytesDrainEvent::BufferLimitExceeded),
        ResponseDrainEvent::NeedMoreBytes => Ok(BackendBytesDrainEvent::NeedMoreBytes),
    }
}

#[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
pub(crate) async fn forward_backend_until_ready<B, C>(
    backend: &mut B,
    client: &mut C,
    backend_buffer: &mut BytesMut,
    drain: &mut BackendResponseDrain,
    max_backend_buffer_bytes: usize,
    read_context: &'static str,
    write_context: &'static str,
    closed_message: &'static str,
) -> anyhow::Result<ReadyStatus>
where
    B: RuntimeByteStream + ?Sized,
    C: RuntimeByteStream + ?Sized,
{
    match forward_backend_until_cycle_complete(
        backend,
        client,
        backend_buffer,
        drain,
        max_backend_buffer_bytes,
        read_context,
        write_context,
        closed_message,
    )
    .await?
    {
        BackendForwardOutcome::Ready(status) => Ok(status),
        BackendForwardOutcome::Flushed => {
            anyhow::bail!("backend flushed response without ReadyForQuery")
        }
    }
}

pub(crate) async fn forward_backend_until_cycle_complete<B, C>(
    backend: &mut B,
    client: &mut C,
    backend_buffer: &mut BytesMut,
    drain: &mut BackendResponseDrain,
    max_backend_buffer_bytes: usize,
    read_context: &'static str,
    write_context: &'static str,
    closed_message: &'static str,
) -> anyhow::Result<BackendForwardOutcome>
where
    B: RuntimeByteStream + ?Sized,
    C: RuntimeByteStream + ?Sized,
{
    if !drain.expects_ready() && drain.flush_cycle_complete() {
        // Nothing to wait for: the cycle elicits no backend reply at all.
        return Ok(BackendForwardOutcome::Flushed);
    }

    loop {
        let read = read_from(backend, backend_buffer)
            .await
            .with_context(|| read_context)?;
        if read == 0 {
            anyhow::bail!(closed_message);
        }

        match drain_backend_response_bytes(backend_buffer, drain, max_backend_buffer_bytes)? {
            BackendBytesDrainEvent::Bytes { bytes, ready } => {
                if !bytes.is_empty() {
                    write_all_to(client, &bytes)
                        .await
                        .with_context(|| write_context)?;
                }
                if let Some(status) = ready {
                    return Ok(BackendForwardOutcome::Ready(status));
                }
                // Only stop once every pending request has been answered.
                // Returning on the first non-empty read would strand the rest of
                // a split response and deadlock a client waiting on it.
                if !drain.expects_ready() && drain.flush_cycle_complete() {
                    return Ok(BackendForwardOutcome::Flushed);
                }
            }
            BackendBytesDrainEvent::BufferLimitExceeded => {
                anyhow::bail!("backend response exceeded configured buffer limit");
            }
            BackendBytesDrainEvent::NeedMoreBytes => {}
        }
    }
}
