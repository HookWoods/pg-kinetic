use bytes::{Bytes, BytesMut};
use pg_kinetic_wire::backend::{parse_backend_frame, BackendFrame, ReadyStatus};
use pg_kinetic_wire::frame::parse_frontend_frame;
use pg_kinetic_wire::{
    frame::FrontendFrame,
    protocol::FrontendTag,
    startup::{parse_startup_packet, StartupPacket},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendCycleShape {
    expected_ready_count: usize,
    needs_sync: bool,
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
    Cancel { bytes: BytesMut },
    EncryptionRequest,
    BufferLimitExceeded,
    NeedMoreBytes,
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

        Self {
            expected_ready_count: query_count.max(1),
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
    pub const fn needs_sync(self) -> bool {
        self.needs_sync
    }
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
        while !buffer[..cycle_len].ends_with_sync_frame()? {
            let Some(next_len) = complete_frontend_frame_len(&buffer[cycle_len..])? else {
                return Ok(FrontendCycleRead::NeedMoreBytes);
            };
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
        StartupPacket::CancelRequest { .. } => Ok(StartupPacketRead::Cancel { bytes: packet }),
        StartupPacket::SslRequest | StartupPacket::GssEncRequest => {
            Ok(StartupPacketRead::EncryptionRequest)
        }
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

trait FrontendCycleBytes {
    fn ends_with_sync_frame(&self) -> anyhow::Result<bool>;
}

impl FrontendCycleBytes for [u8] {
    fn ends_with_sync_frame(&self) -> anyhow::Result<bool> {
        let mut scan = BytesMut::from(self);
        let mut last_is_sync = false;
        while let Some(frame) = parse_frontend_frame(&mut scan)? {
            last_is_sync = frame.tag == u8::from(FrontendTag::Sync);
        }
        Ok(last_is_sync && scan.is_empty())
    }
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

#[derive(Debug)]
pub struct BackendResponseDrain {
    injected_parse_completes: usize,
    ready_count: usize,
    expected_ready_count: usize,
    response_started: bool,
}

impl BackendResponseDrain {
    #[must_use]
    pub const fn new(expected_ready_count: usize, injected_parse_completes: usize) -> Self {
        Self {
            injected_parse_completes,
            ready_count: 0,
            expected_ready_count,
            response_started: false,
        }
    }

    #[must_use]
    pub const fn from_state(
        expected_ready_count: usize,
        ready_count: usize,
        injected_parse_completes: usize,
        response_started: bool,
    ) -> Self {
        Self {
            injected_parse_completes,
            ready_count,
            expected_ready_count,
            response_started,
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
            if self.injected_parse_completes > 0 && frame.tag == b'1' {
                self.injected_parse_completes -= 1;
                continue;
            }

            self.response_started = true;
            on_frame(&frame)?;
            if let Some(status) = frame.ready_status() {
                self.ready_count += 1;
                ready = Some(status);
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
