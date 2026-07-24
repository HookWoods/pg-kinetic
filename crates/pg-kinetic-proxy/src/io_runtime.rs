use bytes::{Bytes, BytesMut};
use pg_kinetic_wire::backend::{parse_backend_frame, BackendFrame, ReadyStatus};

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
