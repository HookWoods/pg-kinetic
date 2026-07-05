use bytes::{Buf, Bytes, BytesMut};

use crate::wire::error::WireError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadyStatus {
    Idle,
    InTransaction,
    FailedTransaction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendFrame {
    pub tag: u8,
    pub payload: Bytes,
}

impl BackendFrame {
    #[must_use]
    pub fn ready_status(&self) -> Option<ReadyStatus> {
        if self.tag != b'Z' || self.payload.len() != 1 {
            return None;
        }

        match self.payload[0] {
            b'I' => Some(ReadyStatus::Idle),
            b'T' => Some(ReadyStatus::InTransaction),
            b'E' => Some(ReadyStatus::FailedTransaction),
            _ => None,
        }
    }
}

pub fn parse_backend_frame(buffer: &mut BytesMut) -> Result<Option<BackendFrame>, WireError> {
    const HEADER_LEN: usize = 5;

    if buffer.len() < HEADER_LEN {
        return Ok(None);
    }

    let tag = buffer[0];
    let length = i32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
    if length < 4 {
        return Err(WireError::InvalidBackendFrameLength(length));
    }

    let total_len = 1 + length as usize;
    if buffer.len() < total_len {
        return Ok(None);
    }

    buffer.advance(1);
    buffer.advance(4);
    let payload = buffer.split_to((length - 4) as usize).freeze();

    Ok(Some(BackendFrame { tag, payload }))
}
