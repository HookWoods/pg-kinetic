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

    #[must_use]
    pub fn sqlstate(&self) -> Option<&str> {
        if self.tag != b'E' {
            return None;
        }

        let mut offset = 0;
        while offset < self.payload.len() {
            let field_type = self.payload[offset];
            offset += 1;

            if field_type == 0 {
                return None;
            }

            let remaining = self.payload.get(offset..)?;
            let terminator = remaining.iter().position(|byte| *byte == 0)?;
            let value = std::str::from_utf8(&remaining[..terminator]).ok()?;

            if field_type == b'C' {
                return Some(value);
            }

            offset += terminator + 1;
        }

        None
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
