use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::WireError,
    protocol::{BackendTag, ReadyStatusByte},
    sqlstate::SqlState,
};

const SQLSTATE_FIELD_KIND: u8 = b'C';
const SEVERITY_FIELD_KIND: u8 = b'S';
const MESSAGE_FIELD_KIND: u8 = b'M';

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

#[must_use]
pub fn build_error_response(sqlstate: &str, message: &str) -> Bytes {
    let mut payload = BytesMut::new();
    payload.put_u8(SEVERITY_FIELD_KIND);
    payload.extend_from_slice(b"ERROR\0");
    payload.put_u8(SQLSTATE_FIELD_KIND);
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.put_u8(0);
    payload.put_u8(MESSAGE_FIELD_KIND);
    payload.extend_from_slice(message.as_bytes());
    payload.put_u8(0);
    payload.put_u8(0);

    let mut frame = BytesMut::with_capacity(payload.len() + 5);
    frame.put_u8(u8::from(BackendTag::ErrorResponse));
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(&payload);
    frame.freeze()
}

impl BackendFrame {
    #[must_use]
    pub fn ready_status(&self) -> Option<ReadyStatus> {
        if self.tag != u8::from(BackendTag::ReadyForQuery) || self.payload.len() != 1 {
            return None;
        }

        match self.payload[0] {
            byte if byte == u8::from(ReadyStatusByte::Idle) => Some(ReadyStatus::Idle),
            byte if byte == u8::from(ReadyStatusByte::InTransaction) => {
                Some(ReadyStatus::InTransaction)
            }
            byte if byte == u8::from(ReadyStatusByte::FailedTransaction) => {
                Some(ReadyStatus::FailedTransaction)
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn sqlstate(&self) -> Option<SqlState> {
        if self.tag != u8::from(BackendTag::ErrorResponse) {
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

            if field_type == SQLSTATE_FIELD_KIND {
                return SqlState::parse(value);
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
