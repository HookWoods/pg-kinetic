use bytes::{Buf, Bytes, BytesMut};

use crate::error::WireError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendFrame {
    pub tag: u8,
    pub payload: Bytes,
}

pub fn parse_frontend_frame(buffer: &mut BytesMut) -> Result<Option<FrontendFrame>, WireError> {
    const HEADER_LEN: usize = 5;

    if buffer.len() < HEADER_LEN {
        return Ok(None);
    }

    let tag = buffer[0];
    let length = i32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
    if length < 4 {
        return Err(WireError::InvalidFrameLength(length));
    }

    let total_len = 1 + length as usize;
    if buffer.len() < total_len {
        return Ok(None);
    }

    buffer.advance(1);
    buffer.advance(4);

    let payload = buffer.split_to((length - 4) as usize).freeze();
    Ok(Some(FrontendFrame { tag, payload }))
}
