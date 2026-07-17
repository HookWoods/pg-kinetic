use bytes::{Buf, Bytes, BytesMut};

use crate::error::WireError;

const FRAME_HEADER_LEN: usize = 5;
const FRAME_LENGTH_PREFIX_LEN: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendFrame {
    pub tag: u8,
    pub payload: Bytes,
}

pub fn parse_frontend_frame(buffer: &mut BytesMut) -> Result<Option<FrontendFrame>, WireError> {
    if buffer.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }

    let tag = buffer[0];
    let length = i32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
    if length < FRAME_LENGTH_PREFIX_LEN as i32 {
        return Err(WireError::InvalidFrameLength(length));
    }

    let payload_len = length as usize - FRAME_LENGTH_PREFIX_LEN;
    let total_len = FRAME_HEADER_LEN + payload_len;
    if buffer.len() < total_len {
        return Ok(None);
    }

    let mut frame = buffer.split_to(total_len);
    frame.advance(FRAME_HEADER_LEN);
    let payload = frame.freeze();
    Ok(Some(FrontendFrame { tag, payload }))
}
