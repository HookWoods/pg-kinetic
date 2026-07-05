use bytes::{BufMut, Bytes, BytesMut};

use crate::{error::WireError, frame::FrontendFrame, protocol::FrontendTag};

const STATEMENT_KIND: u8 = b'S';

pub fn rewrite_parse_statement_name(
    frame: &FrontendFrame,
    backend_name: &str,
) -> Result<FrontendFrame, WireError> {
    if frame.tag != u8::from(FrontendTag::Parse) {
        return Ok(frame.clone());
    }

    let (_, after_name) = read_cstr(frame.payload.as_ref(), 0)?;
    let mut payload = BytesMut::new();
    write_cstr(&mut payload, backend_name);
    payload.extend_from_slice(&frame.payload[after_name..]);

    Ok(FrontendFrame {
        tag: frame.tag,
        payload: payload.freeze(),
    })
}

pub fn rewrite_bind_statement_name(
    frame: &FrontendFrame,
    backend_name: &str,
) -> Result<FrontendFrame, WireError> {
    if frame.tag != u8::from(FrontendTag::Bind) {
        return Ok(frame.clone());
    }

    let (_, after_portal) = read_cstr(frame.payload.as_ref(), 0)?;
    let (_, after_statement) = read_cstr(frame.payload.as_ref(), after_portal)?;

    let mut payload = BytesMut::new();
    payload.extend_from_slice(&frame.payload[..after_portal]);
    write_cstr(&mut payload, backend_name);
    payload.extend_from_slice(&frame.payload[after_statement..]);

    Ok(FrontendFrame {
        tag: frame.tag,
        payload: payload.freeze(),
    })
}

pub fn rewrite_describe_statement_name(
    frame: &FrontendFrame,
    backend_name: &str,
) -> Result<FrontendFrame, WireError> {
    rewrite_statement_target(frame, u8::from(FrontendTag::Describe), backend_name)
}

pub fn rewrite_close_statement_name(
    frame: &FrontendFrame,
    backend_name: &str,
) -> Result<FrontendFrame, WireError> {
    rewrite_statement_target(frame, u8::from(FrontendTag::Close), backend_name)
}

fn rewrite_statement_target(
    frame: &FrontendFrame,
    expected_tag: u8,
    backend_name: &str,
) -> Result<FrontendFrame, WireError> {
    if frame.tag != expected_tag {
        return Ok(frame.clone());
    }

    if frame.payload.first() != Some(&STATEMENT_KIND) {
        return Ok(frame.clone());
    }

    let mut payload = BytesMut::new();
    payload.put_u8(STATEMENT_KIND);
    write_cstr(&mut payload, backend_name);

    Ok(FrontendFrame {
        tag: frame.tag,
        payload: payload.freeze(),
    })
}

fn read_cstr(bytes: &[u8], start: usize) -> Result<(&str, usize), WireError> {
    if start > bytes.len() {
        return Err(WireError::IncompleteFrame);
    }

    let relative_end = bytes[start..]
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(WireError::IncompleteFrame)?;
    let end = start + relative_end;
    let value = std::str::from_utf8(&bytes[start..end]).map_err(|_| WireError::InvalidUtf8)?;

    Ok((value, end + 1))
}

fn write_cstr(buffer: &mut BytesMut, value: &str) {
    buffer.extend_from_slice(value.as_bytes());
    buffer.put_u8(0);
}

pub fn encode_frontend_frame(frame: &FrontendFrame) -> Bytes {
    let mut encoded = BytesMut::with_capacity(5 + frame.payload.len());
    encoded.put_u8(frame.tag);
    encoded.put_i32((frame.payload.len() + 4) as i32);
    encoded.extend_from_slice(&frame.payload);
    encoded.freeze()
}

pub fn build_parse_frame(
    backend_name: &str,
    query: &str,
    parameter_type_oids: &[i32],
) -> FrontendFrame {
    let mut payload = BytesMut::new();
    write_cstr(&mut payload, backend_name);
    write_cstr(&mut payload, query);
    payload.put_i16(parameter_type_oids.len() as i16);
    for oid in parameter_type_oids {
        payload.put_i32(*oid);
    }

    FrontendFrame {
        tag: u8::from(FrontendTag::Parse),
        payload: payload.freeze(),
    }
}
