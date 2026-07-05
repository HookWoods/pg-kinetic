use crate::wire::{error::WireError, frame::FrontendFrame};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseMessage {
    pub statement_name: String,
    pub query: String,
    pub parameter_type_oids: Vec<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeTarget {
    Statement(String),
    Portal(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CloseTarget {
    Statement(String),
    Portal(String),
}

pub fn parse_simple_query(frame: &FrontendFrame) -> Result<Option<&str>, WireError> {
    if frame.tag != b'Q' {
        return Ok(None);
    }

    read_cstr(frame.payload.as_ref(), 0).map(|(query, _)| Some(query))
}

pub fn parse_parse_message(frame: &FrontendFrame) -> Result<Option<ParseMessage>, WireError> {
    if frame.tag != b'P' {
        return Ok(None);
    }

    let (statement_name, mut offset) = read_cstr(frame.payload.as_ref(), 0)?;
    let (query, next_offset) = read_cstr(frame.payload.as_ref(), offset)?;
    offset = next_offset;

    if frame.payload.len() < offset + 2 {
        return Err(WireError::IncompleteFrame);
    }

    let parameter_count = i16::from_be_bytes([frame.payload[offset], frame.payload[offset + 1]]);
    if parameter_count < 0 {
        return Err(WireError::IncompleteFrame);
    }

    offset += 2;
    let parameter_count = parameter_count as usize;
    let parameter_bytes = parameter_count
        .checked_mul(4)
        .ok_or(WireError::IncompleteFrame)?;
    if frame.payload.len() < offset + parameter_bytes {
        return Err(WireError::IncompleteFrame);
    }

    let mut parameter_type_oids = Vec::with_capacity(parameter_count);
    for index in 0..parameter_count {
        let base = offset + (index * 4);
        parameter_type_oids.push(i32::from_be_bytes([
            frame.payload[base],
            frame.payload[base + 1],
            frame.payload[base + 2],
            frame.payload[base + 3],
        ]));
    }

    Ok(Some(ParseMessage {
        statement_name: statement_name.to_string(),
        query: query.to_string(),
        parameter_type_oids,
    }))
}

pub fn parse_bind_statement_name(frame: &FrontendFrame) -> Result<Option<String>, WireError> {
    if frame.tag != b'B' {
        return Ok(None);
    }

    let (_, statement_offset) = read_cstr(frame.payload.as_ref(), 0)?;
    let (statement_name, _) = read_cstr(frame.payload.as_ref(), statement_offset)?;
    Ok(Some(statement_name.to_string()))
}

pub fn parse_describe_target(frame: &FrontendFrame) -> Result<Option<DescribeTarget>, WireError> {
    if frame.tag != b'D' {
        return Ok(None);
    }

    let target_kind = frame
        .payload
        .first()
        .copied()
        .ok_or(WireError::IncompleteFrame)?;
    let (target_name, _) = read_cstr(frame.payload.as_ref(), 1)?;

    match target_kind {
        b'S' => Ok(Some(DescribeTarget::Statement(target_name.to_string()))),
        b'P' => Ok(Some(DescribeTarget::Portal(target_name.to_string()))),
        _ => Ok(None),
    }
}

pub fn parse_close_target(frame: &FrontendFrame) -> Result<Option<CloseTarget>, WireError> {
    if frame.tag != b'C' {
        return Ok(None);
    }

    let target_kind = frame
        .payload
        .first()
        .copied()
        .ok_or(WireError::IncompleteFrame)?;
    let (target_name, _) = read_cstr(frame.payload.as_ref(), 1)?;

    match target_kind {
        b'S' => Ok(Some(CloseTarget::Statement(target_name.to_string()))),
        b'P' => Ok(Some(CloseTarget::Portal(target_name.to_string()))),
        _ => Ok(None),
    }
}

fn read_cstr(bytes: &[u8], start: usize) -> Result<(&str, usize), WireError> {
    let terminator = bytes[start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| start + offset)
        .ok_or(WireError::IncompleteFrame)?;

    let value = std::str::from_utf8(&bytes[start..terminator]).map_err(|_| WireError::InvalidUtf8)?;
    Ok((value, terminator + 1))
}
