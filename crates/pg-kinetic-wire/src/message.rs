use crate::{error::WireError, frame::FrontendFrame, protocol::FrontendTag};

const STATEMENT_KIND: u8 = b'S';
const PORTAL_KIND: u8 = b'P';

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
    if frame.tag != u8::from(FrontendTag::Query) {
        return Ok(None);
    }

    read_cstr(frame.payload.as_ref(), 0).map(|(query, _)| Some(query))
}

pub fn parse_parse_message(frame: &FrontendFrame) -> Result<Option<ParseMessage>, WireError> {
    if frame.tag != u8::from(FrontendTag::Parse) {
        return Ok(None);
    }

    let payload = frame.payload.as_ref();
    let (statement_name, offset) = read_cstr(payload, 0)?;
    let (query, offset) = read_cstr(payload, offset)?;

    let parameter_count = payload
        .get(offset..offset + 2)
        .ok_or(WireError::IncompleteFrame)?;
    let parameter_count = i16::from_be_bytes([parameter_count[0], parameter_count[1]]);
    if parameter_count < 0 {
        return Err(WireError::IncompleteFrame);
    }

    let offset = offset + 2;
    let parameter_count = parameter_count as usize;
    let parameter_bytes = parameter_count
        .checked_mul(4)
        .ok_or(WireError::IncompleteFrame)?;
    let parameter_type_oids = payload
        .get(offset..offset + parameter_bytes)
        .ok_or(WireError::IncompleteFrame)?
        .chunks_exact(4)
        .map(|oid| i32::from_be_bytes([oid[0], oid[1], oid[2], oid[3]]))
        .collect();

    Ok(Some(ParseMessage {
        statement_name: statement_name.to_string(),
        query: query.to_string(),
        parameter_type_oids,
    }))
}

pub fn parse_bind_statement_name(frame: &FrontendFrame) -> Result<Option<String>, WireError> {
    if frame.tag != u8::from(FrontendTag::Bind) {
        return Ok(None);
    }

    let payload = frame.payload.as_ref();
    let (_, statement_offset) = read_cstr(payload, 0)?;
    let (statement_name, _) = read_cstr(payload, statement_offset)?;
    Ok(Some(statement_name.to_string()))
}

pub fn parse_describe_target(frame: &FrontendFrame) -> Result<Option<DescribeTarget>, WireError> {
    if frame.tag != u8::from(FrontendTag::Describe) {
        return Ok(None);
    }

    let payload = frame.payload.as_ref();
    let target_kind = payload.first().copied().ok_or(WireError::IncompleteFrame)?;
    let (target_name, _) = read_cstr(payload, 1)?;

    match target_kind {
        STATEMENT_KIND => Ok(Some(DescribeTarget::Statement(target_name.to_string()))),
        PORTAL_KIND => Ok(Some(DescribeTarget::Portal(target_name.to_string()))),
        _ => Ok(None),
    }
}

pub fn parse_close_target(frame: &FrontendFrame) -> Result<Option<CloseTarget>, WireError> {
    if frame.tag != u8::from(FrontendTag::Close) {
        return Ok(None);
    }

    let payload = frame.payload.as_ref();
    let target_kind = payload.first().copied().ok_or(WireError::IncompleteFrame)?;
    let (target_name, _) = read_cstr(payload, 1)?;

    match target_kind {
        STATEMENT_KIND => Ok(Some(CloseTarget::Statement(target_name.to_string()))),
        PORTAL_KIND => Ok(Some(CloseTarget::Portal(target_name.to_string()))),
        _ => Ok(None),
    }
}

fn read_cstr(bytes: &[u8], start: usize) -> Result<(&str, usize), WireError> {
    let remaining = bytes.get(start..).ok_or(WireError::IncompleteFrame)?;
    let terminator = remaining
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(WireError::IncompleteFrame)?;

    let value =
        std::str::from_utf8(&remaining[..terminator]).map_err(|_| WireError::InvalidUtf8)?;
    Ok((value, start + terminator + 1))
}
