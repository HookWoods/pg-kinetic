use bytes::{BufMut, BytesMut};

use crate::protocol::{BackendTag, ReadyStatusByte};

const TEXT_OID: i32 = 25;
const INT8_OID: i32 = 20;
const FLOAT8_OID: i32 = 701;
const BOOL_OID: i32 = 16;
const TIMESTAMP_OID: i32 = 1114;

const TEXT_TYPE_SIZE: i16 = -1;
const INT8_TYPE_SIZE: i16 = 8;
const FLOAT8_TYPE_SIZE: i16 = 8;
const BOOL_TYPE_SIZE: i16 = 1;
const TIMESTAMP_TYPE_SIZE: i16 = 8;

const TEXT_FORMAT_CODE: i16 = 0;
const TYPE_MODIFIER: i32 = -1;
const UNKNOWN_TABLE_OID: i32 = 0;
const UNKNOWN_ATTR_NUMBER: i16 = 0;
const SELECT_COMMAND_TAG: &str = "SHOW";
const ROW_DESCRIPTION_TAG: u8 = b'T';

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminWireType {
    Text,
    Int8,
    Float8,
    Bool,
    Timestamp,
}

impl AdminWireType {
    #[must_use]
    pub const fn oid(self) -> i32 {
        match self {
            Self::Text => TEXT_OID,
            Self::Int8 => INT8_OID,
            Self::Float8 => FLOAT8_OID,
            Self::Bool => BOOL_OID,
            Self::Timestamp => TIMESTAMP_OID,
        }
    }

    #[must_use]
    pub const fn type_size(self) -> i16 {
        match self {
            Self::Text => TEXT_TYPE_SIZE,
            Self::Int8 => INT8_TYPE_SIZE,
            Self::Float8 => FLOAT8_TYPE_SIZE,
            Self::Bool => BOOL_TYPE_SIZE,
            Self::Timestamp => TIMESTAMP_TYPE_SIZE,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminWireColumn {
    name: String,
    wire_type: AdminWireType,
}

impl AdminWireColumn {
    #[must_use]
    pub fn new(name: impl Into<String>, wire_type: AdminWireType) -> Self {
        Self {
            name: name.into(),
            wire_type,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn wire_type(&self) -> AdminWireType {
        self.wire_type
    }
}

#[must_use]
pub fn build_row_description(columns: &[AdminWireColumn]) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_i16(columns.len() as i16);

    for column in columns {
        payload.extend_from_slice(column.name.as_bytes());
        payload.put_u8(0);
        payload.put_i32(UNKNOWN_TABLE_OID);
        payload.put_i16(UNKNOWN_ATTR_NUMBER);
        payload.put_i32(column.wire_type.oid());
        payload.put_i16(column.wire_type.type_size());
        payload.put_i32(TYPE_MODIFIER);
        payload.put_i16(TEXT_FORMAT_CODE);
    }

    encode_backend_message_raw(ROW_DESCRIPTION_TAG, payload)
}

#[must_use]
pub fn build_data_row(columns: &[AdminWireColumn], rows: &[Vec<String>]) -> BytesMut {
    let mut message = BytesMut::new();

    for row in rows {
        assert_eq!(
            row.len(),
            columns.len(),
            "admin row width must match the declared columns",
        );

        let mut payload = BytesMut::new();
        payload.put_i16(columns.len() as i16);

        for value in row {
            payload.put_i32(value.len() as i32);
            payload.extend_from_slice(value.as_bytes());
        }

        message.extend_from_slice(&encode_backend_message(BackendTag::DataRow, payload));
    }

    message
}

#[must_use]
pub fn build_command_complete(command_tag: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(command_tag.as_bytes());
    payload.put_u8(0);

    encode_backend_message(BackendTag::CommandComplete, payload)
}

#[must_use]
pub fn build_admin_table_response(columns: &[AdminWireColumn], rows: &[Vec<String>]) -> BytesMut {
    let mut response = BytesMut::new();
    response.extend_from_slice(&build_row_description(columns));
    response.extend_from_slice(&build_data_row(columns, rows));
    response.extend_from_slice(&build_command_complete(SELECT_COMMAND_TAG));
    response.extend_from_slice(&build_ready_for_query());
    response
}

#[must_use]
pub fn build_command_response(command_tag: &str) -> BytesMut {
    let mut response = BytesMut::new();
    response.extend_from_slice(&build_command_complete(command_tag));
    response.extend_from_slice(&build_ready_for_query());
    response
}

fn build_ready_for_query() -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_u8(u8::from(ReadyStatusByte::Idle));

    encode_backend_message(BackendTag::ReadyForQuery, payload)
}

fn encode_backend_message(tag: BackendTag, payload: BytesMut) -> BytesMut {
    let mut frame = BytesMut::with_capacity(payload.len() + 5);
    frame.put_u8(u8::from(tag));
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(&payload);
    frame
}

fn encode_backend_message_raw(tag: u8, payload: BytesMut) -> BytesMut {
    let mut frame = BytesMut::with_capacity(payload.len() + 5);
    frame.put_u8(tag);
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(&payload);
    frame
}
