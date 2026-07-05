use bytes::{Buf, Bytes};

use crate::{
    error::WireError,
    protocol::{CANCEL_REQUEST_CODE, GSSENC_REQUEST_CODE, SSL_REQUEST_CODE},
};
const MIN_STARTUP_LEN: i32 = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupPacket {
    Startup {
        protocol_major: i16,
        protocol_minor: i16,
        parameters: Vec<(String, String)>,
    },
    SslRequest,
    GssEncRequest,
    CancelRequest {
        process_id: i32,
        secret_key: i32,
    },
}

pub fn parse_startup_packet(packet: &[u8]) -> Result<StartupPacket, WireError> {
    if packet.len() < 4 {
        return Err(WireError::IncompleteStartupPacket);
    }

    let mut cursor = Bytes::copy_from_slice(packet);
    let len = cursor.get_i32();
    if len < MIN_STARTUP_LEN {
        return Err(WireError::InvalidStartupLength(len));
    }

    if packet.len() != len as usize {
        return Err(WireError::IncompleteStartupPacket);
    }

    let code = cursor.get_i32();
    match code {
        SSL_REQUEST_CODE => Ok(StartupPacket::SslRequest),
        GSSENC_REQUEST_CODE => Ok(StartupPacket::GssEncRequest),
        CANCEL_REQUEST_CODE => {
            if cursor.remaining() != 8 {
                return Err(WireError::InvalidStartupLength(len));
            }

            Ok(StartupPacket::CancelRequest {
                process_id: cursor.get_i32(),
                secret_key: cursor.get_i32(),
            })
        }
        protocol => {
            let protocol_major = ((protocol >> 16) & 0xffff) as i16;
            let protocol_minor = (protocol & 0xffff) as i16;
            let parameters = parse_parameters(cursor.as_ref())?;

            Ok(StartupPacket::Startup {
                protocol_major,
                protocol_minor,
                parameters,
            })
        }
    }
}

fn parse_parameters(bytes: &[u8]) -> Result<Vec<(String, String)>, WireError> {
    if bytes.is_empty() || bytes[bytes.len() - 1] != 0 {
        return Err(WireError::UnterminatedStartupParameters);
    }

    let mut parameters = Vec::new();
    let mut start = 0;

    while start < bytes.len() {
        if bytes[start] == 0 {
            break;
        }

        let key_end = find_null(bytes, start).ok_or(WireError::UnterminatedStartupParameters)?;
        let key =
            std::str::from_utf8(&bytes[start..key_end]).map_err(|_| WireError::InvalidUtf8)?;
        let value_start = key_end + 1;
        if value_start >= bytes.len() {
            return Err(WireError::MissingStartupParameterValue(key.to_owned()));
        }

        let value_end =
            find_null(bytes, value_start).ok_or(WireError::UnterminatedStartupParameters)?;
        let value = std::str::from_utf8(&bytes[value_start..value_end])
            .map_err(|_| WireError::InvalidUtf8)?;

        parameters.push((key.to_owned(), value.to_owned()));
        start = value_end + 1;
    }

    Ok(parameters)
}

fn find_null(bytes: &[u8], start: usize) -> Option<usize> {
    bytes[start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| start + offset)
}
