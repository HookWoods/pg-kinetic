#[cfg(test)]
extern crate std;

pub const BACKEND_TAG_ERROR_RESPONSE: u8 = b'E';
pub const BACKEND_TAG_READY_FOR_QUERY: u8 = b'Z';
pub const READY_STATUS_IDLE: u8 = b'I';
pub const READY_STATUS_IN_TRANSACTION: u8 = b'T';
pub const READY_STATUS_FAILED_TRANSACTION: u8 = b'E';
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;
pub const SCANNER_PROTOCOL_UNCERTAIN: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanCarry {
    pub header: [u8; 5],
    pub header_len: u8,
    pub payload_remaining: u32,
    pub pending_tag: u8,
    pub uncertain: u8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameScan {
    pub consumed: usize,
    pub saw_error: bool,
    pub ready_status: Option<u8>,
    pub protocol_uncertain: bool,
}

pub fn scan_backend_frames(buf: &[u8], carry: &mut ScanCarry) -> FrameScan {
    if carry.uncertain == SCANNER_PROTOCOL_UNCERTAIN {
        return FrameScan {
            consumed: buf.len(),
            saw_error: false,
            ready_status: None,
            protocol_uncertain: true,
        };
    }

    let mut offset = 0;
    let mut saw_error = false;
    let mut ready_status = None;
    let mut protocol_uncertain = false;

    while offset < buf.len() {
        if carry.payload_remaining > 0 {
            let available = buf.len() - offset;
            let consumed = core::cmp::min(available, carry.payload_remaining as usize);
            if carry.pending_tag == BACKEND_TAG_READY_FOR_QUERY {
                let status = buf[offset];
                match status {
                    READY_STATUS_IDLE
                    | READY_STATUS_IN_TRANSACTION
                    | READY_STATUS_FAILED_TRANSACTION
                        if consumed == 1 && carry.payload_remaining == 1 =>
                    {
                        ready_status = Some(status);
                    }
                    _ => {
                        carry.uncertain = SCANNER_PROTOCOL_UNCERTAIN;
                        protocol_uncertain = true;
                    }
                }
            }
            offset += consumed;
            carry.payload_remaining -= consumed as u32;
            if carry.payload_remaining == 0 {
                carry.pending_tag = 0;
            }
            continue;
        }

        while carry.header_len < 5 && offset < buf.len() {
            carry.header[carry.header_len as usize] = buf[offset];
            carry.header_len += 1;
            offset += 1;
        }

        if carry.header_len < 5 {
            break;
        }

        let tag = carry.header[0];
        let length = u32::from_be_bytes([
            carry.header[1],
            carry.header[2],
            carry.header[3],
            carry.header[4],
        ]);

        if !(4..=MAX_FRAME_LEN).contains(&length) {
            carry.uncertain = SCANNER_PROTOCOL_UNCERTAIN;
            protocol_uncertain = true;
            break;
        }

        let payload_len = length - 4;
        let available = buf.len() - offset;
        let consumed = core::cmp::min(available, payload_len as usize);
        let payload = &buf[offset..offset + consumed];

        if tag == BACKEND_TAG_ERROR_RESPONSE {
            saw_error = true;
        }

        if tag == BACKEND_TAG_READY_FOR_QUERY && payload_len == 1 && consumed == 1 {
            match payload[0] {
                READY_STATUS_IDLE
                | READY_STATUS_IN_TRANSACTION
                | READY_STATUS_FAILED_TRANSACTION => {
                    ready_status = Some(payload[0]);
                }
                _ => {
                    carry.uncertain = SCANNER_PROTOCOL_UNCERTAIN;
                    protocol_uncertain = true;
                }
            }
        }

        offset += consumed;
        carry.header = [0; 5];
        carry.header_len = 0;

        if consumed < payload_len as usize {
            carry.pending_tag = tag;
            carry.payload_remaining = payload_len - consumed as u32;
        }
    }

    FrameScan {
        consumed: offset,
        saw_error,
        ready_status,
        protocol_uncertain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::vec::Vec;

    fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(payload.len() + 5);
        bytes.extend_from_slice(&[tag]);
        bytes.extend_from_slice(&((payload.len() as u32 + 4).to_be_bytes()));
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn scanner_detects_ready_for_query_across_one_byte_chunks() {
        let stream = [
            frame(b'D', b"row1").as_slice(),
            frame(b'C', b"SELECT 1\0").as_slice(),
            frame(BACKEND_TAG_READY_FOR_QUERY, b"I").as_slice(),
        ]
        .concat();

        let mut carry = ScanCarry::default();
        let mut ready = None;
        let mut consumed = 0;

        for chunk in stream.chunks(1) {
            let scan = scan_backend_frames(chunk, &mut carry);
            consumed += scan.consumed;
            if scan.ready_status.is_some() {
                ready = scan.ready_status;
            }
        }

        assert_eq!(consumed, stream.len());
        assert_eq!(ready, Some(READY_STATUS_IDLE));
        assert_eq!(carry, ScanCarry::default());
    }

    #[test]
    fn scanner_flags_error_response_before_ready() {
        let stream = [
            frame(BACKEND_TAG_ERROR_RESPONSE, b"SERROR\0C42P01\0\0").as_slice(),
            frame(BACKEND_TAG_READY_FOR_QUERY, b"E").as_slice(),
        ]
        .concat();

        let mut carry = ScanCarry::default();
        let scan = scan_backend_frames(&stream, &mut carry);

        assert_eq!(scan.consumed, stream.len());
        assert!(scan.saw_error);
        assert_eq!(scan.ready_status, Some(READY_STATUS_FAILED_TRANSACTION));
        assert_eq!(carry, ScanCarry::default());
    }

    #[test]
    fn scanner_carries_split_payload() {
        let stream = frame(b'D', b"abcdef");
        let mut carry = ScanCarry::default();

        let first = scan_backend_frames(&stream[..7], &mut carry);
        assert_eq!(first.consumed, 7);
        assert_eq!(first.ready_status, None);
        assert_eq!(carry.pending_tag, b'D');
        assert_eq!(carry.payload_remaining, 4);

        let second = scan_backend_frames(&stream[7..], &mut carry);
        assert_eq!(second.consumed, 4);
        assert_eq!(carry, ScanCarry::default());
    }

    #[test]
    fn scanner_marks_invalid_lengths_uncertain() {
        let invalid = [b'Z', 0, 0, 0, 3];
        let mut carry = ScanCarry::default();

        let scan = scan_backend_frames(&invalid, &mut carry);

        assert_eq!(scan.consumed, invalid.len());
        assert!(scan.protocol_uncertain);
        assert_eq!(carry.uncertain, SCANNER_PROTOCOL_UNCERTAIN);
    }
}
