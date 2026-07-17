use bytes::{BufMut, Bytes, BytesMut};
use pg_kinetic::wire::{
    backend::parse_backend_frame,
    error::WireError,
    frame::{parse_frontend_frame, FrontendFrame},
    message::parse_simple_query,
    rewrite::encode_frontend_frame,
};
use pretty_assertions::assert_eq;

const FRAME_HEADER_LEN: usize = 5;

fn wire_frame(tag: u8, payload: &[u8]) -> BytesMut {
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.put_u8(tag);
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(payload);
    frame
}

#[test]
fn frontend_parser_reuses_the_input_allocation_for_common_headers() {
    let mut input = wire_frame(b'Q', b"select 1\0");
    let payload_start = input.as_ptr().wrapping_add(FRAME_HEADER_LEN);

    let frame = parse_frontend_frame(&mut input)
        .expect("frame parses")
        .expect("frame is complete");

    assert_eq!(frame.payload.as_ptr(), payload_start);
    assert_eq!(frame.payload, Bytes::from_static(b"select 1\0"));
    assert!(input.is_empty());
}

#[test]
fn backend_parser_uses_typed_errors_for_invalid_lengths() {
    let mut input = BytesMut::from(&b"Z\0\0\0\x03"[..]);

    let error = parse_backend_frame(&mut input).expect_err("short length is rejected");

    assert!(matches!(error, WireError::InvalidBackendFrameLength(3)));
    assert_eq!(&input[..], &b"Z\0\0\0\x03"[..]);
}

#[test]
fn simple_query_forwarding_preserves_wire_bytes() {
    let expected = wire_frame(b'Q', b"select 1\0");
    let mut input = expected.clone();

    let frame = parse_frontend_frame(&mut input)
        .expect("simple query parses")
        .expect("simple query is complete");

    assert_eq!(
        parse_simple_query(&frame).expect("query text parses"),
        Some("select 1")
    );
    assert_eq!(&encode_frontend_frame(&frame)[..], &expected[..]);
}

#[test]
fn extended_query_forwarding_preserves_each_wire_frame() {
    let frames = [
        wire_frame(b'P', b"statement\0select $1::int\0\0\x01\0\0\0\x17"),
        wire_frame(b'B', b"\0statement\0\0\0\0\0\0\0"),
        wire_frame(b'E', b"\0\0\0\0"),
        wire_frame(b'S', b""),
    ];
    let expected = frames.concat();
    let mut input = BytesMut::from(expected.as_slice());
    let mut forwarded = BytesMut::new();

    while let Some(frame) = parse_frontend_frame(&mut input).expect("extended frame parses") {
        forwarded.extend_from_slice(&encode_frontend_frame(&frame));
    }

    assert_eq!(&forwarded[..], expected.as_slice());
}

#[test]
fn reused_buffer_does_not_expose_prior_frame_bytes() {
    let mut input = BytesMut::with_capacity(64);
    input.extend_from_slice(&wire_frame(b'Q', b"select first\0"));
    let first = parse_frontend_frame(&mut input)
        .expect("first frame parses")
        .expect("first frame is complete");
    assert_eq!(first.payload, Bytes::from_static(b"select first\0"));
    assert!(input.is_empty());

    input.extend_from_slice(&wire_frame(b'Q', b"select second\0"));
    let second = parse_frontend_frame(&mut input)
        .expect("second frame parses")
        .expect("second frame is complete");

    assert_eq!(second.payload, Bytes::from_static(b"select second\0"));
    assert!(!second
        .payload
        .windows(b"first".len())
        .any(|bytes| bytes == b"first"));
}

#[test]
fn malformed_frames_return_safe_protocol_errors() {
    let mut invalid_length = BytesMut::from(&b"Q\0\0\0\x03"[..]);
    let error = parse_frontend_frame(&mut invalid_length).expect_err("short length is rejected");
    assert!(matches!(error, WireError::InvalidFrameLength(3)));

    let malformed_query = FrontendFrame {
        tag: b'Q',
        payload: Bytes::from_static(b"select 1"),
    };
    let error = parse_simple_query(&malformed_query).expect_err("unterminated query is rejected");
    assert!(matches!(error, WireError::IncompleteFrame));
}
