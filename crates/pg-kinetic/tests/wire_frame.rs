use bytes::{BufMut, Bytes, BytesMut};
use pg_kinetic::wire::frame::{parse_frontend_frame, FrontendFrame};
use pretty_assertions::assert_eq;

#[test]
fn waits_for_complete_frame() {
    let mut buffer = BytesMut::from(&b"Q\0\0\0"[..]);

    let parsed = parse_frontend_frame(&mut buffer).expect("partial frame is not an error");

    assert_eq!(parsed, None);
    assert_eq!(&buffer[..], &b"Q\0\0\0"[..]);
}

#[test]
fn parses_simple_query_frame() {
    let mut buffer = BytesMut::new();
    buffer.put_u8(b'Q');
    buffer.put_i32(13);
    buffer.extend_from_slice(b"select 1\0");

    let frame = parse_frontend_frame(&mut buffer)
        .expect("frame parses")
        .expect("frame exists");

    assert_eq!(
        frame,
        FrontendFrame {
            tag: b'Q',
            payload: Bytes::from_static(b"select 1\0"),
        }
    );
    assert!(buffer.is_empty());
}

#[test]
fn rejects_invalid_frame_length() {
    let mut buffer = BytesMut::new();
    buffer.put_u8(b'Q');
    buffer.put_i32(3);

    let error = parse_frontend_frame(&mut buffer).expect_err("length below four fails");
    assert!(error
        .to_string()
        .contains("frontend frame length is invalid"));
}
