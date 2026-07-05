use bytes::{BufMut, Bytes, BytesMut};
use pg_kinetic::wire::backend::{parse_backend_frame, BackendFrame, ReadyStatus};
use pretty_assertions::assert_eq;

#[test]
fn waits_for_complete_backend_frame() {
    let mut buffer = BytesMut::from(&b"Z\0\0\0"[..]);

    let parsed = parse_backend_frame(&mut buffer).expect("partial frame is not an error");

    assert_eq!(parsed, None);
    assert_eq!(&buffer[..], &b"Z\0\0\0"[..]);
}

#[test]
fn parses_ready_for_query_idle() {
    let mut buffer = BytesMut::new();
    buffer.put_u8(b'Z');
    buffer.put_i32(5);
    buffer.put_u8(b'I');

    let frame = parse_backend_frame(&mut buffer)
        .expect("frame parses")
        .expect("frame exists");

    assert_eq!(
        frame,
        BackendFrame {
            tag: b'Z',
            payload: Bytes::from_static(b"I"),
        }
    );
    assert_eq!(frame.ready_status(), Some(ReadyStatus::Idle));
    assert!(buffer.is_empty());
}

#[test]
fn parses_ready_for_query_in_transaction() {
    let mut buffer = BytesMut::new();
    buffer.put_u8(b'Z');
    buffer.put_i32(5);
    buffer.put_u8(b'T');

    let frame = parse_backend_frame(&mut buffer)
        .expect("frame parses")
        .expect("frame exists");

    assert_eq!(frame.ready_status(), Some(ReadyStatus::InTransaction));
}

#[test]
fn rejects_invalid_backend_frame_length() {
    let mut buffer = BytesMut::new();
    buffer.put_u8(b'Z');
    buffer.put_i32(3);

    let error = parse_backend_frame(&mut buffer).expect_err("length below four fails");
    assert!(error
        .to_string()
        .contains("backend frame length is invalid"));
}
