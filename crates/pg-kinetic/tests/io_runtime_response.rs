use bytes::{BufMut, BytesMut};
use pg_kinetic::wire::backend::{parse_backend_frame, ReadyStatus};

#[test]
fn backend_ready_status_is_detected_from_buffered_frames() {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');

    let frame = parse_backend_frame(&mut bytes)
        .expect("parse")
        .expect("frame");

    assert_eq!(frame.ready_status(), Some(ReadyStatus::Idle));
    assert!(bytes.is_empty());
}
