use bytes::BufMut;
use pg_kinetic::wire::{frame::FrontendFrame, protocol::FrontendTag};

#[test]
fn simple_query_frame_encoding_remains_query_tagged() {
    let mut payload = bytes::BytesMut::new();
    payload.extend_from_slice(b"select 1");
    payload.put_u8(0);
    let frame = FrontendFrame {
        tag: u8::from(FrontendTag::Query),
        payload: payload.freeze(),
    };

    assert_eq!(frame.tag, b'Q');
    assert_eq!(&frame.payload[..], b"select 1\0");
}
