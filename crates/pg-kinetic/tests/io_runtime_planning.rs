use bytes::BufMut;
use pg_kinetic::proxy_runtime::io_runtime::FrontendCycleShape;
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

#[test]
fn frontend_cycle_shape_counts_batched_simple_query_ready_frames() {
    let frames = vec![
        frontend_frame(FrontendTag::Query, b"select 1\0"),
        frontend_frame(FrontendTag::Query, b"select 2\0"),
        frontend_frame(FrontendTag::Query, b"select 3\0"),
    ];

    let shape = FrontendCycleShape::from_frames(&frames);

    assert_eq!(shape.expected_ready_count(), 3);
    assert!(!shape.needs_sync());
}

#[test]
fn frontend_cycle_shape_requires_sync_for_extended_protocol_frames() {
    let frames = vec![
        frontend_frame(FrontendTag::Parse, b""),
        frontend_frame(FrontendTag::Bind, b""),
        frontend_frame(FrontendTag::Execute, b""),
        frontend_frame(FrontendTag::Sync, b""),
    ];

    let shape = FrontendCycleShape::from_frames(&frames);

    assert_eq!(shape.expected_ready_count(), 1);
    assert!(shape.needs_sync());
}

fn frontend_frame(tag: FrontendTag, payload: &[u8]) -> FrontendFrame {
    FrontendFrame {
        tag: u8::from(tag),
        payload: bytes::Bytes::copy_from_slice(payload),
    }
}
