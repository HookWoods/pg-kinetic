use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::proxy_runtime::io_runtime::{
    take_frontend_cycle_bytes, take_startup_packet_bytes, try_enter_client_capacity,
    FrontendCycleRead, FrontendCycleShape, StartupPacketRead,
};
use pg_kinetic::wire::{
    frame::FrontendFrame,
    protocol::{FrontendTag, ProtocolVersion, GSSENC_REQUEST_CODE, SSL_REQUEST_CODE},
};

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

#[test]
fn frontend_cycle_shape_from_wire_bytes_counts_complete_simple_queries() {
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&encoded_frontend_frame(FrontendTag::Query, b"select 1\0"));
    bytes.extend_from_slice(&encoded_frontend_frame(FrontendTag::Query, b"select 2\0"));
    bytes.extend_from_slice(&encoded_frontend_frame(FrontendTag::Query, b"select 3\0")[..3]);

    let shape = FrontendCycleShape::from_wire_bytes(&bytes)
        .expect("shape parses")
        .expect("complete prefix");

    assert_eq!(shape.expected_ready_count(), 2);
    assert!(!shape.needs_sync());
}

#[test]
fn frontend_cycle_bytes_take_complete_simple_prefix_and_leave_partial_tail() {
    let mut bytes = BytesMut::new();
    let first = encoded_frontend_frame(FrontendTag::Query, b"select 1\0");
    let second = encoded_frontend_frame(FrontendTag::Query, b"select 2\0");
    let partial = encoded_frontend_frame(FrontendTag::Query, b"select 3\0");
    bytes.extend_from_slice(&first);
    bytes.extend_from_slice(&second);
    bytes.extend_from_slice(&partial[..3]);

    let outcome = take_frontend_cycle_bytes(&mut bytes, 1024).expect("cycle read");

    let FrontendCycleRead::Complete {
        bytes: complete,
        shape,
    } = outcome
    else {
        panic!("expected complete cycle");
    };
    assert_eq!(complete.len(), first.len() + second.len());
    assert_eq!(shape.expected_ready_count(), 2);
    assert_eq!(bytes.len(), 3);
}

#[test]
fn frontend_cycle_bytes_wait_for_incomplete_first_frame() {
    let mut bytes = encoded_frontend_frame(FrontendTag::Query, b"select 1\0");
    bytes.truncate(4);

    let outcome = take_frontend_cycle_bytes(&mut bytes, 1024).expect("cycle read");

    assert_eq!(outcome, FrontendCycleRead::NeedMoreBytes);
    assert_eq!(bytes.len(), 4);
}

#[test]
fn frontend_cycle_bytes_reports_client_buffer_limit() {
    let mut bytes = encoded_frontend_frame(FrontendTag::Query, b"select 1\0");

    let outcome = take_frontend_cycle_bytes(&mut bytes, 4).expect("cycle read");

    assert_eq!(outcome, FrontendCycleRead::BufferLimitExceeded);
    assert!(!bytes.is_empty());
}

#[test]
fn frontend_cycle_bytes_treats_terminate_as_complete_close_cycle() {
    let mut bytes = encoded_frontend_frame(FrontendTag::Terminate, b"");

    let outcome = take_frontend_cycle_bytes(&mut bytes, 1024).expect("cycle read");

    let FrontendCycleRead::Terminate { bytes: complete } = outcome else {
        panic!("expected terminate cycle");
    };
    assert_eq!(complete[0], u8::from(FrontendTag::Terminate));
    assert!(bytes.is_empty());
}

#[test]
fn startup_packet_bytes_wait_for_split_packet() {
    let mut bytes = startup_packet();
    bytes.truncate(7);

    let outcome = take_startup_packet_bytes(&mut bytes, 1024).expect("startup read");

    assert_eq!(outcome, StartupPacketRead::NeedMoreBytes);
    assert_eq!(bytes.len(), 7);
}

#[test]
fn startup_packet_bytes_take_complete_packet() {
    let mut bytes = startup_packet();
    let len = bytes.len();

    let outcome = take_startup_packet_bytes(&mut bytes, 1024).expect("startup read");

    let StartupPacketRead::Packet(packet) = outcome else {
        panic!("expected startup packet");
    };
    assert_eq!(packet.len(), len);
    assert!(bytes.is_empty());
}

#[test]
fn startup_packet_bytes_treat_ssl_and_gss_as_encryption_requests() {
    for code in [SSL_REQUEST_CODE, GSSENC_REQUEST_CODE] {
        let mut bytes = startup_code_packet(code);

        let outcome = take_startup_packet_bytes(&mut bytes, 1024).expect("startup read");

        assert_eq!(outcome, StartupPacketRead::EncryptionRequest);
        assert!(bytes.is_empty());
    }
}

#[test]
fn startup_packet_bytes_reports_client_buffer_limit() {
    let mut bytes = startup_packet();

    let outcome = take_startup_packet_bytes(&mut bytes, 7).expect("startup read");

    assert_eq!(outcome, StartupPacketRead::BufferLimitExceeded);
    assert!(!bytes.is_empty());
}

#[test]
fn client_capacity_guard_enforces_shared_limit() {
    let active = Arc::new(AtomicUsize::new(0));
    let first = try_enter_client_capacity(&active, 1).expect("first client is accepted");

    assert!(try_enter_client_capacity(&active, 1).is_none());
    assert_eq!(active.load(Ordering::Acquire), 1);

    drop(first);

    assert_eq!(active.load(Ordering::Acquire), 0);
    assert!(try_enter_client_capacity(&active, 1).is_some());
}

fn frontend_frame(tag: FrontendTag, payload: &[u8]) -> FrontendFrame {
    FrontendFrame {
        tag: u8::from(tag),
        payload: bytes::Bytes::copy_from_slice(payload),
    }
}

fn encoded_frontend_frame(tag: FrontendTag, payload: &[u8]) -> BytesMut {
    let mut frame = BytesMut::with_capacity(payload.len() + 5);
    frame.put_u8(u8::from(tag));
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(payload);
    frame
}

fn startup_packet() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0app\0database\0app\0\0");

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet
}

fn startup_code_packet(code: i32) -> BytesMut {
    let mut packet = BytesMut::new();
    packet.put_i32(8);
    packet.put_i32(code);
    packet
}
