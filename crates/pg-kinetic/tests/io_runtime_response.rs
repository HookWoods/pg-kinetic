use bytes::{BufMut, BytesMut};
use pg_kinetic::proxy_runtime::io_runtime::{
    drain_backend_response_bytes, BackendBytesDrainEvent, BackendResponseDrain, ResponseDrainEvent,
};
use pg_kinetic::wire::{
    backend::{parse_backend_frame, ReadyStatus},
    protocol::BackendTag,
};

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

#[test]
fn response_drain_preserves_partial_frames_until_ready() {
    let mut drain = BackendResponseDrain::new(1, 0);
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    let mut forwarded = Vec::new();

    let event = drain.drain(&mut bytes, &mut forwarded).expect("drain");

    assert_eq!(event, ResponseDrainEvent::NeedMoreBytes);
    assert!(forwarded.is_empty());
    assert_eq!(bytes.len(), 5);

    bytes.put_u8(b'I');
    let event = drain.drain(&mut bytes, &mut forwarded).expect("drain");

    assert_eq!(
        event,
        ResponseDrainEvent::Frames {
            ready: Some(ReadyStatus::Idle),
            response_started: true
        }
    );
    assert_eq!(drain.ready_count(), 1);
    assert!(bytes.is_empty());
    assert_eq!(forwarded.len(), 1);
}

#[test]
fn response_drain_waits_for_all_expected_ready_frames() {
    let mut drain = BackendResponseDrain::new(2, 0);
    let mut bytes = BytesMut::new();
    for status in [b'I', b'T'] {
        bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
        bytes.put_i32(5);
        bytes.put_u8(status);
    }
    let mut forwarded = Vec::new();

    let event = drain.drain(&mut bytes, &mut forwarded).expect("drain");

    assert_eq!(
        event,
        ResponseDrainEvent::Frames {
            ready: Some(ReadyStatus::InTransaction),
            response_started: true
        }
    );
    assert_eq!(drain.ready_count(), 2);
    assert_eq!(forwarded.len(), 2);
}

#[test]
fn response_drain_hides_injected_parse_completes() {
    let mut drain = BackendResponseDrain::new(1, 1);
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'1');
    bytes.put_i32(4);
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    let mut forwarded = Vec::new();

    let event = drain.drain(&mut bytes, &mut forwarded).expect("drain");

    assert_eq!(
        event,
        ResponseDrainEvent::Frames {
            ready: Some(ReadyStatus::Idle),
            response_started: true
        }
    );
    assert_eq!(forwarded.len(), 1);
    assert_eq!(forwarded[0].0[0], u8::from(BackendTag::ReadyForQuery));
}

#[test]
fn response_drain_reports_buffer_limit_before_forwarding() {
    let mut drain = BackendResponseDrain::new(1, 0);
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    let mut forwarded = Vec::new();

    let event = drain
        .drain_with_limit(&mut bytes, &mut forwarded, 4)
        .expect("drain with limit");

    assert_eq!(event, ResponseDrainEvent::BufferLimitExceeded);
    assert!(forwarded.is_empty());
    assert!(!bytes.is_empty());
}

#[test]
fn response_byte_drain_encodes_complete_frames_and_preserves_partial_tail() {
    let mut drain = BackendResponseDrain::new(1, 0);
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::CommandComplete));
    bytes.put_i32(13);
    bytes.extend_from_slice(b"SELECT 1\0");
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.put_u8(u8::from(BackendTag::DataRow));
    bytes.put_i32(10);

    let event = drain_backend_response_bytes(&mut bytes, &mut drain, 1024).expect("drain bytes");

    let BackendBytesDrainEvent::Bytes {
        bytes: forwarded,
        ready,
    } = event
    else {
        panic!("expected forwarded bytes");
    };
    assert_eq!(ready, Some(ReadyStatus::Idle));
    assert_eq!(forwarded[0], u8::from(BackendTag::CommandComplete));
    assert_eq!(forwarded[14], u8::from(BackendTag::ReadyForQuery));
    assert_eq!(bytes.len(), 5);
}
