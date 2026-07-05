use bytes::{BufMut, Bytes, BytesMut};
use pg_kinetic::wire::frame::FrontendFrame;
use pg_kinetic::wire::rewrite::{
    build_parse_frame, rewrite_bind_statement_name, rewrite_close_statement_name,
    rewrite_describe_statement_name, rewrite_parse_statement_name,
};
use pretty_assertions::assert_eq;

fn parse_payload() -> Bytes {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(b"stmt1\0");
    payload.extend_from_slice(b"select $1::int\0");
    payload.put_i16(1);
    payload.put_i32(23);
    payload.freeze()
}

#[test]
fn rewrites_parse_statement_name() {
    let frame = FrontendFrame {
        tag: b'P',
        payload: parse_payload(),
    };

    let rewritten = rewrite_parse_statement_name(&frame, "pgk_1_1").expect("rewrite succeeds");

    let expected = {
        let mut payload = BytesMut::new();
        payload.extend_from_slice(b"pgk_1_1\0");
        payload.extend_from_slice(b"select $1::int\0");
        payload.put_i16(1);
        payload.put_i32(23);
        payload.freeze()
    };

    assert_eq!(rewritten.payload, expected);
}

#[test]
fn rewrites_bind_statement_name() {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(b"portal1\0");
    payload.extend_from_slice(b"stmt1\0");
    payload.put_i16(0);
    payload.put_i16(0);
    payload.put_i16(0);

    let frame = FrontendFrame {
        tag: b'B',
        payload: payload.freeze(),
    };

    let rewritten = rewrite_bind_statement_name(&frame, "pgk_1_1").expect("rewrite succeeds");

    let expected = {
        let mut payload = BytesMut::new();
        payload.extend_from_slice(b"portal1\0");
        payload.extend_from_slice(b"pgk_1_1\0");
        payload.put_i16(0);
        payload.put_i16(0);
        payload.put_i16(0);
        payload.freeze()
    };

    assert_eq!(rewritten.payload, expected);
}

#[test]
fn rewrites_describe_statement_name() {
    let mut payload = BytesMut::new();
    payload.put_u8(b'S');
    payload.extend_from_slice(b"stmt1\0");

    let frame = FrontendFrame {
        tag: b'D',
        payload: payload.freeze(),
    };

    let rewritten = rewrite_describe_statement_name(&frame, "pgk_1_1").expect("rewrite succeeds");

    assert_eq!(rewritten.payload, Bytes::from_static(b"Spgk_1_1\0"));
}

#[test]
fn rewrites_close_statement_name() {
    let mut payload = BytesMut::new();
    payload.put_u8(b'S');
    payload.extend_from_slice(b"stmt1\0");

    let frame = FrontendFrame {
        tag: b'C',
        payload: payload.freeze(),
    };

    let rewritten = rewrite_close_statement_name(&frame, "pgk_1_1").expect("rewrite succeeds");

    assert_eq!(rewritten.payload, Bytes::from_static(b"Spgk_1_1\0"));
}

#[test]
fn builds_backend_parse_frame_for_materialization() {
    let frame = build_parse_frame("pgk_1_1", "select $1::int", &[23]);

    let expected = {
        let mut payload = BytesMut::new();
        payload.extend_from_slice(b"pgk_1_1\0");
        payload.extend_from_slice(b"select $1::int\0");
        payload.put_i16(1);
        payload.put_i32(23);
        payload.freeze()
    };

    assert_eq!(frame.tag, b'P');
    assert_eq!(frame.payload, expected);
}
