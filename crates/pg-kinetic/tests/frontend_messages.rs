use bytes::{BufMut, Bytes, BytesMut};
use pg_kinetic::wire::frame::FrontendFrame;
use pg_kinetic::wire::message::{
    parse_bind_statement_name, parse_close_target, parse_describe_target, parse_parse_message,
    parse_simple_query, CloseTarget, DescribeTarget, ParseMessage,
};
use pretty_assertions::assert_eq;

#[test]
fn parses_simple_query_text() {
    let frame = FrontendFrame {
        tag: b'Q',
        payload: Bytes::from_static(b"select 1\0"),
    };

    assert_eq!(
        parse_simple_query(&frame).expect("query parses"),
        Some("select 1")
    );
}

#[test]
fn parses_named_parse_message() {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(b"stmt1\0");
    payload.extend_from_slice(b"select $1::int\0");
    payload.put_i16(1);
    payload.put_i32(23);

    let frame = FrontendFrame {
        tag: b'P',
        payload: payload.freeze(),
    };

    assert_eq!(
        parse_parse_message(&frame).expect("parse parses"),
        Some(ParseMessage {
            statement_name: "stmt1".to_string(),
            query: "select $1::int".to_string(),
            parameter_type_oids: vec![23],
        })
    );
}

#[test]
fn parses_bind_statement_name() {
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

    assert_eq!(
        parse_bind_statement_name(&frame).expect("bind parses"),
        Some("stmt1".to_string())
    );
}

#[test]
fn parses_describe_statement_target() {
    let mut payload = BytesMut::new();
    payload.put_u8(b'S');
    payload.extend_from_slice(b"stmt1\0");

    let frame = FrontendFrame {
        tag: b'D',
        payload: payload.freeze(),
    };

    assert_eq!(
        parse_describe_target(&frame).expect("describe parses"),
        Some(DescribeTarget::Statement("stmt1".to_string()))
    );
}

#[test]
fn parses_close_statement_target() {
    let mut payload = BytesMut::new();
    payload.put_u8(b'S');
    payload.extend_from_slice(b"stmt1\0");

    let frame = FrontendFrame {
        tag: b'C',
        payload: payload.freeze(),
    };

    assert_eq!(
        parse_close_target(&frame).expect("close parses"),
        Some(CloseTarget::Statement("stmt1".to_string()))
    );
}
