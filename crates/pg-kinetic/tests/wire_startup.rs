use bytes::{BufMut, BytesMut};
use pg_kinetic::wire::startup::{parse_startup_packet, StartupPacket};
use pretty_assertions::assert_eq;

fn startup_bytes() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(196_608);
    body.extend_from_slice(
        b"user\0postgres\0database\0postgres\0application_name\0pg-kinetic-test\0\0",
    );

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet
}

#[test]
fn parses_startup_packet_parameters() {
    let packet = parse_startup_packet(&startup_bytes()).expect("startup parses");

    assert_eq!(
        packet,
        StartupPacket::Startup {
            protocol_major: 3,
            protocol_minor: 0,
            parameters: vec![
                ("user".to_string(), "postgres".to_string()),
                ("database".to_string(), "postgres".to_string()),
                (
                    "application_name".to_string(),
                    "pg-kinetic-test".to_string()
                ),
            ],
        }
    );
}

#[test]
fn parses_ssl_request() {
    let mut packet = BytesMut::new();
    packet.put_i32(8);
    packet.put_i32(80_877_103);

    assert_eq!(
        parse_startup_packet(&packet).expect("ssl request parses"),
        StartupPacket::SslRequest
    );
}

#[test]
fn parses_gssenc_request() {
    let mut packet = BytesMut::new();
    packet.put_i32(8);
    packet.put_i32(80_877_104);

    assert_eq!(
        parse_startup_packet(&packet).expect("gssenc request parses"),
        StartupPacket::GssEncRequest
    );
}

#[test]
fn parses_cancel_request() {
    let mut packet = BytesMut::new();
    packet.put_i32(16);
    packet.put_i32(80_877_102);
    packet.put_i32(12);
    packet.put_i32(34);

    assert_eq!(
        parse_startup_packet(&packet).expect("cancel request parses"),
        StartupPacket::CancelRequest {
            process_id: 12,
            secret_key: 34,
        }
    );
}

#[test]
fn rejects_short_packet() {
    let error = parse_startup_packet(&[0, 0, 0, 3]).expect_err("short packet fails");
    assert!(error.to_string().contains("invalid startup packet length"));
}
