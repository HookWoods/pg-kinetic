use bytes::{BufMut, BytesMut};
use pg_kinetic_wire::tls::{is_ssl_request, ssl_request_packet, SslResponse};

#[test]
fn detects_postgres_ssl_request() {
    let packet = ssl_request_packet();
    assert!(is_ssl_request(&packet));
}

#[test]
fn response_bytes_match_postgres_protocol() {
    assert_eq!(u8::from(SslResponse::Accept), b'S');
    assert_eq!(u8::from(SslResponse::Deny), b'N');
}

#[test]
fn startup_packet_is_not_ssl_request() {
    let mut packet = BytesMut::new();
    packet.put_i32(8);
    packet.put_i32(196_608);
    assert!(!is_ssl_request(&packet));
}
