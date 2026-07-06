use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const SSL_REQUEST_CODE: i32 = 80_877_103;
pub const SSL_REQUEST_LEN: i32 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SslResponse {
    Accept,
    Deny,
}

impl From<SslResponse> for u8 {
    fn from(response: SslResponse) -> Self {
        match response {
            SslResponse::Accept => b'S',
            SslResponse::Deny => b'N',
        }
    }
}

#[must_use]
pub fn is_ssl_request(packet: &[u8]) -> bool {
    if packet.len() != SSL_REQUEST_LEN as usize {
        return false;
    }

    let mut cursor = Bytes::copy_from_slice(packet);
    cursor.get_i32() == SSL_REQUEST_LEN && cursor.get_i32() == SSL_REQUEST_CODE
}

#[must_use]
pub fn ssl_request_packet() -> BytesMut {
    let mut packet = BytesMut::with_capacity(SSL_REQUEST_LEN as usize);
    packet.put_i32(SSL_REQUEST_LEN);
    packet.put_i32(SSL_REQUEST_CODE);
    packet
}
