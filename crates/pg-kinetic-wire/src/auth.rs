use bytes::{BufMut, BytesMut};

use crate::protocol::BackendTag;

const AUTHENTICATION_OK_CODE: i32 = 0;
const AUTHENTICATION_CLEAR_TEXT_PASSWORD_CODE: i32 = 3;
const AUTHENTICATION_MD5_PASSWORD_CODE: i32 = 5;
const AUTHENTICATION_SASL_CODE: i32 = 10;
const AUTHENTICATION_SASL_CONTINUE_CODE: i32 = 11;
const AUTHENTICATION_SASL_FINAL_CODE: i32 = 12;

const SCRAM_SHA_256_MECHANISM: &str = "SCRAM-SHA-256";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthRequest<'a> {
    Ok,
    CleartextPassword,
    Md5Password([u8; 4]),
    Sasl(&'a [&'a str]),
    SaslContinue(&'a [u8]),
    SaslFinal(&'a [u8]),
}

#[must_use]
pub fn build_auth_message(request: AuthRequest<'_>) -> BytesMut {
    let mut payload = BytesMut::new();

    match request {
        AuthRequest::Ok => payload.put_i32(AUTHENTICATION_OK_CODE),
        AuthRequest::CleartextPassword => payload.put_i32(AUTHENTICATION_CLEAR_TEXT_PASSWORD_CODE),
        AuthRequest::Md5Password(salt) => {
            payload.put_i32(AUTHENTICATION_MD5_PASSWORD_CODE);
            payload.extend_from_slice(&salt);
        }
        AuthRequest::Sasl(mechanisms) => {
            payload.put_i32(AUTHENTICATION_SASL_CODE);
            for mechanism in mechanisms {
                payload.extend_from_slice(mechanism.as_bytes());
                payload.put_u8(0);
            }
            payload.put_u8(0);
        }
        AuthRequest::SaslContinue(data) => {
            payload.put_i32(AUTHENTICATION_SASL_CONTINUE_CODE);
            payload.extend_from_slice(data);
        }
        AuthRequest::SaslFinal(data) => {
            payload.put_i32(AUTHENTICATION_SASL_FINAL_CODE);
            payload.extend_from_slice(data);
        }
    }

    let mut message = BytesMut::with_capacity(payload.len() + 5);
    message.put_u8(u8::from(BackendTag::Authentication));
    message.put_i32((payload.len() + 4) as i32);
    message.extend_from_slice(&payload);
    message
}

#[must_use]
pub fn authentication_ok() -> BytesMut {
    build_auth_message(AuthRequest::Ok)
}

#[must_use]
pub fn authentication_cleartext_password() -> BytesMut {
    build_auth_message(AuthRequest::CleartextPassword)
}

#[must_use]
pub fn authentication_md5_password(salt: [u8; 4]) -> BytesMut {
    build_auth_message(AuthRequest::Md5Password(salt))
}

#[must_use]
pub fn authentication_sasl_scram_sha_256() -> BytesMut {
    build_auth_message(AuthRequest::Sasl(&[SCRAM_SHA_256_MECHANISM]))
}

#[must_use]
pub fn authentication_sasl_continue(data: &[u8]) -> BytesMut {
    build_auth_message(AuthRequest::SaslContinue(data))
}

#[must_use]
pub fn authentication_sasl_final(data: &[u8]) -> BytesMut {
    build_auth_message(AuthRequest::SaslFinal(data))
}
