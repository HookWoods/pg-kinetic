use bytes::Bytes;
use pg_kinetic_wire::auth::{
    authentication_cleartext_password, authentication_md5_password, authentication_ok,
    authentication_sasl_continue, authentication_sasl_final, authentication_sasl_scram_sha_256,
};

#[test]
fn builds_authentication_ok() {
    assert_eq!(
        authentication_ok().freeze(),
        Bytes::from_static(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]),
    );
}

#[test]
fn builds_authentication_cleartext_password() {
    assert_eq!(
        authentication_cleartext_password().freeze(),
        Bytes::from_static(&[b'R', 0, 0, 0, 8, 0, 0, 0, 3]),
    );
}

#[test]
fn builds_authentication_md5_password() {
    assert_eq!(
        authentication_md5_password([1, 2, 3, 4]).freeze(),
        Bytes::from_static(&[b'R', 0, 0, 0, 12, 0, 0, 0, 5, 1, 2, 3, 4]),
    );
}

#[test]
fn builds_authentication_sasl_scram_sha_256() {
    assert_eq!(
        authentication_sasl_scram_sha_256().freeze(),
        Bytes::from_static(&[
            b'R', 0, 0, 0, 23, 0, 0, 0, 10, b'S', b'C', b'R', b'A', b'M', b'-', b'S', b'H',
            b'A', b'-', b'2', b'5', b'6', 0, 0,
        ]),
    );
}

#[test]
fn builds_authentication_sasl_continue() {
    assert_eq!(
        authentication_sasl_continue(&[0x01, 0x02, 0x03]).freeze(),
        Bytes::from_static(&[b'R', 0, 0, 0, 11, 0, 0, 0, 11, 1, 2, 3]),
    );
}

#[test]
fn builds_authentication_sasl_final() {
    assert_eq!(
        authentication_sasl_final(b"v=server-proof").freeze(),
        Bytes::from_static(&[
            b'R', 0, 0, 0, 22, 0, 0, 0, 12, b'v', b'=', b's', b'e', b'r', b'v', b'e', b'r',
            b'-', b'p', b'r', b'o', b'o', b'f',
        ]),
    );
}
