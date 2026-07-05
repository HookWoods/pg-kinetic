use bytes::Bytes;
use pg_kinetic::wire::backend::BackendFrame;
use pg_kinetic::wire::sqlstate::SqlState;

#[test]
fn extracts_sqlstate_from_error_response() {
    let frame = BackendFrame {
        tag: b'E',
        payload: Bytes::from_static(b"SERROR\0C26000\0Mprepared statement missing\0\0"),
    };

    assert_eq!(frame.sqlstate(), Some(SqlState::InvalidSqlStatementName));
}
