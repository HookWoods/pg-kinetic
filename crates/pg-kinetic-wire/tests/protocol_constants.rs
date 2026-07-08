use pg_kinetic_wire::{
    protocol::{BackendTag, FrontendTag, ProtocolVersion, ReadyStatusByte},
    sqlstate::SqlState,
};

#[test]
fn protocol_constants_match_postgres_wire_values() {
    assert_eq!(ProtocolVersion::V3.to_i32(), 196_608);
    assert_eq!(u8::from(FrontendTag::Query), b'Q');
    assert_eq!(u8::from(FrontendTag::Parse), b'P');
    assert_eq!(u8::from(FrontendTag::Sync), b'S');
    assert_eq!(u8::from(BackendTag::ErrorResponse), b'E');
    assert_eq!(u8::from(BackendTag::ReadyForQuery), b'Z');
    assert_eq!(u8::from(ReadyStatusByte::Idle), b'I');
}

#[test]
fn sqlstate_constants_are_stable() {
    assert_eq!(SqlState::TooManyConnections.as_str(), "53300");
    assert_eq!(SqlState::QueryCanceled.as_str(), "57014");
    assert_eq!(SqlState::CannotConnectNow.as_str(), "57P03");
    assert_eq!(SqlState::InvalidSqlStatementName.as_str(), "26000");
    assert_eq!(SqlState::FeatureNotSupported.as_str(), "0A000");
}
