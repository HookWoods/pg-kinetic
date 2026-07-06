use pg_kinetic_core::security::{
    AuthMode, BackendTlsMode, ClientTlsMode, DrainState, HealthStatus, ReloadField,
};

#[test]
fn security_mode_labels_are_stable() {
    assert_eq!(ClientTlsMode::Disable.as_str(), "disable");
    assert_eq!(ClientTlsMode::Allow.as_str(), "allow");
    assert_eq!(ClientTlsMode::Require.as_str(), "require");
    assert_eq!(ClientTlsMode::VerifyClient.as_str(), "verify_client");
    assert_eq!(BackendTlsMode::VerifyFull.as_str(), "verify_full");
    assert_eq!(AuthMode::ScramSha256.as_str(), "scram_sha_256");
}

#[test]
fn health_and_drain_labels_are_stable() {
    assert_eq!(DrainState::Accepting.as_str(), "accepting");
    assert_eq!(DrainState::Draining.as_str(), "draining");
    assert_eq!(HealthStatus::Ready.as_str(), "ready");
    assert_eq!(ReloadField::Qos.as_str(), "qos");
}
