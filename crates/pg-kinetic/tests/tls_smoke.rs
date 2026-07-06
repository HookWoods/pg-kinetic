use std::{path::PathBuf, sync::Arc};

use pg_kinetic::config::{BackendTlsMode, ClientTlsMode, TlsConfig};
use pg_kinetic_proxy::tls::{load_backend_client_config, load_server_config};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

fn base_tls_config() -> TlsConfig {
    TlsConfig {
        client_tls_mode: ClientTlsMode::Allow,
        client_cert_path: Some(fixture_path("server-chain.pem")),
        client_key_path: Some(fixture_path("server-key.pem")),
        client_ca_path: Some(fixture_path("ca.pem")),
        backend_tls_mode: BackendTlsMode::Prefer,
        backend_ca_path: Some(fixture_path("ca.pem")),
        backend_server_name: Some(String::from("pg-kinetic.test")),
    }
}

#[test]
fn server_config_loads_certificate_chain_and_key() {
    let config = base_tls_config();

    let tls_config = load_server_config(&config).expect("server TLS config loads");

    assert_eq!(Arc::strong_count(&tls_config), 1);
}

#[test]
fn backend_client_config_loads_ca_roots_from_pem() {
    let config = base_tls_config();

    let client_config = load_backend_client_config(&config).expect("backend TLS config loads");

    assert_eq!(Arc::strong_count(&client_config), 1);
}

#[test]
fn invalid_cert_and_key_paths_are_reported_clearly() {
    let mut missing_cert = base_tls_config();
    missing_cert.client_cert_path = Some(fixture_path("missing-cert.pem"));

    let cert_error = load_server_config(&missing_cert).expect_err("missing cert path fails");
    let cert_error = cert_error.to_string();
    assert!(
        cert_error.contains("missing-cert.pem"),
        "error should mention missing certificate path: {cert_error}"
    );
    assert!(
        cert_error.contains("client TLS certificate"),
        "error should mention certificate loading: {cert_error}"
    );

    let mut missing_key = base_tls_config();
    missing_key.client_key_path = Some(fixture_path("missing-key.pem"));

    let key_error = load_server_config(&missing_key).expect_err("missing key path fails");
    let key_error = key_error.to_string();
    assert!(
        key_error.contains("missing-key.pem"),
        "error should mention missing key path: {key_error}"
    );
    assert!(
        key_error.contains("client TLS private key"),
        "error should mention key loading: {key_error}"
    );
}

#[test]
fn verify_client_mode_requires_client_ca() {
    let mut config = base_tls_config();
    config.client_tls_mode = ClientTlsMode::VerifyClient;
    config.client_ca_path = None;

    let error = load_server_config(&config).expect_err("verify client requires ca");
    let error = error.to_string();
    assert!(
        error.contains("client TLS CA path is required"),
        "error should explain missing client CA: {error}"
    );
}
