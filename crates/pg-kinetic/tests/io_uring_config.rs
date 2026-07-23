use pg_kinetic::{
    config::{AuthMode, BackendTlsMode, ClientTlsMode, Config, RouteConfig},
    core::runtime::RuntimeEngine,
    proxy_runtime::io_uring,
};

fn io_uring_config() -> Config {
    let mut config = Config::default();
    config.runtime.engine.runtime_engine = RuntimeEngine::ExperimentalIoUring;
    config.runtime.engine.experimental_runtime_enabled = true;
    config
}

#[test]
fn io_uring_rejects_client_tls_until_semantic_runtime_exists() {
    let mut config = io_uring_config();
    config.tls.client_tls_mode = ClientTlsMode::Require;

    let error = io_uring::validate_supported_config_for_test(&config)
        .expect_err("client TLS is not supported yet");

    assert!(error.to_string().contains("client_tls_mode=disable"));
}

#[test]
fn io_uring_rejects_backend_tls_until_semantic_runtime_exists() {
    let mut config = io_uring_config();
    config.tls.backend_tls_mode = BackendTlsMode::Require;

    let error = io_uring::validate_supported_config_for_test(&config)
        .expect_err("backend TLS is not supported yet");

    assert!(error.to_string().contains("backend_tls_mode=disable"));
}

#[test]
fn io_uring_rejects_auth_modes_until_auth_path_exists() {
    let mut config = io_uring_config();
    config.auth.auth_mode = AuthMode::Trust;

    let error = io_uring::validate_supported_config_for_test(&config)
        .expect_err("auth modes are not supported yet");

    assert!(error.to_string().contains("auth_mode=pass_through"));
}

#[test]
fn io_uring_rejects_routes_until_pooling_path_exists() {
    let mut config = io_uring_config();
    config.routes = vec![RouteConfig::default()];

    let error = io_uring::validate_supported_config_for_test(&config)
        .expect_err("routes are not supported yet");

    assert!(error.to_string().contains("routes to be omitted"));
}
