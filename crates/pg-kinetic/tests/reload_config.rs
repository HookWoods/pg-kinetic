use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use pg_kinetic::{
    config::{AuthMode, BackendTlsMode, ClientTlsMode, Config, ConnectionConfig},
    proxy_runtime::reload::{
        load_auth_users, load_client_tls_server_config, load_effective_config, reload_once,
        validate_runtime_assets, ReloadDecision,
    },
};
use tokio::sync::RwLock;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

fn temp_path(prefix: &str, suffix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "pg-kinetic-{prefix}-{}-{timestamp}{suffix}",
        std::process::id()
    ))
}

fn write_temp_file(prefix: &str, suffix: &str, contents: &str) -> PathBuf {
    let path = temp_path(prefix, suffix);
    fs::write(&path, contents).expect("write temp file");
    path
}

fn copy_fixture(prefix: &str, name: &str) -> PathBuf {
    let path = temp_path(prefix, &format!("-{name}"));
    fs::copy(fixture_path(name), &path).expect("copy fixture");
    path
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn base_config() -> Config {
    Config {
        connection: Default::default(),
        routes: Vec::new(),
        runtime: Default::default(),
        capacity: Default::default(),
        pool_lifecycle: Default::default(),
        performance: Default::default(),
        qos: Default::default(),
        admin: Default::default(),
        observability: Default::default(),
        tls: Default::default(),
        auth: Default::default(),
        reload: Default::default(),
        drain: Default::default(),
        health: Default::default(),
        socket: Default::default(),
    }
}

#[test]
fn backend_service_auth_requires_a_password_source() {
    let mut config = base_config();
    config.auth.auth_mode = AuthMode::Trust;
    config.auth.backend_user = Some(String::from("pool_user"));

    let error = validate_runtime_assets(&config).expect_err("incomplete credentials must fail");

    assert!(error.to_string().contains("backend_user"));
}

#[test]
fn backend_service_auth_requires_a_service_user() {
    let mut config = base_config();
    config.auth.auth_mode = AuthMode::Trust;
    config.auth.backend_password_env_var_name = Some(String::from("PG_KINETIC_POOL_PASSWORD"));

    let error = validate_runtime_assets(&config).expect_err("incomplete credentials must fail");

    assert!(error.to_string().contains("backend_password_env_var_name"));
}

#[test]
fn pass_through_auth_rejects_backend_service_credentials() {
    let mut config = base_config();
    config.auth.backend_user = Some(String::from("pool_user"));
    config.auth.backend_password_env_var_name = Some(String::from("PG_KINETIC_POOL_PASSWORD"));

    let error = validate_runtime_assets(&config)
        .expect_err("pass-through must not use service credentials");

    assert!(error.to_string().contains("pass_through"));
}

#[allow(clippy::too_many_arguments)]
fn file_config(
    listen_addr: SocketAddr,
    backend_addr: SocketAddr,
    auth_users_file: &Path,
    client_cert_path: &Path,
    client_key_path: &Path,
    client_ca_path: &Path,
    socket_nodelay: bool,
    query_timeout_ms: u64,
    reload_enabled: bool,
) -> String {
    let client_cert_path = toml_path(client_cert_path);
    let client_key_path = toml_path(client_key_path);
    let client_ca_path = toml_path(client_ca_path);
    let auth_users_file = toml_path(auth_users_file);

    format!(
        r#"
[connection]
listen_addr = "{listen_addr}"
backend_addr = "{backend_addr}"

[performance]
checkout_timeout_ms = 250
recovery_mode = "drop"
recovery_timeout_ms = 7_500
backend_reset_query = "DISCARD TEMP"

[qos]
max_route_in_flight = 7
max_route_waiters = 9
query_timeout_ms = {query_timeout_ms}
idle_client_timeout_ms = 5_678
idle_transaction_timeout_ms = 9_012
max_client_buffer_bytes = 111
max_backend_buffer_bytes = 222
overload_error_code = "53301"

[tls]
client_tls_mode = "verify_client"
client_cert_path = "{client_cert_path}"
client_key_path = "{client_key_path}"
client_ca_path = "{client_ca_path}"
backend_tls_mode = "prefer"
backend_ca_path = "{client_ca_path}"
backend_server_name = "db.example.com"

[auth]
auth_mode = "trust"
auth_users_file = "{auth_users_file}"
backend_user = "proxy_user"
backend_password_env_var_name = "PG_KINETIC_BACKEND_PASSWORD"
auth_failure_message_mode = "detailed"

[reload]
config_reload_interval_ms = 7_500
reload_enabled = {reload_enabled}

[drain]
drain_timeout_ms = 45_000
reject_new_clients_during_drain = true

[health]
health_addr = "127.0.0.1:9091"
readiness_backend_check_interval_ms = 333
readiness_timeout_ms = 4_444

[socket]
tcp_nodelay = {socket_nodelay}
tcp_keepalive = true
tcp_keepalive_idle_ms = 1_111
tcp_keepalive_interval_ms = 2_222
tcp_keepalive_retries = 3
tcp_user_timeout_ms = 3_333
tcp_send_buffer_bytes = 4_444
tcp_recv_buffer_bytes = 5_555
strict_socket_option_mode = true
"#,
    )
}

#[test]
fn loads_config_from_toml_file() {
    let mut config = base_config();
    let auth_users_file = write_temp_file("auth-users", ".toml", "alice = trust\n");
    let client_cert_path = fixture_path("server-chain.pem");
    let client_key_path = fixture_path("server-key.pem");
    let client_ca_path = fixture_path("ca.pem");
    let config_file = write_temp_file(
        "config",
        ".toml",
        &file_config(
            "0.0.0.0:6432".parse().expect("listen"),
            "127.0.0.1:5433".parse().expect("backend"),
            &auth_users_file,
            &client_cert_path,
            &client_key_path,
            &client_ca_path,
            false,
            1_234,
            true,
        ),
    );
    config.reload.config_file = Some(config_file);

    let effective = load_effective_config(&config).expect("load config file");

    assert_eq!(
        effective.connection,
        ConnectionConfig {
            listen_addr: "0.0.0.0:6432".parse().expect("listen"),
            backend_addr: "127.0.0.1:5433".parse().expect("backend"),
        }
    );
    assert_eq!(effective.performance.checkout_timeout_ms, 250);
    assert_eq!(
        effective.performance.recovery_mode,
        pg_kinetic::recovery::RecoveryMode::Drop
    );
    assert_eq!(effective.qos.max_route_in_flight, 7);
    assert_eq!(effective.qos.query_timeout_ms, 1_234);
    assert_eq!(effective.tls.client_tls_mode, ClientTlsMode::VerifyClient);
    assert_eq!(effective.tls.backend_tls_mode, BackendTlsMode::Prefer);
    assert_eq!(effective.auth.auth_mode, AuthMode::Trust);
    assert_eq!(effective.reload.config_reload_interval_ms, 7_500);
    assert!(effective.reload.reload_enabled);
    assert!(!effective.socket.tcp_nodelay);
}

#[test]
fn cli_and_env_values_override_file_values_where_applicable() {
    let mut config = base_config();
    config.connection.listen_addr = "127.0.0.1:7000".parse().expect("listen");
    config.connection.backend_addr = "127.0.0.1:7443".parse().expect("backend");
    config.auth.auth_mode = AuthMode::Trust;
    config.socket.tcp_nodelay = false;

    let auth_users_file = write_temp_file("auth-users", ".toml", "alice = trust\n");
    let client_cert_path = fixture_path("server-chain.pem");
    let client_key_path = fixture_path("server-key.pem");
    let client_ca_path = fixture_path("ca.pem");
    let config_file = write_temp_file(
        "config",
        ".toml",
        &file_config(
            "0.0.0.0:6432".parse().expect("listen"),
            "127.0.0.1:5433".parse().expect("backend"),
            &auth_users_file,
            &client_cert_path,
            &client_key_path,
            &client_ca_path,
            true,
            999,
            false,
        ),
    );
    config.reload.config_file = Some(config_file);

    let effective = load_effective_config(&config).expect("load config file");

    assert_eq!(
        effective.connection.listen_addr,
        "127.0.0.1:7000".parse().expect("listen")
    );
    assert_eq!(
        effective.connection.backend_addr,
        "127.0.0.1:7443".parse().expect("backend")
    );
    assert_eq!(effective.auth.auth_mode, AuthMode::Trust);
    assert!(!effective.socket.tcp_nodelay);
    assert_eq!(effective.qos.query_timeout_ms, 999);
}

#[tokio::test]
async fn safe_reload_applies_qos_timeouts_socket_tls_and_users() {
    let auth_users_v1 = write_temp_file("auth-users-v1", ".toml", "alice = trust\n");
    let auth_users_v2 = write_temp_file("auth-users-v2", ".toml", "alice = trust\nbob = trust\n");
    let server_chain_v1 = copy_fixture("server-chain-v1", "server-chain.pem");
    let server_key_v1 = copy_fixture("server-key-v1", "server-key.pem");
    let server_chain_v2 = copy_fixture("server-chain-v2", "server-chain.pem");
    let server_key_v2 = copy_fixture("server-key-v2", "server-key.pem");
    let client_ca = fixture_path("ca.pem");

    let mut config = base_config();
    let config_file = write_temp_file(
        "config",
        ".toml",
        &file_config(
            "127.0.0.1:6543".parse().expect("listen"),
            "127.0.0.1:5432".parse().expect("backend"),
            &auth_users_v1,
            &server_chain_v1,
            &server_key_v1,
            &client_ca,
            true,
            2_222,
            true,
        ),
    );
    config.reload.config_file = Some(config_file.clone());

    let effective = load_effective_config(&config).expect("initial load");
    let active_config = Arc::new(RwLock::new(effective.clone()));

    assert_eq!(effective.qos.query_timeout_ms, 2_222);
    assert!(load_auth_users(&effective)
        .expect("users")
        .as_ref()
        .expect("users")
        .get("bob")
        .is_none());
    assert!(load_client_tls_server_config(&effective)
        .expect("tls config")
        .is_some());

    let updated_config = write_temp_file(
        "config",
        ".toml",
        &file_config(
            "127.0.0.1:6543".parse().expect("listen"),
            "127.0.0.1:5432".parse().expect("backend"),
            &auth_users_v2,
            &server_chain_v2,
            &server_key_v2,
            &client_ca,
            false,
            3_333,
            true,
        ),
    );
    fs::write(
        &config_file,
        fs::read_to_string(&updated_config).expect("read updated config"),
    )
    .expect("overwrite config file");
    fs::write(&auth_users_v2, "alice = trust\nbob = trust\n").expect("update auth users");

    let decision = reload_once(&config, &active_config).await.expect("reload");

    assert_eq!(decision, ReloadDecision::Applied);
    let updated_config = active_config.read().await.clone();
    assert_eq!(updated_config.qos.query_timeout_ms, 3_333);
    assert!(!updated_config.socket.tcp_nodelay);
    assert!(load_auth_users(&updated_config)
        .expect("users")
        .as_ref()
        .expect("users")
        .get("bob")
        .is_some());
    assert_ne!(
        effective.tls.client_cert_path,
        updated_config.tls.client_cert_path
    );
    assert!(load_client_tls_server_config(&updated_config)
        .expect("tls config")
        .is_some());
}

#[tokio::test]
async fn unsafe_reload_rejects_listener_backend_and_auth_mode_changes() {
    let auth_users_file = write_temp_file("auth-users", ".toml", "alice = trust\n");
    let client_cert_path = fixture_path("server-chain.pem");
    let client_key_path = fixture_path("server-key.pem");
    let client_ca_path = fixture_path("ca.pem");

    let mut config = base_config();
    let config_file = write_temp_file(
        "config",
        ".toml",
        &file_config(
            "127.0.0.1:6543".parse().expect("listen"),
            "127.0.0.1:5432".parse().expect("backend"),
            &auth_users_file,
            &client_cert_path,
            &client_key_path,
            &client_ca_path,
            true,
            2_222,
            true,
        ),
    );
    config.reload.config_file = Some(config_file.clone());

    let effective = load_effective_config(&config).expect("initial load");
    let active_config = Arc::new(RwLock::new(effective.clone()));

    fs::write(
        &config_file,
        r#"
[connection]
listen_addr = "0.0.0.0:6544"
backend_addr = "127.0.0.1:5432"

[auth]
auth_mode = "scram_sha_256"
auth_users_file = "does-not-matter.toml"
"#,
    )
    .expect("write unsafe config");

    let decision = reload_once(&config, &active_config).await.expect("reload");

    assert_eq!(decision, ReloadDecision::Rejected);
    assert_eq!(
        active_config.read().await.connection.listen_addr,
        "127.0.0.1:6543".parse().expect("listen")
    );
    assert_eq!(active_config.read().await.qos.query_timeout_ms, 2_222);
}

#[tokio::test]
async fn invalid_reload_keeps_previous_config_active() {
    let auth_users_file = write_temp_file("auth-users", ".toml", "alice = trust\n");
    let client_cert_path = fixture_path("server-chain.pem");
    let client_key_path = fixture_path("server-key.pem");
    let client_ca_path = fixture_path("ca.pem");

    let mut config = base_config();
    let config_file = write_temp_file(
        "config",
        ".toml",
        &file_config(
            "127.0.0.1:6543".parse().expect("listen"),
            "127.0.0.1:5432".parse().expect("backend"),
            &auth_users_file,
            &client_cert_path,
            &client_key_path,
            &client_ca_path,
            true,
            2_222,
            true,
        ),
    );
    config.reload.config_file = Some(config_file.clone());

    let effective = load_effective_config(&config).expect("initial load");
    let active_config = Arc::new(RwLock::new(effective.clone()));

    fs::write(&config_file, "[qos\n").expect("write invalid config");

    let error = reload_once(&config, &active_config)
        .await
        .expect_err("invalid reload");
    assert!(error.to_string().contains("parse config file"));

    assert_eq!(active_config.read().await.qos.query_timeout_ms, 2_222);
}
