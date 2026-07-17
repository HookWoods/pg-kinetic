use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use pg_kinetic_proxy::preflight::{PreflightCheck, PreflightRunner};

const SCRAM_VERIFIER: &str = "SCRAM-SHA-256$4096:c2FsdHlzYWx0$RdRL9M4hIQ6KSGRy8YdcY/rWTt9c53a35goFQzcrGXw=:lNY6toUrz5jlkvLtdJbAj5bXIomZuncUbgsZq5rYF5M=";

fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_pg-kinetic")
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

fn temp_path(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    std::env::temp_dir().join(format!("pg-kinetic-preflight-{name}-{unique}.toml"))
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}

fn write_file(name: &str, contents: &str) -> PathBuf {
    let path = temp_path(name);
    fs::write(&path, contents).expect("write config");
    path
}

fn write_auth_users_file(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pg-kinetic-preflight-auth-users-{name}-{}.txt",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos()
    ));
    fs::write(&path, contents).expect("write auth users file");
    path
}

fn run_preflight(config: &Path) -> std::process::Output {
    Command::new(binary_path())
        .args([
            "preflight",
            "--config",
            config.to_str().expect("config path"),
            "--format",
            "json",
        ])
        .output()
        .expect("run preflight")
}

fn base_config(listen_addr: &str, backend_addr: &str, extra_sections: &str) -> String {
    format!(
        r#"
[connection]
listen_addr = "{listen_addr}"
backend_addr = "{backend_addr}"

{extra_sections}
"#
    )
}

#[test]
fn preflight_loads_config_without_starting_proxy_listeners() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let listen_addr = listener.local_addr().expect("listener addr");
    let config = write_file(
        "no-listener",
        &base_config(&listen_addr.to_string(), "127.0.0.1:5432", ""),
    );

    let output = run_preflight(&config);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"ok\":true"));
    drop(listener);
}

#[test]
fn preflight_validates_tls_files_when_configured() {
    let valid_config = write_file(
        "tls-valid",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            &format!(
                r#"
[tls]
client_tls_mode = "verify_client"
client_cert_path = "{}"
client_key_path = "{}"
client_ca_path = "{}"
backend_tls_mode = "verify_full"
backend_ca_path = "{}"
backend_server_name = "localhost"
"#,
                toml_path(&fixture_path("server-chain.pem")),
                toml_path(&fixture_path("server-key.pem")),
                toml_path(&fixture_path("ca.pem")),
                toml_path(&fixture_path("ca.pem"))
            ),
        ),
    );

    let valid_report = PreflightRunner::new(&valid_config).run();
    assert!(!valid_report.has_errors(), "{valid_report:?}");

    let broken_config = write_file(
        "tls-broken",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            &format!(
                r#"
[tls]
client_tls_mode = "verify_client"
client_cert_path = "{}"
client_key_path = "{}"
client_ca_path = "{}"
backend_tls_mode = "verify_full"
backend_ca_path = "{}"
backend_server_name = "localhost"
"#,
                toml_path(&temp_path("missing-client-cert")),
                toml_path(&fixture_path("server-key.pem")),
                toml_path(&fixture_path("ca.pem")),
                toml_path(&fixture_path("ca.pem"))
            ),
        ),
    );

    let broken_report = PreflightRunner::new(&broken_config).run();
    assert!(broken_report.has_errors());
    assert!(broken_report
        .errors()
        .iter()
        .any(|finding| finding.check == PreflightCheck::TlsFiles));
}

#[test]
fn preflight_validates_auth_user_store_when_configured() {
    let valid_auth_users = write_auth_users_file("valid", "alice = trust\n");
    let valid_config = write_file(
        "auth-valid",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            &format!(
                r#"
[auth]
auth_mode = "trust"
auth_users_file = "{}"
"#,
                toml_path(&valid_auth_users)
            ),
        ),
    );

    let valid_report = PreflightRunner::new(&valid_config).run();
    assert!(!valid_report.has_errors(), "{valid_report:?}");

    let invalid_auth_users = write_auth_users_file("invalid", "alice = not-a-verifier\n");
    let invalid_config = write_file(
        "auth-invalid",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            &format!(
                r#"
[auth]
auth_mode = "scram_sha_256"
auth_users_file = "{}"
"#,
                toml_path(&invalid_auth_users)
            ),
        ),
    );

    let invalid_report = PreflightRunner::new(&invalid_config).run();
    assert!(invalid_report.has_errors());
    assert!(invalid_report
        .errors()
        .iter()
        .any(|finding| finding.check == PreflightCheck::AuthUsers));
}

#[test]
fn preflight_validates_route_maps_and_policies() {
    let config = write_file(
        "routes-policies",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "schema_table"
schema = "public"
table = "orders"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "deny-legacy"
hook_point = "before_routing"
kind = "deny"
reason = "legacy tenant blocked"
"#,
        ),
    );

    let report = PreflightRunner::new(&config).run();

    assert!(!report.has_errors(), "{report:?}");
}

#[test]
fn preflight_validates_mirror_target_isolation() {
    let config = write_file(
        "mirror-isolation",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            r#"
[mirror]
mirroring_enabled = true
mirror_mode = "read_only"
mirror_timeout_ms = 100
mirror_max_in_flight = 128
mirror_sample_rate = 1.0
mirror_writes_enabled = false
mirror_require_isolated_target = true
target.address = "127.0.0.1:5432"
target.isolated = false
"#,
        ),
    );

    let report = PreflightRunner::new(&config).run();

    assert!(report.has_errors(), "{report:?}");
    assert!(report
        .errors()
        .iter()
        .any(|finding| finding.check == PreflightCheck::MirrorIsolation));
}

#[test]
fn preflight_validates_lifecycle_and_adaptive_guardrails() {
    let config = write_file(
        "lifecycle-adaptive",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            r#"
[runtime.lifecycle]
startup_grace_ms = 0
shutdown_grace_ms = 2_000
termination_grace_period_seconds = 1

[runtime.production]
adaptive_mode = "recommend"
adaptive_window_ms = 0
adaptive_min_confidence = 1.5
"#,
        ),
    );

    let report = PreflightRunner::new(&config).run();

    assert!(report.has_errors(), "{report:?}");
    assert!(report
        .errors()
        .iter()
        .any(|finding| finding.check == PreflightCheck::LifecycleGuardrails));
    assert!(report
        .errors()
        .iter()
        .any(|finding| finding.check == PreflightCheck::AdaptiveGuardrails));
}

#[test]
fn preflight_reports_warnings_separately_from_errors() {
    let config = write_file(
        "warnings-separate",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            r#"
[mirror]
mirroring_enabled = true
mirror_mode = "read_only"
mirror_timeout_ms = 100
mirror_max_in_flight = 128
mirror_sample_rate = 1.0
mirror_writes_enabled = false
mirror_require_isolated_target = true
target.address = "127.0.0.1:5432"
target.isolated = false
"#,
        ),
    );

    let report = PreflightRunner::new(&config).run();

    assert!(!report.warnings().is_empty());
    assert!(!report.errors().is_empty());
    assert!(report
        .warnings()
        .iter()
        .all(|finding| finding.severity == pg_kinetic_proxy::preflight::PreflightSeverity::Warning));
    assert!(report
        .errors()
        .iter()
        .all(|finding| finding.severity == pg_kinetic_proxy::preflight::PreflightSeverity::Error));
    let json = report.render_json();
    assert!(json.contains("\"warnings\""));
    assert!(json.contains("\"errors\""));
}

#[test]
fn preflight_output_supports_json() {
    let config = write_file(
        "json-output",
        &base_config("127.0.0.1:0", "127.0.0.1:5432", ""),
    );

    let output = run_preflight(&config);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":true"));
    assert!(stdout.contains("\"warnings\""));
    assert!(stdout.contains("\"errors\""));
}

#[test]
fn preflight_redacts_secrets() {
    let auth_users_file = write_auth_users_file("redacted", &format!("alice = {SCRAM_VERIFIER}\n"));
    let config = write_file(
        "redaction",
        &base_config(
            "127.0.0.1:0",
            "127.0.0.1:5432",
            &format!(
                r#"
[auth]
auth_mode = "scram_sha_256"
auth_users_file = "{}"
backend_password_env_var_name = "PG_KINETIC_BACKEND_PASSWORD"
"#,
                toml_path(&auth_users_file)
            ),
        ),
    );

    let output = run_preflight(&config);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":true"));
    assert!(!stdout.contains(SCRAM_VERIFIER));
    assert!(!stdout.contains("PG_KINETIC_BACKEND_PASSWORD"));
}
