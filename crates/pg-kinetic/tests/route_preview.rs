use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_pg-kinetic")
}

fn temp_config_path(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    std::env::temp_dir().join(format!("pg-kinetic-{name}-{unique}.toml"))
}

fn write_config(name: &str, contents: &str) -> PathBuf {
    let path = temp_config_path(name);
    fs::write(&path, contents).expect("write config");
    path
}

fn run_preview(config: &Path, database: &str, user: &str, sql: &str) -> std::process::Output {
    Command::new(binary_path())
        .args([
            "route-preview",
            "--config",
            config.to_str().expect("config path"),
            "--database",
            database,
            "--user",
            user,
            "--sql",
            sql,
        ])
        .output()
        .expect("run preview")
}

fn standard_config(listen_addr: &str, multi_shard_policy: &str, route_maps: &str) -> String {
    format!(
        r#"
[connection]
listen_addr = "{listen_addr}"
backend_addr = "127.0.0.1:5432"

[sharding]
sharding_enabled = true
multi_shard_policy = "{multi_shard_policy}"

{route_maps}
"#
    )
}

#[test]
fn preview_loads_config_without_starting_proxy_listeners() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let listen_addr = listener.local_addr().expect("listener addr");
    let config = write_config(
        "no-listener",
        &standard_config(
            &listen_addr.to_string(),
            "first_match",
            r#"
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
"#,
        ),
    );

    let output = run_preview(
        &config,
        "appdb",
        "app",
        "select * from public.orders where tenant_id = 42",
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":true"));
    assert!(stdout.contains("\"backend_role\":\"primary\""));
    drop(listener);
}

#[test]
fn preview_prints_route_shard_backend_and_reason() {
    let config = write_config(
        "preview-output",
        &standard_config(
            "127.0.0.1:6543",
            "first_match",
            r#"
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
"#,
        ),
    );

    let output = run_preview(
        &config,
        "appdb",
        "app",
        "select * from public.orders where tenant_id = 42",
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"route\":\"appdb/app/<none>/default\""));
    assert!(stdout.contains("\"shard_id\":\"tenant-a\""));
    assert!(stdout.contains("\"backend_role\":\"primary\""));
    assert!(stdout.contains("\"reason\":\"hash_match\""));
}

#[test]
fn preview_reports_multi_shard_rejection() {
    let config = write_config(
        "multi-shard-rejection",
        &standard_config(
            "127.0.0.1:6543",
            "reject",
            r#"
[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-a"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
"#,
        ),
    );

    let output = run_preview(
        &config,
        "appdb",
        "app",
        "/* pg-kinetic: shard=tenant-a */ select 1",
    );

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":false"));
    assert!(stdout.contains("\"reason\":\"multi_shard_rejected\""));
    assert!(stdout.contains("\"code\":\"multi_shard_rejected\""));
}

#[test]
fn preview_reports_primary_fallback() {
    let config = write_config(
        "primary-fallback",
        &standard_config(
            "127.0.0.1:6543",
            "first_match",
            r#"
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
"#,
        ),
    );

    let output = run_preview(&config, "appdb", "app", "select 1");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"reason\":\"fallback_primary\""));
    assert!(stdout.contains("\"backend_role\":\"primary\""));
    assert!(stdout.contains("\"shard_id\":null"));
}

#[test]
fn preview_handles_explicit_shard_and_tenant_hints() {
    let shard_config = write_config(
        "explicit-shard",
        &standard_config(
            "127.0.0.1:6543",
            "first_match",
            r#"
[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-a"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"
"#,
        ),
    );

    let tenant_config = write_config(
        "explicit-tenant",
        &standard_config(
            "127.0.0.1:6543",
            "first_match",
            r#"
[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-b"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-b"
"#,
        ),
    );

    let shard_output = run_preview(
        &shard_config,
        "appdb",
        "app",
        "/* pg-kinetic: shard=tenant-a */ select 1",
    );
    let tenant_output = run_preview(
        &tenant_config,
        "appdb",
        "app",
        "/* pg-kinetic: tenant=tenant-b */ select 1",
    );

    assert!(
        shard_output.status.success(),
        "{}",
        String::from_utf8_lossy(&shard_output.stderr)
    );
    assert!(
        tenant_output.status.success(),
        "{}",
        String::from_utf8_lossy(&tenant_output.stderr)
    );

    let shard_stdout = String::from_utf8_lossy(&shard_output.stdout);
    let tenant_stdout = String::from_utf8_lossy(&tenant_output.stdout);
    assert!(shard_stdout.contains("\"shard_id\":\"tenant-a\""));
    assert!(shard_stdout.contains("\"reason\":\"admin_override\""));
    assert!(tenant_stdout.contains("\"shard_id\":\"tenant-b\""));
    assert!(tenant_stdout.contains("\"reason\":\"admin_override\""));
}

#[test]
fn preview_output_redacts_secrets() {
    let config = write_config(
        "redaction",
        &standard_config(
            "127.0.0.1:6543",
            "first_match",
            r#"
[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-secret-value"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-secret"
"#,
        ),
    );

    let output = run_preview(&config, "appdb", "app", "select 1");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("tenant-secret-value"));
}

#[test]
fn preview_exits_non_zero_on_invalid_route_map() {
    let config = write_config(
        "invalid-route-map",
        &standard_config(
            "127.0.0.1:6543",
            "reject",
            r#"
[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-a"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
"#,
        ),
    );

    let output = run_preview(
        &config,
        "appdb",
        "app",
        "/* pg-kinetic: shard=tenant-a */ select 1",
    );

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"reason\":\"multi_shard_rejected\""));
}
