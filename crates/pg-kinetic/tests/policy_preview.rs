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

fn run_preview(config: &Path, route: &str, shard: &str) -> std::process::Output {
    Command::new(binary_path())
        .args([
            "policy-preview",
            "--config",
            config.to_str().expect("config path"),
            "--database",
            "appdb",
            "--user",
            "app",
            "--route",
            route,
            "--shard",
            shard,
            "--query-class",
            "read_only",
            "--format",
            "json",
        ])
        .output()
        .expect("run preview")
}

fn standard_config(listen_addr: &str, policy_section: &str) -> String {
    format!(
        r#"
[connection]
listen_addr = "{listen_addr}"
backend_addr = "127.0.0.1:5432"

{policy_section}
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
            r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "orders-shadow"
"#,
        ),
    );

    let output = run_preview(&config, "orders", "shard_a");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":true"));
    drop(listener);
}

#[test]
fn preview_evaluates_policy_against_a_redacted_synthetic_context() {
    let config = write_config(
        "redacted-context",
        &standard_config(
            "127.0.0.1:6543",
            r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "orders-shadow"
"#,
        ),
    );

    let output = run_preview(&config, "orders", "shard_a");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"context\":"));
    assert!(stdout.contains("sensitive_inputs=<redacted>"));
    assert!(!stdout.contains("preview-secret-token"));
    assert!(!stdout.contains("preview-password"));
}

#[test]
fn preview_shows_original_route_and_policy_adjusted_route() {
    let config = write_config(
        "route-adjustment",
        &standard_config(
            "127.0.0.1:6543",
            r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "orders-shadow"
"#,
        ),
    );

    let output = run_preview(&config, "orders", "shard_a");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"original_route\":\"orders\""));
    assert!(stdout.contains("\"policy_adjusted_route\":\"orders-shadow\""));
}

#[test]
fn preview_shows_dry_run_outcome() {
    let config = write_config(
        "dry-run",
        &standard_config(
            "127.0.0.1:6543",
            r#"
[policy]
policy_mode = "dry_run"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "orders-shadow"
"#,
        ),
    );

    let output = run_preview(&config, "orders", "shard_a");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"dry_run_outcome\":\"dry_run\""));
    assert!(stdout.contains("\"dry_run_reason\":\"would_override\""));
}

#[test]
fn preview_shows_deny_action_and_sqlstate() {
    let config = write_config(
        "deny-action",
        &standard_config(
            "127.0.0.1:6543",
            r#"
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

    let output = run_preview(&config, "orders", "shard_a");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"action\":\"deny\""));
    assert!(stdout.contains("\"sqlstate\":\"P0001\""));
}

#[test]
fn preview_redacts_secrets() {
    let config = write_config(
        "redacts-secrets",
        &standard_config(
            "127.0.0.1:6543",
            r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "orders-shadow"
"#,
        ),
    );

    let output = run_preview(&config, "orders", "shard_a");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("<redacted>"));
    assert!(!stdout.contains("preview-secret-token"));
}

#[test]
fn preview_exits_non_zero_on_invalid_policy_config() {
    let config = write_config(
        "invalid-policy",
        &standard_config(
            "127.0.0.1:6543",
            r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "deny-empty"
hook_point = "before_routing"
kind = "deny"
reason = "   "
"#,
        ),
    );

    let output = run_preview(&config, "orders", "shard_a");

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"ok\":false"));
    assert!(stdout.contains("\"code\":\"invalid_policy_configuration\""));
}
