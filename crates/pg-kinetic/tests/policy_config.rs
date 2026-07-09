use std::path::PathBuf;

use clap::Parser;
use pg_kinetic::{
    config::{
        InlinePolicyActionConfig, PolicyAuditConfig, PolicyConfig, PolicyFileConfig,
        PolicyWasmConfig,
    },
    proxy_runtime::snapshot::SettingsSnapshot,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct PolicyDocument {
    policy: PolicyConfig,
}

#[test]
fn policy_engine_is_disabled_by_default() {
    let policy = PolicyConfig::default();

    assert_eq!(policy.policy_mode.as_str(), "disabled");
    assert!(policy.policy_files.is_empty());
    assert!(policy.inline_rules.is_empty());
    assert_eq!(
        policy.policy_audit,
        PolicyAuditConfig {
            policy_audit_enabled: true,
            policy_audit_sample_rate: 1.0,
        }
    );
    assert_eq!(
        policy.policy_wasm,
        PolicyWasmConfig {
            policy_wasm_enabled: false,
        }
    );
    assert_eq!(policy.policy_eval_timeout_ms, 5);
    assert_eq!(policy.policy_max_context_bytes, 8_192);
}

#[test]
fn policy_mode_can_be_parsed_from_cli() {
    let disabled = PolicyConfig::try_parse_from(["pg-kinetic", "--policy-mode", "disabled"])
        .expect("disabled parses");
    let enforce = PolicyConfig::try_parse_from(["pg-kinetic", "--policy-mode", "enforce"])
        .expect("enforce parses");
    let dry_run = PolicyConfig::try_parse_from(["pg-kinetic", "--policy-mode", "dry_run"])
        .expect("dry_run parses");

    assert_eq!(disabled.policy_mode.as_str(), "disabled");
    assert_eq!(enforce.policy_mode.as_str(), "enforce");
    assert_eq!(dry_run.policy_mode.as_str(), "dry_run");
}

#[test]
fn policy_file_paths_and_inline_rules_parse_from_config() {
    let document = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"
        policy_eval_timeout_ms = 7
        policy_max_context_bytes = 16_384

        [[policy.policy_files]]
        path = "policies/base.toml"

        [[policy.policy_files]]
        path = "policies/tenant.toml"

        [[policy.inline_rules]]
        policy_id = "deny-legacy"
        hook_point = "before_routing"
        kind = "deny"
        reason = "legacy tenant blocked"

        [[policy.inline_rules]]
        policy_id = "allow-admin"
        hook_point = "after_routing"
        kind = "allow"
        "#,
    )
    .expect("policy config parses");

    assert_eq!(document.policy.policy_mode.as_str(), "enforce");
    assert_eq!(document.policy.policy_files.len(), 2);
    assert_eq!(
        document.policy.policy_files[0],
        PolicyFileConfig {
            path: PathBuf::from("policies/base.toml"),
        }
    );
    assert_eq!(
        document.policy.policy_files[1],
        PolicyFileConfig {
            path: PathBuf::from("policies/tenant.toml"),
        }
    );
    assert_eq!(document.policy.inline_rules.len(), 2);
    assert!(matches!(
        document.policy.inline_rules[0].action,
        InlinePolicyActionConfig::Deny { .. }
    ));
    assert!(matches!(
        document.policy.inline_rules[1].action,
        InlinePolicyActionConfig::Allow
    ));
}

#[test]
fn deny_action_requires_a_reason() {
    let document = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"

        [[policy.inline_rules]]
        policy_id = "deny-empty"
        hook_point = "before_routing"
        kind = "deny"
        reason = "   "
        "#,
    )
    .expect("policy config parses");

    let error = document.policy.validate().expect_err("missing deny reason");
    assert_eq!(error, "deny action requires a reason");
}

#[test]
fn route_override_action_must_reference_an_existing_route() {
    let document = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"

        [[policy.inline_rules]]
        policy_id = "route-fallback"
        hook_point = "before_routing"
        kind = "route_override"
        target_id = "route-1"
        "#,
    )
    .expect("policy config parses");

    let error = document
        .policy
        .validate_with_context(["route-0"], false, std::iter::empty::<&str>())
        .expect_err("missing route target is rejected");
    assert_eq!(
        error,
        "route override target 'route-1' does not reference an existing route"
    );
}

#[test]
fn shard_override_action_must_reference_an_existing_shard_when_sharding_is_enabled() {
    let document = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"

        [[policy.inline_rules]]
        policy_id = "shard-fallback"
        hook_point = "before_routing"
        kind = "shard_override"
        target_id = "tenant-a"
        "#,
    )
    .expect("policy config parses");

    let error = document
        .policy
        .validate_with_context(["route-0"], true, ["tenant-b"])
        .expect_err("missing shard target is rejected");
    assert_eq!(
        error,
        "shard override target 'tenant-a' does not reference an existing shard"
    );
}

#[test]
fn wasm_policies_are_rejected_unless_policy_wasm_is_enabled() {
    let disabled_document = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"

        [[policy.inline_rules]]
        policy_id = "wasm-rule"
        hook_point = "before_checkout"
        kind = "wasm"
        module_path = "policies/wasm/policy.wasm"
        "#,
    )
    .expect("policy config parses");

    let error = disabled_document
        .policy
        .validate()
        .expect_err("wasm policies are disabled by default");
    assert_eq!(
        error,
        "wasm policies require policy_wasm_enabled to be true"
    );

    let enabled_document = toml::from_str::<PolicyDocument>(
        r#"
        [policy]
        policy_mode = "enforce"
        policy_wasm_enabled = true

        [[policy.inline_rules]]
        policy_id = "wasm-rule"
        hook_point = "before_checkout"
        kind = "wasm"
        module_path = "policies/wasm/policy.wasm"
        "#,
    )
    .expect("policy config parses");

    enabled_document
        .policy
        .validate()
        .expect("enabled wasm policies are accepted");
}

#[test]
fn secret_values_are_not_exposed_through_debug_admin_settings_snapshots() {
    let mut config = pg_kinetic::config::Config::default();
    config.auth.backend_password_env_var_name =
        Some(String::from("PG_KINETIC_BACKEND_PASSWORD"));

    let snapshot = SettingsSnapshot::from_config(&config);
    let debug = format!("{snapshot:?}");

    assert!(!debug.contains("PG_KINETIC_BACKEND_PASSWORD"));
    assert!(!debug.contains("backend_password_env_var_name"));
}
