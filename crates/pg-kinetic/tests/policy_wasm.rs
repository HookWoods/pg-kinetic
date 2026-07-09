use std::time::Duration;
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use pg_kinetic::config::{
    InlinePolicyActionConfig, InlinePolicyConfig, PolicyConfig, PolicyWasmConfig,
};
use pg_kinetic::core::policy::{
    PolicyAction, PolicyFailureMode, PolicyHookPoint, PolicyId, PolicyPluginError,
};
use pg_kinetic::proxy_runtime::policy::PolicyRuntime;

#[cfg(feature = "policy-wasm")]
use pg_kinetic::{
    core::{
        lsn::FreshnessStatus,
        policy::{PolicyMode, PolicyVersion},
        routing::{BackendRole, QueryClass},
        session::TransactionAccessMode,
    },
    proxy_runtime::policy::PolicyEvalInput,
};

#[cfg(feature = "policy-wasm")]
use std::sync::Arc;

fn wasm_module_path(label: &str, source: &str) -> PathBuf {
    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("pg-kinetic-{label}-{unique_suffix}.wat"));
    std::fs::write(&path, source).expect("write wasm module");
    path
}

fn wasm_rule(module_path: PathBuf) -> InlinePolicyConfig {
    InlinePolicyConfig {
        policy_id: PolicyId::new("wasm-rule").expect("policy id"),
        hook_point: PolicyHookPoint::BeforeCheckout,
        action: InlinePolicyActionConfig::Wasm { module_path },
    }
}

fn wasm_policy_config(module_path: PathBuf, wasm_enabled: bool) -> PolicyConfig {
    PolicyConfig {
        policy_wasm: PolicyWasmConfig {
            policy_wasm_enabled: wasm_enabled,
        },
        inline_rules: vec![wasm_rule(module_path)],
        ..PolicyConfig::default()
    }
}

#[cfg(feature = "policy-wasm")]
fn sample_policy_input() -> PolicyEvalInput {
    PolicyEvalInput {
        database: Arc::from("appdb"),
        user: Arc::from("reporter"),
        application_name: Some(Arc::from("psql")),
        route: Some(Arc::from("read-route")),
        shard: Some(Arc::from("tenant-a")),
        backend_role: BackendRole::Replica,
        query_class: QueryClass::ReadOnly,
        transaction_mode: TransactionAccessMode::ReadOnly,
        freshness_state: FreshnessStatus::Waiting,
        routing_decision: None,
        shard_route_decision: None,
        password: Some(Arc::from("swordfish")),
        bind_values: vec![Arc::from("alpha=1"), Arc::from("beta=2")],
        tls_certificate_body: Some(Arc::from("-----BEGIN CERTIFICATE-----")),
        raw_sql_text: Some(Arc::from(
            "SELECT * FROM secrets WHERE token = 'top-secret'",
        )),
        secrets: vec![Arc::from("top-secret")],
    }
}

#[test]
fn wasm_policy_support_is_disabled_by_default() {
    let config = PolicyConfig::default();
    let runtime = PolicyRuntime::from_config(&config);

    assert!(!config.policy_wasm.policy_wasm_enabled);
    assert!(!runtime.policy_wasm_enabled());
}

#[test]
fn policy_failure_mode_defaults_follow_policy_mode() {
    let enforce_runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(pg_kinetic::core::policy::PolicyMode::Enforce);
    let dry_run_runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(pg_kinetic::core::policy::PolicyMode::DryRun);

    assert_eq!(
        enforce_runtime.policy_failure_mode(),
        PolicyFailureMode::FailClosed
    );
    assert_eq!(
        dry_run_runtime.policy_failure_mode(),
        PolicyFailureMode::DisablePolicy
    );
}

#[test]
fn policy_failure_mode_maps_timeout_and_engine_errors_to_configured_fallbacks() {
    let timeout_error =
        PolicyPluginError::evaluation_timeout(Duration::from_millis(12), Duration::from_millis(5));
    let engine_error = PolicyPluginError::output_validation_failed("policy engine exploded");

    let fail_closed_runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(pg_kinetic::core::policy::PolicyMode::Enforce);
    assert_eq!(
        fail_closed_runtime.policy_failure_action_for_error(&timeout_error),
        Some(PolicyAction::deny())
    );
    assert_eq!(
        fail_closed_runtime.policy_failure_action_for_error(&engine_error),
        Some(PolicyAction::deny())
    );

    let fail_open_runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(pg_kinetic::core::policy::PolicyMode::Enforce)
        .with_policy_failure_mode(PolicyFailureMode::FailOpen);
    assert_eq!(
        fail_open_runtime.policy_failure_action_for_error(&timeout_error),
        Some(PolicyAction::allow())
    );
    assert_eq!(
        fail_open_runtime.policy_failure_action_for_error(&engine_error),
        Some(PolicyAction::allow())
    );

    let disable_policy_runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(pg_kinetic::core::policy::PolicyMode::DryRun)
        .with_policy_failure_mode(PolicyFailureMode::DisablePolicy);
    assert_eq!(
        disable_policy_runtime.policy_failure_action_for_error(&timeout_error),
        None
    );
    assert_eq!(
        disable_policy_runtime.policy_failure_action_for_error(&engine_error),
        None
    );
}

#[cfg(not(feature = "policy-wasm"))]
#[test]
fn enabling_wasm_requires_feature_support() {
    let module_path = wasm_module_path(
        "feature-off",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 1)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            i32.const 0)
        )
        "#,
    );
    let config = wasm_policy_config(module_path, true);

    let error = config
        .validate()
        .expect_err("feature-gated wasm policies are rejected");
    assert!(error.contains("policy-wasm"));
}

#[cfg(feature = "policy-wasm")]
#[test]
fn enabling_wasm_requires_explicit_config() {
    let module_path = wasm_module_path(
        "config-enabled",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 1)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            i32.const 0)
        )
        "#,
    );

    let disabled_config = wasm_policy_config(module_path.clone(), false);
    let error = disabled_config
        .validate()
        .expect_err("wasm policies remain opt-in at the config layer");
    assert_eq!(
        error,
        "wasm policies require policy_wasm_enabled to be true"
    );

    let enabled_config = wasm_policy_config(module_path, true);
    enabled_config
        .validate()
        .expect("enabled wasm policies with a valid module validate");
}

#[cfg(feature = "policy-wasm")]
#[test]
fn module_load_validates_abi_version() {
    let module_path = wasm_module_path(
        "abi-version",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 2)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            i32.const 0)
        )
        "#,
    );
    let config = wasm_policy_config(module_path, true);

    let error = config
        .validate()
        .expect_err("invalid abi version is rejected");
    assert!(error.contains("ABI version"));
}

#[cfg(feature = "policy-wasm")]
#[test]
fn module_execution_respects_timeout() {
    let module_path = wasm_module_path(
        "timeout",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 1)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            (local i32)
            i32.const 1000000
            local.set 2
            block $exit
              loop $spin
                local.get 2
                i32.const 1
                i32.sub
                local.tee 2
                i32.eqz
                br_if $exit
                br $spin
              end
            end
            i32.const 0)
        )
        "#,
    );
    let runtime = PolicyRuntime::new(Duration::from_millis(1), 8_192)
        .with_policy_mode(PolicyMode::Enforce)
        .with_policy_wasm_enabled(true);
    let rule = wasm_rule(module_path);

    let error = runtime
        .evaluate_wasm_policy(&rule, &sample_policy_input())
        .expect_err("busy wasm module times out");
    assert!(error
        .to_string()
        .contains("policy plugin evaluation exceeded"));
}

#[cfg(feature = "policy-wasm")]
#[test]
fn invalid_module_output_is_rejected() {
    let module_path = wasm_module_path(
        "invalid-output",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 1)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            i32.const 9)
        )
        "#,
    );
    let runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(PolicyMode::Enforce)
        .with_policy_wasm_enabled(true);
    let rule = wasm_rule(module_path);

    let error = runtime
        .evaluate_wasm_policy(&rule, &sample_policy_input())
        .expect_err("invalid wasm output is rejected");
    assert!(error
        .to_string()
        .contains("invalid wasm policy action code"));
}

#[cfg(feature = "policy-wasm")]
#[test]
fn deny_action_from_wasm_is_enforced_in_enforce_mode() {
    let module_path = wasm_module_path(
        "deny-enforce",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 1)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            i32.const 1)
        )
        "#,
    );
    let runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(PolicyMode::Enforce)
        .with_policy_wasm_enabled(true);
    let rule = wasm_rule(module_path);

    let decision = runtime
        .evaluate_wasm_policy(&rule, &sample_policy_input())
        .expect("deny policy evaluates");
    assert!(matches!(
        decision.action,
        pg_kinetic::core::policy::PolicyAction::Deny { .. }
    ));
    assert_eq!(
        decision.outcome,
        pg_kinetic::core::policy::PolicyOutcome::Applied
    );
}

#[cfg(feature = "policy-wasm")]
#[test]
fn deny_action_from_wasm_is_dry_run_only_in_dry_run_mode() {
    let module_path = wasm_module_path(
        "deny-dry-run",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 1)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            i32.const 1)
        )
        "#,
    );
    let runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(PolicyMode::DryRun)
        .with_policy_wasm_enabled(true);
    let rule = wasm_rule(module_path);

    let decision = runtime
        .evaluate_wasm_policy(&rule, &sample_policy_input())
        .expect("deny policy evaluates");
    assert!(matches!(
        decision.action,
        pg_kinetic::core::policy::PolicyAction::Deny { .. }
    ));
    assert_eq!(
        decision.outcome,
        pg_kinetic::core::policy::PolicyOutcome::DryRun
    );
}

#[cfg(feature = "policy-wasm")]
#[test]
fn wasm_policy_cannot_access_filesystem_network_secrets_or_raw_sql_text() {
    let module_path = wasm_module_path(
        "sanitized-input",
        r#"
        (module
          (memory (export "memory") 1)
          (func (export "pg_kinetic_policy_abi_version") (result i32)
            i32.const 1)
          (func (export "pg_kinetic_policy_evaluate") (param i32 i32) (result i32)
            i32.const 0)
        )
        "#,
    );
    let runtime = PolicyRuntime::new(Duration::from_millis(10), 8_192)
        .with_policy_mode(PolicyMode::Enforce)
        .with_policy_wasm_enabled(true);
    let rule = wasm_rule(module_path);

    let evaluator = pg_kinetic::proxy_runtime::policy_wasm::WasmPolicyEvaluator::load(
        match &rule.action {
            InlinePolicyActionConfig::Wasm { module_path } => module_path,
            _ => unreachable!("rule is always wasm"),
        },
        runtime.plugin_host_limits(),
    )
    .expect("wasm evaluator loads");

    let plugin_input = evaluator
        .build_plugin_input(
            rule.policy_id.clone(),
            PolicyVersion::new(1).expect("policy version"),
            rule.hook_point,
            &sample_policy_input(),
        )
        .expect("plugin input builds");

    let rendered_context = plugin_input.context.to_string();
    assert!(!rendered_context.contains("top-secret"));
    assert!(!rendered_context.contains("SELECT * FROM secrets"));
    assert!(rendered_context.contains("<redacted>"));
    assert!(!plugin_input.requested_access.filesystem);
    assert!(!plugin_input.requested_access.network);
    assert!(!plugin_input.requested_access.secret);
}
