use std::sync::Arc;

use pg_kinetic::{
    config::PolicyConfig,
    core::policy::PolicyMode,
    proxy_runtime::{
        policy::{PolicyReloadErrorCode, PolicyStore},
        reload::{reload_policy_once, record_policy_reload},
        snapshot::SnapshotStore,
    },
};
use serde::Deserialize;

#[derive(Deserialize)]
struct PolicyDocument {
    policy: PolicyConfig,
}

fn policy_config(toml: &str) -> PolicyConfig {
    toml::from_str::<PolicyDocument>(toml)
        .expect("policy config parses")
        .policy
}

fn base_store() -> PolicyStore {
    PolicyStore::new(PolicyConfig::default())
}

#[test]
fn valid_policy_reload_swaps_atomically() {
    let store = base_store();
    let before = store.snapshot();
    let next_policy = policy_config(
        r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "route-1"

[[policy.inline_rules]]
policy_id = "shard-fallback"
hook_point = "before_routing"
kind = "shard_override"
target_id = "tenant-a"
"#,
    );

    let result = store.reload(
        &next_policy,
        ["route-0", "route-1"],
        true,
        ["tenant-a", "tenant-b"],
    );
    let after = store.snapshot();

    assert!(result.success);
    assert_eq!(result.policy_generation_id, 1);
    assert!(!Arc::ptr_eq(&before, &after));
    assert_eq!(before.generation.as_u64(), 0);
    assert_eq!(after.generation.as_u64(), 1);
    assert_eq!(before.runtime.policy_mode(), PolicyMode::Disabled);
    assert_eq!(after.runtime.policy_mode(), PolicyMode::Enforce);
    assert_eq!(after.config, next_policy);
}

#[test]
fn invalid_policy_reload_is_rejected_and_old_policy_remains_active() {
    let store = base_store();
    let before = store.snapshot();
    let invalid_policy = policy_config(
        r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "route-1"
"#,
    );

    let result = store.reload(&invalid_policy, ["route-0"], false, std::iter::empty::<&str>());
    let after = store.snapshot();

    assert!(!result.success);
    assert_eq!(result.policy_generation_id, 0);
    assert_eq!(result.error_code, Some(PolicyReloadErrorCode::RouteReferenceMissing));
    assert!(result
        .error
        .as_deref()
        .expect("validation error")
        .contains("route override target"));
    assert!(Arc::ptr_eq(&before, &after));
    assert_eq!(after.generation.as_u64(), 0);
    assert_eq!(after.runtime.policy_mode(), PolicyMode::Disabled);
}

#[test]
fn policy_generation_id_increments_on_successful_reload() {
    let store = base_store();
    let first_policy = policy_config(
        r#"
[policy]
policy_mode = "disabled"

[[policy.inline_rules]]
policy_id = "allow-route"
hook_point = "before_routing"
kind = "allow"
"#,
    );
    let second_policy = policy_config(
        r#"
[policy]
policy_mode = "dry_run"

[[policy.inline_rules]]
policy_id = "allow-route"
hook_point = "before_routing"
kind = "allow"
"#,
    );

    let first = store.reload(&first_policy, ["route-0"], false, std::iter::empty::<&str>());
    let second = store.reload(&second_policy, ["route-0"], false, std::iter::empty::<&str>());

    assert!(first.success);
    assert!(second.success);
    assert_eq!(first.policy_generation_id, 1);
    assert_eq!(second.policy_generation_id, 2);
    assert_eq!(store.generation().as_u64(), 2);
}

#[test]
fn policy_validation_checks_route_and_shard_references_against_active_route_map() {
    let store = base_store();
    let route_policy = policy_config(
        r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "route-1"
"#,
    );
    let shard_policy = policy_config(
        r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "shard-fallback"
hook_point = "before_routing"
kind = "shard_override"
target_id = "tenant-a"
"#,
    );

    let route_error = store.reload(&route_policy, ["route-0"], false, std::iter::empty::<&str>());
    let shard_error = store.reload(&shard_policy, ["route-0"], true, ["tenant-b"]);

    assert_eq!(
        route_error.error_code,
        Some(PolicyReloadErrorCode::RouteReferenceMissing)
    );
    assert_eq!(
        shard_error.error_code,
        Some(PolicyReloadErrorCode::ShardReferenceMissing)
    );
}

#[test]
fn disabled_policy_mode_can_reload_but_not_evaluate() {
    let store = base_store();
    let disabled_policy = policy_config(
        r#"
[policy]
policy_mode = "disabled"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "route-0"
"#,
    );

    let result = store.reload(&disabled_policy, ["route-0"], false, std::iter::empty::<&str>());

    assert!(result.success);
    assert_eq!(store.runtime().policy_mode(), PolicyMode::Disabled);
    assert_eq!(store.config(), disabled_policy);
}

#[test]
fn dry_run_mode_can_reload_without_enforcing() {
    let store = base_store();
    let dry_run_policy = policy_config(
        r#"
[policy]
policy_mode = "dry_run"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "route-0"
"#,
    );

    let result = store.reload(&dry_run_policy, ["route-0"], false, std::iter::empty::<&str>());

    assert!(result.success);
    assert_eq!(store.runtime().policy_mode(), PolicyMode::DryRun);
    assert_eq!(store.config(), dry_run_policy);
}

#[test]
fn reload_snapshots_expose_success_failure_generation_and_validation_error_code() {
    let store = base_store();
    let snapshot_store = SnapshotStore::new();
    let valid_policy = policy_config(
        r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "route-0"
"#,
    );
    let invalid_policy = policy_config(
        r#"
[policy]
policy_mode = "enforce"

[[policy.inline_rules]]
policy_id = "route-fallback"
hook_point = "before_routing"
kind = "route_override"
target_id = "route-1"
"#,
    );

    let success = reload_policy_once(
        &store,
        &valid_policy,
        ["route-0"],
        false,
        std::iter::empty::<&str>(),
        Some(&snapshot_store),
    );
    let failure = reload_policy_once(
        &store,
        &invalid_policy,
        ["route-0"],
        false,
        std::iter::empty::<&str>(),
        Some(&snapshot_store),
    );

    assert!(success.success);
    assert!(!failure.success);

    let snapshots = snapshot_store.policy_reload_snapshots();
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].policy_generation_id, 1);
    assert!(snapshots[0].success);
    assert_eq!(snapshots[0].error_code, None);
    assert_eq!(snapshots[1].policy_generation_id, 1);
    assert!(!snapshots[1].success);
    assert_eq!(
        snapshots[1].error_code,
        Some(PolicyReloadErrorCode::RouteReferenceMissing)
    );
}

#[test]
fn record_policy_reload_can_persist_a_snapshot_directly() {
    let snapshot_store = SnapshotStore::new();
    let result = pg_kinetic::proxy_runtime::policy::PolicyReloadResult {
        success: true,
        policy_generation_id: 7,
        error_code: None,
        error: None,
    };

    record_policy_reload(&snapshot_store, &result);

    let snapshots = snapshot_store.policy_reload_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].policy_generation_id, 7);
    assert!(snapshots[0].success);
}
