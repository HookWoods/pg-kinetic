use pg_kinetic::config::{
    MultiShardPolicyConfig, RouteMapPriority, ShardScopeConfig, ShardStrategyConfig,
    ShardTargetConfig, ShardingConfig,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ShardingFileConfig {
    sharding: ShardingConfig,
}

fn parse_sharding_config(toml: &str) -> ShardingConfig {
    toml::from_str::<ShardingFileConfig>(toml)
        .expect("parse sharding config")
        .sharding
}

#[test]
fn sharding_is_disabled_by_default() {
    let sharding = ShardingConfig::default();

    assert!(!sharding.sharding_enabled);
    assert_eq!(sharding.multi_shard_policy, MultiShardPolicyConfig::Reject);
    assert!(sharding.route_map_reload_strict);
    assert!(!sharding.route_preview_enabled);
    assert!(sharding.route_maps.is_empty());
}

#[test]
fn route_map_can_match_database_and_user() {
    let sharding = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "database_user"
database = "postgres"
user = "app"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"
"#,
    );

    assert_eq!(sharding.route_maps.len(), 1);
    assert_eq!(
        sharding.route_maps[0].scope,
        ShardScopeConfig::DatabaseUser {
            database: String::from("postgres"),
            user: String::from("app"),
        }
    );
}

#[test]
fn route_map_can_match_application_name() {
    let sharding = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "application_name"
application_name = "api"

[sharding.route_maps.strategy]
kind = "range"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
"#,
    );

    assert_eq!(sharding.route_maps[0].scope, ShardScopeConfig::ApplicationName {
        application_name: String::from("api"),
    });
}

#[test]
fn route_map_can_match_schema_and_table() {
    let sharding = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "schema_table"
schema = "public"
table = "orders"

[sharding.route_maps.strategy]
kind = "list"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-c"
"#,
    );

    assert_eq!(
        sharding.route_maps[0].scope,
        ShardScopeConfig::SchemaTable {
            schema: String::from("public"),
            table: String::from("orders"),
        }
    );
}

#[test]
fn route_map_can_match_tenant_key() {
    let sharding = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-123"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-d"
"#,
    );

    assert_eq!(
        sharding.route_maps[0].scope,
        ShardScopeConfig::TenantKey {
            tenant_key: String::from("tenant-123"),
        }
    );
}

#[test]
fn hash_range_and_list_strategies_parse_from_config() {
    let hash = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "database_user"
database = "postgres"
user = "app"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-h"
"#,
    );
    let range = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "application_name"
application_name = "reporting"

[sharding.route_maps.strategy]
kind = "range"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-r"
"#,
    );
    let list = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "schema_table"
schema = "public"
table = "invoices"

[sharding.route_maps.strategy]
kind = "list"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-l"
"#,
    );

    assert_eq!(hash.route_maps[0].strategy, ShardStrategyConfig::Hash);
    assert_eq!(range.route_maps[0].strategy, ShardStrategyConfig::Range);
    assert_eq!(list.route_maps[0].strategy, ShardStrategyConfig::List);
}

#[test]
fn route_map_can_target_primary_and_replicas_from_phase_7_route_config() {
    let sharding = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-123"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-primary"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-replica"
"#,
    );

    assert_eq!(
        sharding.route_maps[0].targets,
        vec![
            ShardTargetConfig::Primary {
                shard_id: String::from("tenant-primary"),
            },
            ShardTargetConfig::Replicas {
                shard_id: String::from("tenant-replica"),
            },
        ]
    );
}

#[test]
fn invalid_shard_target_names_are_rejected() {
    let error = toml::from_str::<ShardingFileConfig>(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-123"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "secondary"
shard_id = "tenant-x"
"#,
    )
    .expect_err("invalid target kind is rejected");

    assert!(error.to_string().contains("secondary"));
}

#[test]
fn duplicate_shard_ids_are_rejected() {
    let error = toml::from_str::<ShardingFileConfig>(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "tenant_key"
tenant_key = "tenant-123"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-x"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-x"
"#,
    )
    .expect_err("duplicate shard ids are rejected");

    assert!(error.to_string().contains("duplicate shard id"));
}

#[test]
fn overlapping_scopes_are_rejected_unless_priority_is_explicit() {
    let overlapping = toml::from_str::<ShardingFileConfig>(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "database_user"
database = "postgres"
user = "app"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "database_user"
database = "postgres"
user = "app"

[sharding.route_maps.strategy]
kind = "range"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
"#,
    );

    assert!(overlapping.is_err(), "duplicate scopes should be rejected");

    let prioritized = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

[[sharding.route_maps]]
priority = 10
[sharding.route_maps.scope]
kind = "database_user"
database = "postgres"
user = "app"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "database_user"
database = "postgres"
user = "app"

[sharding.route_maps.strategy]
kind = "range"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
"#,
    );

    assert_eq!(prioritized.route_maps[0].priority, Some(RouteMapPriority(10)));
}

#[test]
fn secret_values_are_not_exposed_through_debug_admin_config_snapshots() {
    let sharding = parse_sharding_config(
        r#"
[sharding]
sharding_enabled = true

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
    );

    let debug = format!("{sharding:?}");

    assert!(!debug.contains("tenant-secret-value"));
    assert!(debug.contains("<redacted>"));
}
