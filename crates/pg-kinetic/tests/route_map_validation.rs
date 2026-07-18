use pg_kinetic::config::{
    MultiShardPolicyConfig, RouteMapPriority, ShardScopeConfig, ShardStrategyConfig,
    ShardTargetConfig, ShardingConfig,
};
use pg_kinetic_core::sharding::{
    ordered_route_indices, validate_route_map, RouteDefinition, RouteMapValidationErrorCode,
    RouteMapValidationInput, ShardKey, ShardRuleDefinition, ShardedTableDefinition,
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

    assert_eq!(
        sharding.route_maps[0].scope,
        ShardScopeConfig::ApplicationName {
            application_name: String::from("api"),
        }
    );
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
fn route_map_can_target_primary_and_replicas_from_route_config() {
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

    assert_eq!(
        prioritized.route_maps[0].priority,
        Some(RouteMapPriority(10))
    );
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

#[test]
fn every_shard_target_references_an_existing_route() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: Some(10),
            is_default: false,
        }],
        sharded_tables: vec![],
        shard_rules: vec![ShardRuleDefinition::Hash {
            route: String::from("missing"),
            weight: 1,
        }],
    });

    assert_eq!(
        report
            .errors()
            .iter()
            .map(|error| error.code().as_str())
            .collect::<Vec<_>>(),
        vec![RouteMapValidationErrorCode::UnknownShardTarget.as_str()]
    );
}

#[test]
fn route_map_requires_at_least_one_shard_rule() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: None,
            is_default: false,
        }],
        sharded_tables: vec![],
        shard_rules: vec![],
    });

    assert_eq!(report.errors().len(), 1);
    assert_eq!(
        report.errors()[0].code(),
        RouteMapValidationErrorCode::MissingShardRule
    );
}

#[test]
fn every_enabled_sharded_table_defines_a_shard_key_column() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: None,
            is_default: false,
        }],
        sharded_tables: vec![ShardedTableDefinition {
            name: String::from("public.orders"),
            enabled: true,
            shard_key_column: None,
        }],
        shard_rules: vec![ShardRuleDefinition::Hash {
            route: String::from("primary"),
            weight: 1,
        }],
    });

    assert_eq!(
        report
            .errors()
            .iter()
            .map(|error| error.code().as_str())
            .collect::<Vec<_>>(),
        vec![RouteMapValidationErrorCode::MissingShardKeyColumn.as_str()]
    );
}

#[test]
fn hash_shard_weights_must_be_positive() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: None,
            is_default: false,
        }],
        sharded_tables: vec![],
        shard_rules: vec![ShardRuleDefinition::Hash {
            route: String::from("primary"),
            weight: 0,
        }],
    });

    assert_eq!(
        report
            .errors()
            .iter()
            .map(|error| error.code().as_str())
            .collect::<Vec<_>>(),
        vec![RouteMapValidationErrorCode::InvalidHashWeight.as_str()]
    );
}

#[test]
fn range_boundaries_must_be_ordered() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: None,
            is_default: false,
        }],
        sharded_tables: vec![],
        shard_rules: vec![ShardRuleDefinition::Range {
            route: String::from("primary"),
            lower_bound: ShardKey::integer(20),
            lower_inclusive: true,
            upper_bound: ShardKey::integer(10),
            upper_inclusive: true,
        }],
    });

    assert_eq!(
        report
            .errors()
            .iter()
            .map(|error| error.code().as_str())
            .collect::<Vec<_>>(),
        vec![RouteMapValidationErrorCode::InvalidRangeBoundaries.as_str()]
    );
}

#[test]
fn list_values_must_be_unique() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: None,
            is_default: false,
        }],
        sharded_tables: vec![],
        shard_rules: vec![ShardRuleDefinition::List {
            route: String::from("primary"),
            values: vec![ShardKey::text("alpha"), ShardKey::text("alpha")],
        }],
    });

    assert_eq!(
        report
            .errors()
            .iter()
            .map(|error| error.code().as_str())
            .collect::<Vec<_>>(),
        vec![RouteMapValidationErrorCode::DuplicateListValue.as_str()]
    );
}

#[test]
fn route_priorities_are_deterministic() {
    let routes_a = vec![
        RouteDefinition {
            name: String::from("gamma"),
            priority: Some(50),
            is_default: false,
        },
        RouteDefinition {
            name: String::from("alpha"),
            priority: None,
            is_default: false,
        },
        RouteDefinition {
            name: String::from("beta"),
            priority: Some(10),
            is_default: false,
        },
    ];
    let routes_b = vec![
        RouteDefinition {
            name: String::from("beta"),
            priority: Some(10),
            is_default: false,
        },
        RouteDefinition {
            name: String::from("gamma"),
            priority: Some(50),
            is_default: false,
        },
        RouteDefinition {
            name: String::from("alpha"),
            priority: None,
            is_default: false,
        },
    ];

    let ordered_a = ordered_route_indices(&routes_a)
        .into_iter()
        .map(|index| routes_a[index].name.as_str())
        .collect::<Vec<_>>();
    let ordered_b = ordered_route_indices(&routes_b)
        .into_iter()
        .map(|index| routes_b[index].name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(ordered_a, vec!["beta", "gamma", "alpha"]);
    assert_eq!(ordered_a, ordered_b);
}

#[test]
fn conflicting_default_routes_are_rejected() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![
            RouteDefinition {
                name: String::from("primary-a"),
                priority: Some(10),
                is_default: true,
            },
            RouteDefinition {
                name: String::from("primary-b"),
                priority: Some(20),
                is_default: true,
            },
        ],
        sharded_tables: vec![],
        shard_rules: vec![ShardRuleDefinition::Hash {
            route: String::from("primary-a"),
            weight: 1,
        }],
    });

    assert_eq!(
        report
            .errors()
            .iter()
            .map(|error| error.code().as_str())
            .collect::<Vec<_>>(),
        vec![RouteMapValidationErrorCode::ConflictingDefaultRoutes.as_str()]
    );
}

#[test]
fn route_map_validation_reports_stable_error_codes() {
    let report = validate_route_map(&RouteMapValidationInput {
        routes: vec![
            RouteDefinition {
                name: String::from("primary"),
                priority: Some(20),
                is_default: true,
            },
            RouteDefinition {
                name: String::from("replica"),
                priority: Some(10),
                is_default: true,
            },
        ],
        sharded_tables: vec![ShardedTableDefinition {
            name: String::from("public.orders"),
            enabled: true,
            shard_key_column: None,
        }],
        shard_rules: vec![
            ShardRuleDefinition::Hash {
                route: String::from("missing"),
                weight: 0,
            },
            ShardRuleDefinition::Range {
                route: String::from("primary"),
                lower_bound: ShardKey::integer(50),
                lower_inclusive: true,
                upper_bound: ShardKey::integer(10),
                upper_inclusive: false,
            },
            ShardRuleDefinition::List {
                route: String::from("replica"),
                values: vec![ShardKey::text("alpha"), ShardKey::text("alpha")],
            },
        ],
    });

    let error_codes = report
        .errors()
        .iter()
        .map(|error| error.code().as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        error_codes,
        vec![
            RouteMapValidationErrorCode::ConflictingDefaultRoutes.as_str(),
            RouteMapValidationErrorCode::MissingShardKeyColumn.as_str(),
            RouteMapValidationErrorCode::UnknownShardTarget.as_str(),
            RouteMapValidationErrorCode::InvalidHashWeight.as_str(),
            RouteMapValidationErrorCode::InvalidRangeBoundaries.as_str(),
            RouteMapValidationErrorCode::DuplicateListValue.as_str(),
        ]
    );
}
