use pg_kinetic_core::{
    shard_extract::{extract_shard_hint, extract_shard_key, ShardExtraction, ShardHint},
    sharding::{RouteDefinition, RouteMapValidationInput, ShardKey, ShardedTableDefinition},
};
use pretty_assertions::assert_eq;
use std::sync::Arc;

fn route_map(table_name: &str, shard_key_column: &str) -> RouteMapValidationInput {
    RouteMapValidationInput {
        routes: vec![RouteDefinition {
            name: String::from("primary"),
            priority: None,
            is_default: true,
        }],
        sharded_tables: vec![ShardedTableDefinition {
            name: table_name.to_owned(),
            enabled: true,
            shard_key_column: Some(shard_key_column.to_owned()),
        }],
        shard_rules: vec![],
    }
}

fn assert_key(
    extraction: ShardExtraction,
    schema: Option<&str>,
    table: &str,
    column: &str,
    key: ShardKey,
) {
    assert_eq!(
        extraction,
        ShardExtraction::Key {
            schema: schema.map(Arc::from),
            table: Arc::from(table),
            column: Arc::from(column),
            key,
        }
    );
}

#[test]
fn extracts_shard_key_from_simple_where_clause() {
    let extraction = extract_shard_key(
        "select * from accounts where tenant_id = 42",
        &route_map("accounts", "tenant_id"),
    );

    assert_key(
        extraction,
        None,
        "accounts",
        "tenant_id",
        ShardKey::integer(42),
    );
}

#[test]
fn extracts_shard_key_from_simple_insert() {
    let extraction = extract_shard_key(
        "insert into accounts (id, tenant_id, name) values (1, 42, 'Ada')",
        &route_map("accounts", "tenant_id"),
    );

    assert_key(
        extraction,
        None,
        "accounts",
        "tenant_id",
        ShardKey::integer(42),
    );
}

#[test]
fn extracts_shard_key_from_simple_update() {
    let extraction = extract_shard_key(
        "update accounts set name = 'Ada' where tenant_id = 42",
        &route_map("accounts", "tenant_id"),
    );

    assert_key(
        extraction,
        None,
        "accounts",
        "tenant_id",
        ShardKey::integer(42),
    );
}

#[test]
fn extracts_shard_key_from_simple_delete() {
    let extraction = extract_shard_key(
        "delete from accounts where tenant_id = 42",
        &route_map("accounts", "tenant_id"),
    );

    assert_key(
        extraction,
        None,
        "accounts",
        "tenant_id",
        ShardKey::integer(42),
    );
}

#[test]
fn extracts_schema_qualified_table_names() {
    let extraction = extract_shard_key(
        "select * from public.accounts where tenant_id = 42",
        &route_map("public.accounts", "tenant_id"),
    );

    assert_key(
        extraction,
        Some("public"),
        "accounts",
        "tenant_id",
        ShardKey::integer(42),
    );
}

#[test]
fn rejects_expressions_functions_subqueries_casts_and_non_literal_values() {
    let route_map = route_map("accounts", "tenant_id");

    for sql in [
        "select * from accounts where tenant_id = 40 + 2",
        "select * from accounts where tenant_id = lower('42')",
        "select * from accounts where tenant_id = (select 42)",
        "select * from accounts where tenant_id = 42::bigint",
        "select * from accounts where tenant_id = $1",
    ] {
        assert_eq!(extract_shard_key(sql, &route_map), ShardExtraction::Unknown);
    }
}

#[test]
fn rejects_multiple_shard_key_values_unless_they_match() {
    let route_map = route_map("accounts", "tenant_id");

    assert_key(
        extract_shard_key(
            "select * from accounts where tenant_id = 42 and tenant_id = 42",
            &route_map,
        ),
        None,
        "accounts",
        "tenant_id",
        ShardKey::integer(42),
    );

    assert_eq!(
        extract_shard_key(
            "select * from accounts where tenant_id = 42 or tenant_id = 43",
            &route_map,
        ),
        ShardExtraction::Unknown,
    );
}

#[test]
fn returns_unknown_for_malformed_sql() {
    let route_map = route_map("accounts", "tenant_id");

    assert_eq!(
        extract_shard_key("select * from accounts where tenant_id = ", &route_map),
        ShardExtraction::Unknown,
    );
}

#[test]
fn returns_unknown_for_bind_placeholders() {
    let route_map = route_map("accounts", "tenant_id");

    assert_eq!(
        extract_shard_key(
            "update accounts set name = 'Ada' where tenant_id = $1",
            &route_map
        ),
        ShardExtraction::Unknown,
    );
}

#[test]
fn parses_supported_explicit_shard_hints() {
    assert_eq!(
        extract_shard_hint("/* pg-kinetic: shard=shard_a */ select 1"),
        ShardHint::Shard(Arc::from("shard_a")),
    );
    assert_eq!(
        extract_shard_hint("/* pg-kinetic: tenant=tenant_42 */ select 1"),
        ShardHint::Tenant(Arc::from("tenant_42")),
    );
    assert_eq!(
        extract_shard_hint("/* pg-kinetic: route=orders_eu */ select 1"),
        ShardHint::Route(Arc::from("orders_eu")),
    );
}

#[test]
fn rejects_unsupported_shard_hint_grammar() {
    assert_eq!(
        extract_shard_hint("/* pg-kinetic: shard shard_a */ select 1"),
        ShardHint::Unknown,
    );
}
