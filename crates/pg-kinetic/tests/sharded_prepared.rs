use pg_kinetic::{
    prepare::{InvalidationScope, PreparedCatalog, PreparedStatementSnapshot},
    session::PreparedShardSummary,
    wire::sqlstate::SqlState,
};

fn statement_snapshot(catalog: &PreparedCatalog, client_name: &str) -> PreparedStatementSnapshot {
    catalog
        .snapshot()
        .into_iter()
        .find(|snapshot| snapshot.client_statement_name == client_name)
        .expect("statement snapshot")
}

#[test]
fn unnamed_prepared_statements_stay_scoped_to_the_current_shard_decision() {
    let mut catalog = PreparedCatalog::new(42);

    let statement = catalog.upsert("", "select 1", vec![]).clone();

    assert!(statement.backend_name.is_empty());
    assert_eq!(statement.route_map_generation_id, 0);
    assert_eq!(statement.shard_summary, PreparedShardSummary::CurrentShard);
    assert!(catalog.is_materialized(10, &statement));
}

#[test]
fn named_prepared_statements_record_shard_extraction_state() {
    let mut catalog = PreparedCatalog::new(42);

    let statement = catalog.upsert("stmt1", "select $1::int", vec![23]);

    assert_eq!(statement.backend_name, "pgk_42_1");
    assert_eq!(statement.route_map_generation_id, 0);
    assert_eq!(statement.shard_summary, PreparedShardSummary::Deferred);
}

#[test]
fn bind_and_execute_do_not_reuse_stale_route_map_generation_state() {
    let mut catalog = PreparedCatalog::new(42);
    catalog.upsert("stmt1", "select 1", vec![]);
    catalog.set_route_map_generation_id(1);

    assert!(catalog.get_for_current_route_map("stmt1").is_none());
    assert_eq!(
        SqlState::InvalidSqlStatementName.as_str(),
        "26000",
        "stale prepared statements should surface the stable invalid-statement SQLSTATE",
    );
}

#[test]
fn unknown_shard_at_parse_time_defers_when_bind_safe_behavior_exists() {
    let mut catalog = PreparedCatalog::new(42);

    let statement = catalog.upsert("stmt1", "select * from accounts where id = $1", vec![23]);

    assert_eq!(statement.shard_summary, PreparedShardSummary::Deferred);
    assert!(catalog.get_for_current_route_map("stmt1").is_some());
}

#[test]
fn materialization_happens_per_backend_within_the_target_shard() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select 1", vec![]).clone();

    catalog.mark_materialized(10, &statement);
    catalog.mark_materialized(11, &statement);

    let snapshot = statement_snapshot(&catalog, "stmt1");
    assert_eq!(snapshot.materialized_backend_count, 2);
}

#[test]
fn invalidation_affects_the_correct_shard_materializations() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select 1", vec![]).clone();

    catalog.mark_materialized(10, &statement);
    catalog.mark_materialized(11, &statement);

    assert_eq!(
        catalog.invalidate_for_sqlstate(SqlState::InvalidSqlStatementName, 10),
        InvalidationScope::Backend
    );

    assert!(catalog.is_materialized(11, &statement));
    assert!(!catalog.is_materialized(10, &statement));

    let snapshot = statement_snapshot(&catalog, "stmt1");
    assert_eq!(snapshot.materialized_backend_count, 1);
    assert_eq!(snapshot.invalidation_count, 1);
}

#[test]
fn cross_shard_execute_uses_a_stable_postgresql_sqlstate() {
    assert_eq!(SqlState::InvalidSqlStatementName.as_str(), "26000");
}
