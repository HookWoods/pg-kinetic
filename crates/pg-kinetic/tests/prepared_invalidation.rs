use pg_kinetic::{
    prepare::{InvalidationScope, PreparedCatalog},
    wire::sqlstate::SqlState,
};

#[test]
fn invalid_statement_name_invalidates_one_backend_materialization() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select 1", vec![]).clone();
    catalog.mark_materialized(10, &statement);

    assert_eq!(
        catalog.invalidate_for_sqlstate(SqlState::InvalidSqlStatementName, 10),
        InvalidationScope::Backend
    );
    assert!(!catalog.is_materialized(10, &statement));
}

#[test]
fn cached_plan_error_invalidates_all_materializations() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select 1", vec![]).clone();
    catalog.mark_materialized(10, &statement);
    catalog.mark_materialized(11, &statement);

    assert_eq!(
        catalog.invalidate_for_sqlstate(SqlState::FeatureNotSupported, 10),
        InvalidationScope::AllBackends
    );
    assert!(!catalog.is_materialized(10, &statement));
    assert!(!catalog.is_materialized(11, &statement));
}

#[test]
fn unrelated_error_keeps_materialization() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select 1", vec![]).clone();
    catalog.mark_materialized(10, &statement);

    assert_eq!(
        catalog.invalidate_for_sqlstate(SqlState::UniqueViolation, 10),
        InvalidationScope::None
    );
    assert!(catalog.is_materialized(10, &statement));
}
