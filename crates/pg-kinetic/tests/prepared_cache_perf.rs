use pg_kinetic::{
    prepare::{InvalidationScope, PreparedCatalog},
    wire::sqlstate::SqlState,
};

#[test]
fn repeated_lookup_reuses_a_stable_compact_cache_key() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select $1::int", vec![23]).clone();

    assert_ne!(statement.cache_key(), 0);
    assert_eq!(
        catalog
            .get_for_current_route_map("stmt1")
            .expect("current prepared statement")
            .cache_key(),
        statement.cache_key()
    );

    catalog.mark_materialized(10, &statement);

    for _ in 0..32 {
        let cached = catalog
            .get_for_current_route_map("stmt1")
            .expect("current prepared statement");
        assert_eq!(cached.cache_key(), statement.cache_key());
        assert!(catalog.is_materialized(10, cached));
    }
}

#[test]
fn replacement_discards_old_materialization_without_growing_the_cache() {
    let mut catalog = PreparedCatalog::new(42);
    let original = catalog.upsert("stmt1", "select 1", vec![]).clone();
    catalog.mark_materialized(10, &original);

    let replacement = catalog.upsert("stmt1", "select 2", vec![]).clone();

    assert_ne!(original.cache_key(), replacement.cache_key());
    assert!(!catalog.is_materialized(10, &original));
    assert!(!catalog.is_materialized(10, &replacement));
    catalog.mark_materialized(10, &replacement);
    assert_eq!(
        catalog
            .snapshot()
            .into_iter()
            .find(|statement| statement.client_statement_name == "stmt1")
            .expect("replacement snapshot")
            .materialized_backend_count,
        1
    );
}

#[test]
fn backend_invalidation_leaves_other_catalogs_and_backends_hot() {
    let mut first_catalog = PreparedCatalog::new(42);
    let first_statement = first_catalog.upsert("stmt1", "select 1", vec![]).clone();
    first_catalog.mark_materialized(10, &first_statement);
    first_catalog.mark_materialized(11, &first_statement);

    let mut unrelated_catalog = PreparedCatalog::new(43);
    let unrelated_statement = unrelated_catalog
        .upsert("stmt1", "select 1", vec![])
        .clone();
    unrelated_catalog.mark_materialized(10, &unrelated_statement);

    assert_eq!(
        first_catalog.invalidate_for_sqlstate(SqlState::InvalidSqlStatementName, 10),
        InvalidationScope::Backend
    );
    assert!(!first_catalog.is_materialized(10, &first_statement));
    assert!(first_catalog.is_materialized(11, &first_statement));
    assert!(unrelated_catalog.is_materialized(10, &unrelated_statement));
}

#[test]
fn materialization_and_invalidation_counters_remain_stable() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select 1", vec![]).clone();
    catalog.mark_materialized(10, &statement);
    catalog.mark_materialized(11, &statement);

    assert_eq!(
        catalog.invalidate_for_sqlstate(SqlState::UndefinedTable, 10),
        InvalidationScope::AllBackends
    );

    let snapshot = catalog
        .snapshot()
        .into_iter()
        .find(|statement| statement.client_statement_name == "stmt1")
        .expect("prepared statement snapshot");
    assert_eq!(snapshot.materialized_backend_count, 0);
    assert_eq!(snapshot.invalidation_count, 2);
}
