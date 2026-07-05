use pg_kinetic::prepare::{PreparedCatalog, PreparedStatement};

#[test]
fn stores_named_statement_with_internal_name() {
    let mut catalog = PreparedCatalog::new(42);

    let statement = catalog.upsert("stmt1", "select $1::int", vec![23]);

    assert_eq!(statement.client_name, "stmt1");
    assert_eq!(statement.query, "select $1::int");
    assert_eq!(statement.parameter_type_oids, vec![23]);
    assert_eq!(statement.backend_name, "pgk_42_1");
}

#[test]
fn keeps_unnamed_statement_unnamed() {
    let mut catalog = PreparedCatalog::new(7);

    let statement = catalog.upsert("", "select 1", vec![]);

    assert_eq!(statement.client_name, "");
    assert_eq!(statement.backend_name, "");
}

#[test]
fn removes_named_statement() {
    let mut catalog = PreparedCatalog::new(42);
    catalog.upsert("stmt1", "select 1", vec![]);

    assert!(catalog.remove("stmt1").is_some());
    assert!(catalog.get("stmt1").is_none());
}

#[test]
fn backend_materialization_tracks_statement_names() {
    let mut catalog = PreparedCatalog::new(42);
    let statement = catalog.upsert("stmt1", "select 1", vec![]).clone();

    assert!(!catalog.is_materialized(10, &statement));
    catalog.mark_materialized(10, &statement);
    assert!(catalog.is_materialized(10, &statement));
}

#[test]
fn can_store_statement_snapshot() {
    let snapshot = PreparedStatement {
        client_name: "stmt1".to_string(),
        backend_name: "pgk_99_1".to_string(),
        query: "select 1".to_string(),
        parameter_type_oids: vec![],
    };

    assert_eq!(snapshot.client_name, "stmt1");
    assert_eq!(snapshot.backend_name, "pgk_99_1");
}
