use pg_kinetic_core::{
    routing::{QueryClass, RoutingHint},
    sql_classify::{
        classify_sql, contains_data_modifying_cte, extract_routing_hint, has_multiple_statements,
        strip_leading_comments_and_whitespace,
    },
};

#[test]
fn classifies_read_only_candidates() {
    for sql in [
        "SELECT 1",
        "VALUES (1)",
        "TABLE name",
        "SHOW server_version",
    ] {
        assert_eq!(classify_sql(sql), QueryClass::ReadCandidate);
    }
}

#[test]
fn classifies_explain_select_as_read_only_candidate() {
    assert_eq!(classify_sql("EXPLAIN SELECT 1"), QueryClass::ReadCandidate);
}

#[test]
fn classifies_mixed_case_and_whitespace_like_existing_queries() {
    for (sql, expected) in [
        ("  SELECT 1", QueryClass::ReadCandidate),
        ("\n\tselect * from t", QueryClass::ReadCandidate),
        (
            "WITH x AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM x",
            QueryClass::Write,
        ),
        (
            "WiTh  x  aS  (uPdAtE t SET a=1) select 1",
            QueryClass::Write,
        ),
        ("BEGIN", QueryClass::TransactionControl),
    ] {
        assert_eq!(classify_sql(sql), expected, "sql: {sql}");
    }
}

#[test]
fn classifies_write_and_unsafe_statements() {
    for sql in [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET id = 1",
        "DELETE FROM t",
        "MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET id = u.id",
        "TRUNCATE t",
        "CREATE TABLE t(id int)",
        "ALTER TABLE t ADD COLUMN name text",
        "DROP TABLE t",
        "CALL do_work()",
        "DO $$ BEGIN NULL; END $$",
        "VACUUM",
        "ANALYZE t",
        "REINDEX TABLE t",
        "GRANT SELECT ON t TO app",
        "REVOKE SELECT ON t FROM app",
    ] {
        assert!(!classify_sql(sql).routes_to_replica(), "{sql}");
    }
}

#[test]
fn classifies_session_mutation_and_primary_only_statements() {
    for sql in [
        "SET application_name = 'api'",
        "RESET application_name",
        "DISCARD ALL",
        "LISTEN account_events",
        "UNLISTEN account_events",
        "NOTIFY account_events, 'ready'",
        "LOCK TABLE t IN ACCESS EXCLUSIVE MODE",
        "DECLARE c CURSOR FOR SELECT 1",
    ] {
        assert_eq!(classify_sql(sql), QueryClass::SessionMutation);
    }
}

#[test]
fn copy_to_stdout_differs_from_copy_from_stdin() {
    assert_eq!(
        classify_sql("COPY accounts TO STDOUT"),
        QueryClass::ReadCandidate
    );
    assert_eq!(classify_sql("COPY accounts FROM STDIN"), QueryClass::Write);
}

#[test]
fn block_comments_separate_classification_keywords() {
    assert_eq!(
        classify_sql("SELECT/*comment*/1"),
        QueryClass::ReadCandidate
    );
    assert_eq!(
        classify_sql("EXPLAIN/*comment*/SELECT 1"),
        QueryClass::ReadCandidate
    );
    assert_eq!(
        classify_sql("COPY accounts TO/*comment*/STDOUT"),
        QueryClass::ReadCandidate
    );
    assert_eq!(
        classify_sql("COPY accounts FROM/*comment*/STDIN"),
        QueryClass::Write
    );
    assert_eq!(
        classify_sql("BEGIN/*comment*/"),
        QueryClass::TransactionControl
    );
}

#[test]
fn data_modifying_ctes_are_not_replica_safe() {
    assert!(contains_data_modifying_cte(
        "WITH moved AS (INSERT INTO t VALUES (1) RETURNING 1) SELECT 1"
    ));
    assert_eq!(
        classify_sql("WITH moved AS (INSERT INTO t VALUES (1) RETURNING 1) SELECT 1"),
        QueryClass::Write
    );
}

#[test]
fn multi_statement_batches_stay_primary_unless_safe() {
    assert!(has_multiple_statements("BEGIN; SELECT 1; COMMIT"));
    assert_eq!(
        classify_sql("BEGIN; SELECT 1; COMMIT"),
        QueryClass::ReadCandidate
    );
    assert_ne!(
        classify_sql("SELECT 1; UPDATE t SET id = 1"),
        QueryClass::ReadCandidate
    );
}

#[test]
fn malformed_sql_routes_to_primary() {
    assert_eq!(classify_sql(""), QueryClass::Unknown);
    assert_eq!(classify_sql("???"), QueryClass::Unknown);
}

#[test]
fn strips_leading_comments_and_extracts_supported_hints() {
    assert_eq!(
        strip_leading_comments_and_whitespace(" /* note */ SELECT 1"),
        "SELECT 1"
    );
    assert_eq!(
        extract_routing_hint("/* pg-kinetic: primary */ SELECT 1"),
        RoutingHint::Primary
    );
    assert_eq!(
        extract_routing_hint("/* pg-kinetic: replica */ SELECT 1"),
        RoutingHint::Replica
    );
    assert_eq!(
        extract_routing_hint("/* pg-kinetic: stale-ok */ SELECT 1"),
        RoutingHint::StaleOk
    );
    assert_eq!(
        extract_routing_hint("/* pg-kinetic: strict-fresh */ SELECT 1"),
        RoutingHint::StrictFresh
    );
}
