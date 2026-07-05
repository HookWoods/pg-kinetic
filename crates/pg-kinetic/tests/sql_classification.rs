use pg_kinetic::sql::{classify, SetScope, SqlCommand};

#[test]
fn classifies_transaction_commands() {
    assert_eq!(classify("begin"), SqlCommand::Begin);
    assert_eq!(classify("START TRANSACTION"), SqlCommand::Begin);
    assert_eq!(classify("commit;"), SqlCommand::Commit);
    assert_eq!(classify("rollback"), SqlCommand::Rollback);
}

#[test]
fn classifies_set_commands() {
    assert_eq!(
        classify("set application_name = 'api'"),
        SqlCommand::Set {
            scope: SetScope::Session,
            key: "application_name".to_string(),
            value: "'api'".to_string(),
        }
    );
    assert_eq!(
        classify("set local timezone = 'UTC'"),
        SqlCommand::Set {
            scope: SetScope::Local,
            key: "timezone".to_string(),
            value: "'UTC'".to_string(),
        }
    );
}

#[test]
fn classifies_discard_commands() {
    assert_eq!(classify("discard all"), SqlCommand::DiscardAll);
    assert_eq!(classify("discard temp"), SqlCommand::DiscardTemp);
    assert_eq!(classify("discard plans"), SqlCommand::DiscardPlans);
}

#[test]
fn classifies_temp_and_lock_commands() {
    assert_eq!(
        classify("create temporary table t(id int)"),
        SqlCommand::CreateTemp
    );
    assert_eq!(
        classify("select pg_advisory_lock(42)"),
        SqlCommand::AdvisoryLock
    );
    assert_eq!(
        classify("select pg_advisory_unlock(42)"),
        SqlCommand::AdvisoryUnlock
    );
}

#[test]
fn classifies_copy_and_listen_commands() {
    assert_eq!(classify("copy accounts to stdout"), SqlCommand::Copy);
    assert_eq!(classify("listen account_events"), SqlCommand::Listen);
    assert_eq!(classify("unlisten account_events"), SqlCommand::Unlisten);
}

#[test]
fn unknown_select_is_safe_query() {
    assert_eq!(classify("select 1"), SqlCommand::Query);
}
