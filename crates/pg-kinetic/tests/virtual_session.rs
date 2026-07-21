use pg_kinetic::{
    sql::classify,
    virtual_session::{PinReason, VirtualSession},
};

#[test]
fn open_transaction_pins_backend() {
    let mut session = VirtualSession::default();

    session.apply_sql(classify("begin"));

    assert_eq!(session.pin_reason(), Some(PinReason::OpenTransaction));
}

#[test]
fn commit_releases_transaction_pin() {
    let mut session = VirtualSession::default();

    session.apply_sql(classify("begin"));
    session.apply_sql(classify("commit"));

    assert_eq!(session.pin_reason(), None);
}

#[test]
fn multi_statement_transaction_releases_transaction_pin() {
    let mut session = VirtualSession::default();

    session.apply_transaction_sql("begin; update accounts set balance = balance; commit");

    assert_eq!(session.pin_reason(), None);
}

#[test]
fn session_setting_records_replayable_setting() {
    let mut session = VirtualSession::default();

    session.apply_sql(classify("set application_name = 'api'"));

    assert_eq!(session.pin_reason(), None);
    assert_eq!(
        session.replay_sql(),
        vec!["SET application_name = 'api'".to_string()]
    );
}

#[test]
fn transaction_command_tracking_does_not_apply_session_settings() {
    let mut session = VirtualSession::default();
    let command = classify("set application_name = 'api'");

    session.apply_transaction_command(&command, false);

    assert!(session.replay_sql().is_empty());
    assert_eq!(session.pin_reason(), None);
}

#[test]
fn unsafe_setting_pins_session() {
    let mut session = VirtualSession::default();

    session.apply_sql(classify("set role app_user"));

    assert_eq!(session.pin_reason(), Some(PinReason::SessionState));
}

#[test]
fn temp_table_and_listen_pin_session() {
    let mut session = VirtualSession::default();

    session.apply_sql(classify("create temporary table t(id int)"));
    assert_eq!(session.pin_reason(), Some(PinReason::TempTable));

    session.apply_sql(classify("discard temp"));
    session.apply_sql(classify("listen account_events"));
    assert_eq!(session.pin_reason(), Some(PinReason::ListenNotify));
}

#[test]
fn discard_all_clears_virtual_state() {
    let mut session = VirtualSession::default();

    session.apply_sql(classify("set application_name = 'api'"));
    session.apply_sql(classify("create temp table t(id int)"));
    session.apply_sql(classify("discard all"));

    assert_eq!(session.pin_reason(), None);
    assert!(session.replay_sql().is_empty());
}
