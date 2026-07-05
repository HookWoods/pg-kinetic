use pg_kinetic::{
    cleanup::{cleanup_action, CleanupAction},
    sql::classify,
    virtual_session::VirtualSession,
    wire::backend::ReadyStatus,
};

#[test]
fn idle_unpinned_backend_can_be_reused() {
    let session = VirtualSession::default();

    assert_eq!(
        cleanup_action(&session, ReadyStatus::Idle),
        CleanupAction::Reuse
    );
}

#[test]
fn open_transaction_backend_stays_pinned() {
    let mut session = VirtualSession::default();
    session.apply_sql(classify("begin"));

    assert_eq!(
        cleanup_action(&session, ReadyStatus::InTransaction),
        CleanupAction::KeepPinned
    );
}

#[test]
fn failed_transaction_requires_rollback() {
    let mut session = VirtualSession::default();
    session.apply_sql(classify("begin"));
    session.mark_failed_transaction();

    assert_eq!(
        cleanup_action(&session, ReadyStatus::FailedTransaction),
        CleanupAction::RollbackThenReuse
    );
}

#[test]
fn replayable_settings_require_backend_reset_before_reuse() {
    let mut session = VirtualSession::default();
    session.apply_sql(classify("set application_name = 'api'"));

    assert_eq!(
        cleanup_action(&session, ReadyStatus::Idle),
        CleanupAction::ResetThenReuse
    );
}

#[test]
fn unknown_protocol_state_discards_backend() {
    let mut session = VirtualSession::default();
    session.mark_unknown_protocol_state();

    assert_eq!(
        cleanup_action(&session, ReadyStatus::Idle),
        CleanupAction::Discard
    );
}
