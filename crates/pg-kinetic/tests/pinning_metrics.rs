use pg_kinetic::{
    recovery::{RecoveryAction, RecoveryTrigger},
    virtual_session::PinReason,
};

#[test]
fn pin_reason_labels_are_stable() {
    assert_eq!(PinReason::OpenTransaction.metric_label(), "open_transaction");
    assert_eq!(PinReason::SessionState.metric_label(), "session_state");
    assert_eq!(PinReason::TempTable.metric_label(), "temp_table");
    assert_eq!(PinReason::AdvisoryLock.metric_label(), "advisory_lock");
    assert_eq!(PinReason::Copy.metric_label(), "copy");
    assert_eq!(PinReason::ListenNotify.metric_label(), "listen_notify");
    assert_eq!(
        RecoveryTrigger::AbandonedTransaction.metric_label(),
        "abandoned_transaction"
    );
    assert_eq!(RecoveryAction::Rollback.metric_label(), "rollback");
}
