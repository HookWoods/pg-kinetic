use pg_kinetic::recovery::{recovery_action, RecoveryAction, RecoveryMode, RecoveryTrigger};

#[test]
fn failed_transaction_rolls_back_in_recover_mode() {
    assert_eq!(
        recovery_action(RecoveryTrigger::FailedTransaction, RecoveryMode::Recover),
        RecoveryAction::Rollback
    );
}

#[test]
fn abandoned_query_drains_and_resyncs_in_recover_mode() {
    assert_eq!(
        recovery_action(RecoveryTrigger::AbandonedResponse, RecoveryMode::Recover),
        RecoveryAction::DrainAndSync
    );
}

#[test]
fn abandoned_response_is_dropped_in_rollback_only_mode() {
    assert_eq!(
        recovery_action(
            RecoveryTrigger::AbandonedResponse,
            RecoveryMode::RollbackOnly
        ),
        RecoveryAction::Discard
    );
}

#[test]
fn unknown_protocol_state_is_never_recovered() {
    assert_eq!(
        recovery_action(RecoveryTrigger::UnknownProtocolState, RecoveryMode::Recover),
        RecoveryAction::Discard
    );
}

#[test]
fn recovery_metric_labels_are_stable() {
    assert_eq!(
        RecoveryTrigger::AbandonedResponse.metric_label(),
        "abandoned_response"
    );
    assert_eq!(
        RecoveryAction::DrainAndSync.metric_label(),
        "drain_and_sync"
    );
}
