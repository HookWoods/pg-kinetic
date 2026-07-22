use crate::virtual_session::{PinReason, VirtualSession};
use pg_kinetic_wire::backend::ReadyStatus;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupAction {
    Reuse,
    ResetThenReuse,
    KeepPinned,
    RollbackThenReuse,
    RollbackThenKeepPinned,
    Discard,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PoolMode {
    #[default]
    Transaction,
    Session,
}

impl CleanupAction {
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::Reuse => "reuse",
            Self::ResetThenReuse => "reset_then_reuse",
            Self::KeepPinned => "keep_pinned",
            Self::RollbackThenReuse => "rollback_then_reuse",
            Self::RollbackThenKeepPinned => "rollback_then_keep_pinned",
            Self::Discard => "discard",
        }
    }
}

#[must_use]
pub fn cleanup_action(
    session: &VirtualSession,
    backend_status: ReadyStatus,
    mode: PoolMode,
) -> CleanupAction {
    if session.pin_reason() == Some(PinReason::UnknownProtocolState) {
        return CleanupAction::Discard;
    }

    match backend_status {
        ReadyStatus::FailedTransaction => match mode {
            PoolMode::Session => CleanupAction::RollbackThenKeepPinned,
            PoolMode::Transaction => CleanupAction::RollbackThenReuse,
        },
        ReadyStatus::InTransaction => CleanupAction::KeepPinned,
        ReadyStatus::Idle => match mode {
            PoolMode::Session => CleanupAction::KeepPinned,
            PoolMode::Transaction => {
                if session.pin_reason().is_some() {
                    CleanupAction::KeepPinned
                } else if session.has_replayable_settings() {
                    CleanupAction::ResetThenReuse
                } else {
                    CleanupAction::Reuse
                }
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_session::VirtualSession;

    #[test]
    fn session_mode_keeps_the_backend_pinned_between_queries() {
        let session = VirtualSession::default();

        assert_eq!(
            cleanup_action(&session, ReadyStatus::Idle, PoolMode::Session),
            CleanupAction::KeepPinned
        );
        assert_eq!(
            cleanup_action(&session, ReadyStatus::Idle, PoolMode::Transaction),
            CleanupAction::Reuse
        );
    }

    #[test]
    fn session_mode_discards_unknown_protocol_state() {
        let mut session = VirtualSession::default();
        session.mark_unknown_protocol_state();

        assert_eq!(
            cleanup_action(&session, ReadyStatus::Idle, PoolMode::Session),
            CleanupAction::Discard
        );
    }

    #[test]
    fn session_mode_rolls_back_failed_transactions_without_releasing_backend() {
        let mut session = VirtualSession::default();
        session.mark_failed_transaction();

        assert_eq!(
            cleanup_action(&session, ReadyStatus::FailedTransaction, PoolMode::Session),
            CleanupAction::RollbackThenKeepPinned
        );
        assert_eq!(
            cleanup_action(
                &session,
                ReadyStatus::FailedTransaction,
                PoolMode::Transaction
            ),
            CleanupAction::RollbackThenReuse
        );
    }
}
