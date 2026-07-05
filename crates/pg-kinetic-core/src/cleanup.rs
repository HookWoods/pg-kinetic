use crate::virtual_session::{PinReason, VirtualSession};
use pg_kinetic_wire::backend::ReadyStatus;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupAction {
    Reuse,
    ResetThenReuse,
    KeepPinned,
    RollbackThenReuse,
    Discard,
}

impl CleanupAction {
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::Reuse => "reuse",
            Self::ResetThenReuse => "reset_then_reuse",
            Self::KeepPinned => "keep_pinned",
            Self::RollbackThenReuse => "rollback_then_reuse",
            Self::Discard => "discard",
        }
    }
}

#[must_use]
pub fn cleanup_action(session: &VirtualSession, backend_status: ReadyStatus) -> CleanupAction {
    if session.pin_reason() == Some(PinReason::UnknownProtocolState) {
        return CleanupAction::Discard;
    }

    match backend_status {
        ReadyStatus::FailedTransaction => CleanupAction::RollbackThenReuse,
        ReadyStatus::InTransaction => CleanupAction::KeepPinned,
        ReadyStatus::Idle => {
            if session.pin_reason().is_some() {
                CleanupAction::KeepPinned
            } else if session.has_replayable_settings() {
                CleanupAction::ResetThenReuse
            } else {
                CleanupAction::Reuse
            }
        }
    }
}
