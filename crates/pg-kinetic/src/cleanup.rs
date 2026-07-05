use crate::{
    virtual_session::{PinReason, VirtualSession},
    wire::backend::ReadyStatus,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupAction {
    Reuse,
    ResetThenReuse,
    KeepPinned,
    RollbackThenReuse,
    Discard,
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
