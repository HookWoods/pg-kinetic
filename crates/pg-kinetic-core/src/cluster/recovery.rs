use clap::ValueEnum;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum RecoveryMode {
    Recover,
    RollbackOnly,
    Drop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryTrigger {
    FailedTransaction,
    AbandonedTransaction,
    AbandonedResponse,
    UnknownProtocolState,
}

impl RecoveryTrigger {
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::FailedTransaction => "failed_transaction",
            Self::AbandonedTransaction => "abandoned_transaction",
            Self::AbandonedResponse => "abandoned_response",
            Self::UnknownProtocolState => "unknown_protocol_state",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryAction {
    None,
    Rollback,
    DrainAndSync,
    RollbackAndDrain,
    Discard,
}

impl RecoveryAction {
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Rollback => "rollback",
            Self::DrainAndSync => "drain_and_sync",
            Self::RollbackAndDrain => "rollback_and_drain",
            Self::Discard => "discard",
        }
    }
}

#[must_use]
pub const fn recovery_action(trigger: RecoveryTrigger, mode: RecoveryMode) -> RecoveryAction {
    match mode {
        RecoveryMode::Drop => RecoveryAction::Discard,
        RecoveryMode::RollbackOnly => match trigger {
            RecoveryTrigger::FailedTransaction | RecoveryTrigger::AbandonedTransaction => {
                RecoveryAction::Rollback
            }
            RecoveryTrigger::AbandonedResponse | RecoveryTrigger::UnknownProtocolState => {
                RecoveryAction::Discard
            }
        },
        RecoveryMode::Recover => match trigger {
            RecoveryTrigger::FailedTransaction | RecoveryTrigger::AbandonedTransaction => {
                RecoveryAction::Rollback
            }
            RecoveryTrigger::AbandonedResponse => RecoveryAction::DrainAndSync,
            RecoveryTrigger::UnknownProtocolState => RecoveryAction::Discard,
        },
    }
}
