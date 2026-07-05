use std::collections::BTreeMap;

use crate::sql::{SetScope, SqlCommand};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PinReason {
    OpenTransaction,
    FailedTransaction,
    SessionState,
    TempTable,
    AdvisoryLock,
    Copy,
    ListenNotify,
    UnknownProtocolState,
}

impl PinReason {
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::OpenTransaction => "open_transaction",
            Self::FailedTransaction => "failed_transaction",
            Self::SessionState => "session_state",
            Self::TempTable => "temp_table",
            Self::AdvisoryLock => "advisory_lock",
            Self::Copy => "copy",
            Self::ListenNotify => "listen_notify",
            Self::UnknownProtocolState => "unknown_protocol_state",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TransactionState {
    #[default]
    Idle,
    InTransaction,
    Failed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VirtualSession {
    transaction: TransactionState,
    settings: BTreeMap<String, String>,
    has_unsafe_session_state: bool,
    has_temp_table: bool,
    has_advisory_lock: bool,
    in_copy: bool,
    has_listen: bool,
    unknown_protocol_state: bool,
}

impl VirtualSession {
    pub fn apply_sql(&mut self, command: SqlCommand) {
        match command {
            SqlCommand::Begin => self.transaction = TransactionState::InTransaction,
            SqlCommand::Commit | SqlCommand::Rollback => self.transaction = TransactionState::Idle,
            SqlCommand::Set { scope, key, value } => self.apply_set(scope, key, value),
            SqlCommand::Reset { key } => {
                self.settings.remove(&key);
                if !is_replayable_setting(&key) {
                    self.has_unsafe_session_state = true;
                }
            }
            SqlCommand::DiscardAll => self.clear_all(),
            SqlCommand::DiscardTemp => self.has_temp_table = false,
            SqlCommand::DiscardPlans => {}
            SqlCommand::CreateTemp => self.has_temp_table = true,
            SqlCommand::AdvisoryLock => self.has_advisory_lock = true,
            SqlCommand::AdvisoryUnlock => self.has_advisory_lock = false,
            SqlCommand::Copy => self.in_copy = true,
            SqlCommand::Listen => self.has_listen = true,
            SqlCommand::Unlisten => self.has_listen = false,
            SqlCommand::Query => {}
        }
    }

    pub fn mark_ready_after_copy(&mut self) {
        self.in_copy = false;
    }

    pub fn mark_failed_transaction(&mut self) {
        self.transaction = TransactionState::Failed;
    }

    pub fn mark_unknown_protocol_state(&mut self) {
        self.unknown_protocol_state = true;
    }

    #[must_use]
    pub fn pin_reason(&self) -> Option<PinReason> {
        if self.unknown_protocol_state {
            return Some(PinReason::UnknownProtocolState);
        }
        if self.in_copy {
            return Some(PinReason::Copy);
        }
        if self.transaction == TransactionState::Failed {
            return Some(PinReason::FailedTransaction);
        }
        if self.transaction == TransactionState::InTransaction {
            return Some(PinReason::OpenTransaction);
        }
        if self.has_temp_table {
            return Some(PinReason::TempTable);
        }
        if self.has_advisory_lock {
            return Some(PinReason::AdvisoryLock);
        }
        if self.has_listen {
            return Some(PinReason::ListenNotify);
        }
        if self.has_unsafe_session_state {
            return Some(PinReason::SessionState);
        }
        None
    }

    #[must_use]
    pub fn replay_sql(&self) -> Vec<String> {
        self.settings
            .iter()
            .map(|(key, value)| format!("SET {key} = {value}"))
            .collect()
    }

    #[must_use]
    pub fn has_replayable_settings(&self) -> bool {
        !self.settings.is_empty()
    }

    fn apply_set(&mut self, scope: SetScope, key: String, value: String) {
        if scope == SetScope::Local {
            return;
        }

        if is_replayable_setting(&key) {
            self.settings.insert(key, value);
        } else {
            self.has_unsafe_session_state = true;
        }
    }

    fn clear_all(&mut self) {
        self.transaction = TransactionState::Idle;
        self.settings.clear();
        self.has_unsafe_session_state = false;
        self.has_temp_table = false;
        self.has_advisory_lock = false;
        self.in_copy = false;
        self.has_listen = false;
        self.unknown_protocol_state = false;
    }
}

fn is_replayable_setting(key: &str) -> bool {
    matches!(
        key,
        "application_name" | "timezone" | "datestyle" | "search_path" | "extra_float_digits"
    )
}
