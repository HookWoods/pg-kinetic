use std::collections::BTreeMap;

use crate::{
    routing::{BackendRole, RoutingReason},
    session::{ReadRoutingTransactionState, TransactionAccessMode},
    sql::{SetScope, SqlCommand},
    sql_classify::classify_sql,
};

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
    read_routing_transaction: Option<ReadRoutingTransactionState>,
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
            SqlCommand::Begin { access_mode } => self.begin_transaction(access_mode),
            SqlCommand::Commit | SqlCommand::Rollback => self.end_transaction(),
            SqlCommand::SetTransaction { access_mode } => {
                self.set_transaction_access_mode(access_mode)
            }
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

    pub fn apply_transaction_sql(&mut self, sql: &str) {
        let normalized = normalize_sql(sql);
        match normalized.as_str() {
            "begin" | "start transaction" => {
                self.begin_transaction(TransactionAccessMode::ReadWrite)
            }
            "begin read only" | "start transaction read only" => {
                self.begin_transaction(TransactionAccessMode::ReadOnly)
            }
            "begin read write" | "start transaction read write" => {
                self.begin_transaction(TransactionAccessMode::ReadWrite)
            }
            "commit" | "rollback" => self.end_transaction(),
            prefix if prefix.starts_with("set transaction read only") => {
                self.set_transaction_access_mode(TransactionAccessMode::ReadOnly)
            }
            prefix if prefix.starts_with("set transaction read write") => {
                self.set_transaction_access_mode(TransactionAccessMode::ReadWrite)
            }
            _ => {}
        }

        if matches!(classify_sql(sql), crate::routing::QueryClass::Write) {
            self.mark_transaction_write();
        }
    }

    pub fn mark_ready_after_copy(&mut self) {
        self.in_copy = false;
    }

    pub fn mark_failed_transaction(&mut self) {
        self.mark_transaction_write();
        self.transaction = TransactionState::Failed;
    }

    pub fn mark_unknown_protocol_state(&mut self) {
        self.unknown_protocol_state = true;
    }

    pub fn mark_transaction_write(&mut self) {
        if self.transaction != TransactionState::InTransaction {
            return;
        }

        self.read_routing_transaction
            .get_or_insert_with(|| {
                ReadRoutingTransactionState::new(TransactionAccessMode::ReadWrite)
            })
            .force_primary();
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
    pub fn read_routing_transaction_state(&self) -> Option<ReadRoutingTransactionState> {
        self.read_routing_transaction
    }

    #[must_use]
    pub fn transaction_access_mode(&self) -> Option<TransactionAccessMode> {
        self.read_routing_transaction
            .map(ReadRoutingTransactionState::access_mode)
    }

    #[must_use]
    pub fn current_transaction_target_role(&self) -> Option<BackendRole> {
        self.read_routing_transaction
            .map(ReadRoutingTransactionState::target_role)
    }

    #[must_use]
    pub fn current_transaction_route_reason(&self) -> Option<RoutingReason> {
        self.read_routing_transaction
            .map(ReadRoutingTransactionState::route_reason)
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

    fn begin_transaction(&mut self, access_mode: TransactionAccessMode) {
        self.transaction = TransactionState::InTransaction;
        self.read_routing_transaction = Some(ReadRoutingTransactionState::new(access_mode));
    }

    fn end_transaction(&mut self) {
        self.transaction = TransactionState::Idle;
        self.read_routing_transaction = None;
    }

    fn set_transaction_access_mode(&mut self, access_mode: TransactionAccessMode) {
        if self.transaction != TransactionState::InTransaction {
            return;
        }

        self.read_routing_transaction
            .get_or_insert_with(|| ReadRoutingTransactionState::new(access_mode))
            .set_access_mode(access_mode);
    }

    fn clear_all(&mut self) {
        self.transaction = TransactionState::Idle;
        self.read_routing_transaction = None;
        self.settings.clear();
        self.has_unsafe_session_state = false;
        self.has_temp_table = false;
        self.has_advisory_lock = false;
        self.in_copy = false;
        self.has_listen = false;
        self.unknown_protocol_state = false;
    }
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn is_replayable_setting(key: &str) -> bool {
    matches!(
        key,
        "application_name" | "timezone" | "datestyle" | "search_path" | "extra_float_digits"
    )
}
