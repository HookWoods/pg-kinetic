use std::collections::BTreeMap;

use crate::{
    lsn::PgLsn,
    routing::{BackendRole, RoutingReason},
    session::{
        ReadRoutingTransactionState, TransactionAccessMode, TransactionShardDecision,
        TransactionShardState,
    },
    sharding::{MultiShardPolicy, ShardId},
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadAfterWriteState {
    #[default]
    Disabled,
    Required(PgLsn),
    Unknown,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VirtualSession {
    transaction: TransactionState,
    read_routing_transaction: Option<ReadRoutingTransactionState>,
    transaction_shard: Option<TransactionShardState>,
    read_after_write: ReadAfterWriteState,
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

    pub fn transaction_shard_state(&self) -> Option<&TransactionShardState> {
        self.transaction_shard.as_ref()
    }

    pub fn current_transaction_shard_id(&self) -> Option<&ShardId> {
        self.transaction_shard
            .as_ref()
            .map(TransactionShardState::current_shard_id)
    }

    pub fn current_transaction_shard_route_reason(&self) -> Option<RoutingReason> {
        self.transaction_shard
            .as_ref()
            .map(TransactionShardState::current_shard_route_reason)
    }

    pub fn transaction_cross_shard_violation(&self) -> bool {
        self.transaction_shard
            .as_ref()
            .is_some_and(TransactionShardState::cross_shard_violation)
    }

    pub fn set_transaction_shard_affinity(
        &mut self,
        current_shard_id: ShardId,
        current_shard_route_reason: RoutingReason,
    ) {
        self.transaction_shard = Some(TransactionShardState::new(
            current_shard_id,
            current_shard_route_reason,
        ));
    }

    pub fn mark_transaction_cross_shard_violation(&mut self) {
        if let Some(transaction_shard) = self.transaction_shard.as_mut() {
            transaction_shard.mark_cross_shard_violation();
        }
    }

    pub fn clear_transaction_shard_affinity(&mut self) {
        self.transaction_shard = None;
    }

    pub fn apply_transaction_shard_affinity(
        &mut self,
        current_shard_id: Option<ShardId>,
        current_shard_route_reason: RoutingReason,
        multi_shard_policy: MultiShardPolicy,
    ) -> TransactionShardDecision {
        if self.transaction != TransactionState::InTransaction {
            return TransactionShardDecision::Accepted;
        }

        let Some(current_shard_id) = current_shard_id else {
            let _ = multi_shard_policy;
            return TransactionShardDecision::FollowMultiShardPolicy;
        };

        match self.transaction_shard.as_mut() {
            None => {
                self.transaction_shard = Some(TransactionShardState::new(
                    current_shard_id,
                    current_shard_route_reason,
                ));
                TransactionShardDecision::Accepted
            }
            Some(transaction_shard)
                if transaction_shard.current_shard_id() == &current_shard_id =>
            {
                if transaction_shard.current_shard_route_reason() == RoutingReason::UnknownQuery
                    && current_shard_route_reason != RoutingReason::UnknownQuery
                {
                    transaction_shard.set_current_shard_route_reason(current_shard_route_reason);
                }
                TransactionShardDecision::Accepted
            }
            Some(transaction_shard) => {
                transaction_shard.mark_cross_shard_violation();
                let _ = multi_shard_policy;
                TransactionShardDecision::Rejected
            }
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

    #[must_use]
    pub const fn read_after_write_state(&self) -> ReadAfterWriteState {
        self.read_after_write
    }

    pub fn set_read_after_write_required(&mut self, lsn: PgLsn) {
        self.read_after_write = ReadAfterWriteState::Required(lsn);
    }

    pub fn set_read_after_write_unknown(&mut self) {
        self.read_after_write = ReadAfterWriteState::Unknown;
    }

    pub fn clear_read_after_write(&mut self) {
        self.read_after_write = ReadAfterWriteState::Disabled;
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
        self.transaction_shard = None;
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
        self.transaction_shard = None;
        self.read_after_write = ReadAfterWriteState::Disabled;
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
