use crate::{
    routing::{BackendRole, QueryClass, RoutingReason},
    sharding::{MultiShardPolicy, ShardId},
    sql_classify::classify_sql,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientEvent {
    SimpleQuery(String),
    ExtendedQuery,
    Sync,
    Error,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TransactionState {
    #[default]
    Idle,
    InTransaction,
    FailedTransaction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionShardDecision {
    Accepted,
    Rejected,
    FollowMultiShardPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedShardSummary {
    CurrentShard,
    Deferred,
    Shard(ShardId),
}

impl PreparedShardSummary {
    #[must_use]
    pub const fn is_current_shard(&self) -> bool {
        matches!(self, Self::CurrentShard)
    }

    #[must_use]
    pub const fn is_deferred(&self) -> bool {
        matches!(self, Self::Deferred)
    }

    #[must_use]
    pub fn shard_id(&self) -> Option<&ShardId> {
        match self {
            Self::Shard(shard_id) => Some(shard_id),
            Self::CurrentShard | Self::Deferred => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionShardState {
    current_shard_id: ShardId,
    current_shard_route_reason: RoutingReason,
    cross_shard_violation: bool,
}

impl TransactionShardState {
    #[must_use]
    pub fn new(current_shard_id: ShardId, current_shard_route_reason: RoutingReason) -> Self {
        Self {
            current_shard_id,
            current_shard_route_reason,
            cross_shard_violation: false,
        }
    }

    #[must_use]
    pub fn current_shard_id(&self) -> &ShardId {
        &self.current_shard_id
    }

    #[must_use]
    pub const fn current_shard_route_reason(&self) -> RoutingReason {
        self.current_shard_route_reason
    }

    #[must_use]
    pub const fn cross_shard_violation(&self) -> bool {
        self.cross_shard_violation
    }

    pub fn set_current_shard_route_reason(&mut self, current_shard_route_reason: RoutingReason) {
        self.current_shard_route_reason = current_shard_route_reason;
    }

    pub fn mark_cross_shard_violation(&mut self) {
        self.cross_shard_violation = true;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRoutingTransactionState {
    access_mode: TransactionAccessMode,
    target_role: BackendRole,
    route_reason: RoutingReason,
    primary_forced: bool,
}

impl ReadRoutingTransactionState {
    #[must_use]
    pub fn new(access_mode: TransactionAccessMode) -> Self {
        let mut state = Self {
            access_mode,
            target_role: BackendRole::Primary,
            route_reason: RoutingReason::TransactionControl,
            primary_forced: false,
        };
        state.refresh();
        state
    }

    #[must_use]
    pub const fn access_mode(self) -> TransactionAccessMode {
        self.access_mode
    }

    #[must_use]
    pub const fn target_role(self) -> BackendRole {
        self.target_role
    }

    #[must_use]
    pub const fn route_reason(self) -> RoutingReason {
        self.route_reason
    }

    #[must_use]
    pub const fn primary_forced(self) -> bool {
        self.primary_forced
    }

    pub fn set_access_mode(&mut self, access_mode: TransactionAccessMode) {
        self.access_mode = access_mode;
        self.refresh();
    }

    pub fn force_primary(&mut self) {
        self.primary_forced = true;
        self.refresh();
    }

    fn refresh(&mut self) {
        if self.primary_forced {
            self.target_role = BackendRole::Primary;
            self.route_reason = RoutingReason::WriteQuery;
            return;
        }

        match self.access_mode {
            TransactionAccessMode::ReadOnly => {
                self.target_role = BackendRole::Replica;
                self.route_reason = RoutingReason::ReadOnlyQuery;
            }
            TransactionAccessMode::ReadWrite => {
                self.target_role = BackendRole::Primary;
                self.route_reason = RoutingReason::TransactionControl;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PinReason {
    OpenTransaction,
    FailedTransaction,
    SessionState,
    Copy,
    ListenNotify,
    ExtendedQueryCycle,
    UnknownProtocolState,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionState {
    pub transaction: TransactionState,
    read_routing_transaction: Option<ReadRoutingTransactionState>,
    transaction_shard: Option<TransactionShardState>,
    pin_reason: Option<PinReason>,
    in_extended_cycle: bool,
}

impl SessionState {
    pub fn apply(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::SimpleQuery(sql) => self.apply_simple_query(&sql),
            ClientEvent::ExtendedQuery => {
                self.in_extended_cycle = true;
                if self.pin_reason.is_none() {
                    self.pin_reason = Some(PinReason::ExtendedQueryCycle);
                }
            }
            ClientEvent::Sync => {
                self.in_extended_cycle = false;
                if self.pin_reason == Some(PinReason::ExtendedQueryCycle) {
                    self.pin_reason = None;
                }
            }
            ClientEvent::Error => {
                if self.transaction == TransactionState::InTransaction {
                    self.mark_transaction_write();
                    self.transaction = TransactionState::FailedTransaction;
                    self.pin_reason = Some(PinReason::FailedTransaction);
                } else {
                    self.pin_reason = Some(PinReason::UnknownProtocolState);
                }
            }
        }
    }

    #[must_use]
    pub const fn pin_reason(&self) -> Option<PinReason> {
        self.pin_reason
    }

    #[must_use]
    pub const fn in_extended_cycle(&self) -> bool {
        self.in_extended_cycle
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
    pub fn transaction_shard_state(&self) -> Option<&TransactionShardState> {
        self.transaction_shard.as_ref()
    }

    #[must_use]
    pub fn current_transaction_shard_id(&self) -> Option<&ShardId> {
        self.transaction_shard
            .as_ref()
            .map(TransactionShardState::current_shard_id)
    }

    #[must_use]
    pub fn current_transaction_shard_route_reason(&self) -> Option<RoutingReason> {
        self.transaction_shard
            .as_ref()
            .map(TransactionShardState::current_shard_route_reason)
    }

    #[must_use]
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

    fn apply_simple_query(&mut self, sql: &str) {
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
            "commit" | "rollback" => {
                self.end_transaction();
            }
            prefix if prefix.starts_with("set transaction read only") => {
                self.set_transaction_access_mode(TransactionAccessMode::ReadOnly)
            }
            prefix if prefix.starts_with("set transaction read write") => {
                self.set_transaction_access_mode(TransactionAccessMode::ReadWrite)
            }
            prefix if prefix.starts_with("set ") => {
                self.pin_reason = Some(PinReason::SessionState);
            }
            prefix if prefix.starts_with("copy ") => {
                self.pin_reason = Some(PinReason::Copy);
            }
            prefix if prefix.starts_with("listen ") || prefix.starts_with("unlisten ") => {
                self.pin_reason = Some(PinReason::ListenNotify);
            }
            _ => {}
        }

        if matches!(classify_sql(sql), QueryClass::Write) {
            self.mark_transaction_write();
        }
    }

    fn begin_transaction(&mut self, access_mode: TransactionAccessMode) {
        self.transaction = TransactionState::InTransaction;
        self.read_routing_transaction = Some(ReadRoutingTransactionState::new(access_mode));
        self.pin_reason = Some(PinReason::OpenTransaction);
    }

    fn end_transaction(&mut self) {
        self.transaction = TransactionState::Idle;
        self.read_routing_transaction = None;
        self.transaction_shard = None;
        if matches!(
            self.pin_reason,
            Some(PinReason::OpenTransaction | PinReason::FailedTransaction)
        ) {
            self.pin_reason = None;
        }
    }

    fn set_transaction_access_mode(&mut self, access_mode: TransactionAccessMode) {
        if self.transaction != TransactionState::InTransaction {
            return;
        }

        self.read_routing_transaction
            .get_or_insert_with(|| ReadRoutingTransactionState::new(access_mode))
            .set_access_mode(access_mode);
    }

    fn mark_transaction_write(&mut self) {
        if self.transaction != TransactionState::InTransaction {
            return;
        }

        self.read_routing_transaction
            .get_or_insert_with(|| {
                ReadRoutingTransactionState::new(TransactionAccessMode::ReadWrite)
            })
            .force_primary();
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
