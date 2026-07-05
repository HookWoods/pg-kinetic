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
pub enum PinReason {
    OpenTransaction,
    FailedTransaction,
    SessionState,
    Copy,
    ListenNotify,
    UnknownProtocolState,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionState {
    pub transaction: TransactionState,
    pin_reason: Option<PinReason>,
}

impl SessionState {
    pub fn apply(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::SimpleQuery(sql) => self.apply_simple_query(&sql),
            ClientEvent::ExtendedQuery => {}
            ClientEvent::Sync => {}
            ClientEvent::Error => {
                if self.transaction == TransactionState::InTransaction {
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

    fn apply_simple_query(&mut self, sql: &str) {
        let normalized = normalize_sql_prefix(sql);
        match normalized.as_str() {
            "begin" | "start transaction" => {
                self.transaction = TransactionState::InTransaction;
                self.pin_reason = Some(PinReason::OpenTransaction);
            }
            "commit" | "rollback" => {
                self.transaction = TransactionState::Idle;
                if matches!(
                    self.pin_reason,
                    Some(PinReason::OpenTransaction | PinReason::FailedTransaction)
                ) {
                    self.pin_reason = None;
                }
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
    }
}

fn normalize_sql_prefix(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
