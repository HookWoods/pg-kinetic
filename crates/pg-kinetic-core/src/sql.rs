use crate::session::TransactionAccessMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlCommand {
    Begin {
        access_mode: TransactionAccessMode,
    },
    Commit,
    Rollback,
    SetTransaction {
        access_mode: TransactionAccessMode,
    },
    Set {
        scope: SetScope,
        key: String,
        value: String,
    },
    Reset {
        key: String,
    },
    DiscardAll,
    DiscardTemp,
    DiscardPlans,
    CreateTemp,
    AdvisoryLock,
    AdvisoryUnlock,
    Copy,
    Listen,
    Unlisten,
    Query,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetScope {
    Session,
    Local,
}

#[must_use]
pub fn classify(sql: &str) -> SqlCommand {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let normalized = normalize(trimmed);

    if let Some(access_mode) = parse_begin_transaction(&normalized) {
        return SqlCommand::Begin { access_mode };
    }
    if let Some(access_mode) = parse_set_transaction(&normalized) {
        return SqlCommand::SetTransaction { access_mode };
    }

    match normalized.as_str() {
        "commit" | "end" => return SqlCommand::Commit,
        "rollback" => return SqlCommand::Rollback,
        "discard all" => return SqlCommand::DiscardAll,
        "discard temp" | "discard temporary" => return SqlCommand::DiscardTemp,
        "discard plans" => return SqlCommand::DiscardPlans,
        _ => {}
    }

    if normalized.starts_with("set local ") {
        return parse_set(trimmed, SetScope::Local);
    }
    if normalized.starts_with("set ") {
        return parse_set(trimmed, SetScope::Session);
    }
    if normalized.starts_with("reset ") {
        return SqlCommand::Reset {
            key: normalized.trim_start_matches("reset ").trim().to_string(),
        };
    }
    if normalized.starts_with("create temp ") || normalized.starts_with("create temporary ") {
        return SqlCommand::CreateTemp;
    }
    if normalized.contains("pg_advisory_lock(") {
        return SqlCommand::AdvisoryLock;
    }
    if normalized.contains("pg_advisory_unlock") {
        return SqlCommand::AdvisoryUnlock;
    }
    if normalized.starts_with("copy ") {
        return SqlCommand::Copy;
    }
    if normalized.starts_with("listen ") {
        return SqlCommand::Listen;
    }
    if normalized.starts_with("unlisten ") {
        return SqlCommand::Unlisten;
    }

    SqlCommand::Query
}

fn parse_set(sql: &str, scope: SetScope) -> SqlCommand {
    let prefix_len = match scope {
        SetScope::Session => 4,
        SetScope::Local => 10,
    };
    let rest = sql[prefix_len..].trim();
    let Some((key, value)) = rest.split_once('=') else {
        let mut parts = rest.split_whitespace();
        let key = parts.next().unwrap_or_default().to_ascii_lowercase();
        let value = parts.collect::<Vec<_>>().join(" ");
        return SqlCommand::Set { scope, key, value };
    };

    SqlCommand::Set {
        scope,
        key: key.trim().to_ascii_lowercase(),
        value: value.trim().to_string(),
    }
}

fn parse_begin_transaction(normalized: &str) -> Option<TransactionAccessMode> {
    match normalized {
        "begin" | "start transaction" => Some(TransactionAccessMode::ReadWrite),
        "begin read only" | "start transaction read only" => Some(TransactionAccessMode::ReadOnly),
        "begin read write" | "start transaction read write" => {
            Some(TransactionAccessMode::ReadWrite)
        }
        _ => None,
    }
}

fn parse_set_transaction(normalized: &str) -> Option<TransactionAccessMode> {
    match normalized {
        "set transaction read only" => Some(TransactionAccessMode::ReadOnly),
        "set transaction read write" => Some(TransactionAccessMode::ReadWrite),
        _ => None,
    }
}

fn normalize(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
