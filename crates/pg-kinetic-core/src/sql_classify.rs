use crate::routing::{QueryClass, RoutingHint};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlAnalysis {
    query_class: QueryClass,
    routing_hint: RoutingHint,
}

impl SqlAnalysis {
    #[must_use]
    pub const fn query_class(self) -> QueryClass {
        self.query_class
    }

    #[must_use]
    pub const fn routing_hint(self) -> RoutingHint {
        self.routing_hint
    }
}

#[must_use]
pub fn analyze_sql(sql: &str) -> SqlAnalysis {
    SqlAnalysis {
        query_class: classify_sql(sql),
        routing_hint: extract_routing_hint(sql),
    }
}

#[must_use]
pub fn classify_sql(sql: &str) -> QueryClass {
    let sql = strip_leading_comments_and_whitespace(sql);
    if sql.is_empty() {
        return QueryClass::Unknown;
    }

    let statements = split_top_level_statements(sql);
    if statements.len() > 1 {
        return classify_multi_statement(&statements);
    }

    classify_single_statement(sql)
}

#[must_use]
pub fn extract_routing_hint(sql: &str) -> RoutingHint {
    let mut rest = sql.trim_start();

    loop {
        if let Some(after_comment) = rest.strip_prefix("/*") {
            let Some(end) = after_comment.find("*/") else {
                return RoutingHint::None;
            };
            let comment = after_comment[..end].trim();
            let hint = match comment {
                "pg-kinetic: primary" => RoutingHint::Primary,
                "pg-kinetic: replica" => RoutingHint::Replica,
                "pg-kinetic: stale-ok" => RoutingHint::StaleOk,
                "pg-kinetic: strict-fresh" => RoutingHint::StrictFresh,
                _ => RoutingHint::None,
            };

            if hint != RoutingHint::None {
                return hint;
            }

            rest = &after_comment[end + 2..];
            rest = rest.trim_start();
            continue;
        }

        if let Some(after_comment) = rest.strip_prefix("--") {
            if let Some(end) = after_comment.find('\n') {
                rest = &after_comment[end + 1..];
                rest = rest.trim_start();
                continue;
            }

            return RoutingHint::None;
        }

        let trimmed = rest.trim_start();
        if trimmed == rest {
            break;
        }
        rest = trimmed;
    }

    RoutingHint::None
}

#[must_use]
pub fn strip_leading_comments_and_whitespace(sql: &str) -> &str {
    let mut rest = sql;

    loop {
        let trimmed = rest.trim_start();
        if trimmed != rest {
            rest = trimmed;
            continue;
        }

        if let Some(after_comment) = rest.strip_prefix("--") {
            if let Some(end) = after_comment.find('\n') {
                rest = &after_comment[end + 1..];
                continue;
            }

            return "";
        }

        if let Some(after_comment) = rest.strip_prefix("/*") {
            let Some(end) = after_comment.find("*/") else {
                return "";
            };
            rest = &after_comment[end + 2..];
            continue;
        }

        break;
    }

    rest
}

#[must_use]
pub fn has_multiple_statements(sql: &str) -> bool {
    split_top_level_statements(sql).len() > 1
}

#[must_use]
pub fn contains_data_modifying_cte(sql: &str) -> bool {
    let normalized = normalize(strip_leading_comments_and_whitespace(sql));
    if !normalized.starts_with("with ") && normalized != "with" {
        return false;
    }

    normalized.contains(" as (insert")
        || normalized.contains(" as (update")
        || normalized.contains(" as (delete")
        || normalized.contains(" as (merge")
}

fn classify_multi_statement(statements: &[&str]) -> QueryClass {
    let mut saw_read_candidate = false;

    for statement in statements {
        match classify_single_statement(statement) {
            QueryClass::ReadCandidate | QueryClass::ReadOnly => saw_read_candidate = true,
            QueryClass::TransactionControl => {}
            QueryClass::Unknown => return QueryClass::Unknown,
            other => return other,
        }
    }

    if saw_read_candidate {
        QueryClass::ReadCandidate
    } else {
        QueryClass::TransactionControl
    }
}

fn classify_single_statement(sql: &str) -> QueryClass {
    let sql = strip_leading_comments_and_whitespace(sql);
    let normalized = normalize(sql);

    if normalized.is_empty() {
        return QueryClass::Unknown;
    }

    if normalized.starts_with("explain") {
        return classify_explain(sql);
    }

    if normalized.starts_with("with ") || normalized == "with" {
        return if contains_data_modifying_cte(sql) {
            QueryClass::Write
        } else {
            QueryClass::Unknown
        };
    }

    if is_transaction_control(&normalized) {
        return QueryClass::TransactionControl;
    }

    if is_session_mutation(&normalized) {
        return QueryClass::SessionMutation;
    }

    if normalized.starts_with("copy ") {
        return classify_copy(&normalized);
    }

    if normalized.starts_with("select") {
        if contains_data_modifying_cte(sql) || contains_select_side_effects(&normalized) {
            return QueryClass::Write;
        }

        return QueryClass::ReadCandidate;
    }

    if normalized.starts_with("values") || normalized.starts_with("table ") || normalized == "table"
    {
        return QueryClass::ReadCandidate;
    }

    if normalized.starts_with("show ") || normalized == "show" {
        return QueryClass::ReadCandidate;
    }

    if is_write_statement(&normalized) {
        return QueryClass::Write;
    }

    QueryClass::Unknown
}

fn classify_explain(sql: &str) -> QueryClass {
    let sql = strip_leading_comments_and_whitespace(sql);
    let normalized = normalize(sql);

    if !normalized.starts_with("explain") {
        return QueryClass::Unknown;
    }

    let mut rest = sql["explain".len()..].trim_start();
    if rest.starts_with('(') {
        let Some(end) = find_matching_paren(rest) else {
            return QueryClass::Unknown;
        };

        let options = normalize(&rest[1..end]);
        if options.contains("analyze") {
            return QueryClass::Unknown;
        }

        rest = rest[end + 1..].trim_start();
    } else {
        let rest_normalized = normalize(rest);
        if rest_normalized.starts_with("analyze") {
            return QueryClass::Unknown;
        }
    }

    let target = classify_single_statement(rest);
    if target.routes_to_replica() {
        QueryClass::ReadCandidate
    } else {
        QueryClass::Unknown
    }
}

fn classify_copy(normalized: &str) -> QueryClass {
    if normalized.contains(" from stdin") {
        QueryClass::Write
    } else if normalized.contains(" to stdout") {
        QueryClass::ReadCandidate
    } else {
        QueryClass::Write
    }
}

fn contains_select_side_effects(normalized: &str) -> bool {
    normalized.contains(" into ")
        || normalized.contains(" for update")
        || normalized.contains(" for no key update")
        || normalized.contains(" for share")
        || normalized.contains(" for key share")
        || normalized.contains(" lock in share mode")
        || normalized.contains("pg_advisory_lock(")
        || normalized.contains("pg_try_advisory_lock(")
        || normalized.contains("set_config(")
}

fn is_transaction_control(normalized: &str) -> bool {
    normalized == "begin"
        || normalized.starts_with("begin ")
        || normalized == "commit"
        || normalized == "rollback"
        || normalized == "abort"
        || normalized == "end"
        || normalized.starts_with("start transaction")
        || normalized.starts_with("set transaction ")
        || normalized.starts_with("savepoint ")
        || normalized.starts_with("release savepoint")
        || normalized.starts_with("rollback to")
}

fn is_session_mutation(normalized: &str) -> bool {
    normalized.starts_with("set ")
        || normalized.starts_with("reset ")
        || normalized.starts_with("discard ")
        || normalized.starts_with("listen ")
        || normalized.starts_with("unlisten ")
        || normalized.starts_with("notify ")
        || normalized.starts_with("lock ")
        || normalized.starts_with("declare ")
}

fn is_write_statement(normalized: &str) -> bool {
    normalized.starts_with("insert ")
        || normalized == "insert"
        || normalized.starts_with("update ")
        || normalized == "update"
        || normalized.starts_with("delete ")
        || normalized == "delete"
        || normalized.starts_with("merge ")
        || normalized == "merge"
        || normalized.starts_with("truncate ")
        || normalized == "truncate"
        || normalized.starts_with("create ")
        || normalized == "create"
        || normalized.starts_with("alter ")
        || normalized == "alter"
        || normalized.starts_with("drop ")
        || normalized == "drop"
        || normalized.starts_with("call ")
        || normalized == "call"
        || normalized.starts_with("do ")
        || normalized == "do"
        || normalized.starts_with("vacuum ")
        || normalized == "vacuum"
        || normalized.starts_with("analyze ")
        || normalized == "analyze"
        || normalized.starts_with("reindex ")
        || normalized == "reindex"
        || normalized.starts_with("grant ")
        || normalized == "grant"
        || normalized.starts_with("revoke ")
        || normalized == "revoke"
}

fn split_top_level_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    for_each_top_level_statement(sql, |statement| statements.push(statement));
    statements
}

pub fn for_each_top_level_statement<'a>(sql: &'a str, mut visit: impl FnMut(&'a str)) {
    let mut start = 0;
    let mut state = ScanState::Normal;
    let mut iter = sql.char_indices().peekable();

    while let Some((index, ch)) = iter.next() {
        match state {
            ScanState::Normal => match ch {
                '\'' => state = ScanState::SingleQuote,
                '"' => state = ScanState::DoubleQuote,
                '-' if matches!(iter.peek(), Some((_, '-'))) => {
                    iter.next();
                    state = ScanState::LineComment;
                }
                '/' if matches!(iter.peek(), Some((_, '*'))) => {
                    iter.next();
                    state = ScanState::BlockComment;
                }
                ';' => {
                    let statement = sql[start..index].trim();
                    if !statement.is_empty() {
                        visit(statement);
                    }
                    start = index + ch.len_utf8();
                }
                _ => {}
            },
            ScanState::SingleQuote => {
                if ch == '\'' {
                    if matches!(iter.peek(), Some((_, '\''))) {
                        iter.next();
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }
            ScanState::DoubleQuote => {
                if ch == '"' {
                    if matches!(iter.peek(), Some((_, '"'))) {
                        iter.next();
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }
            ScanState::LineComment => {
                if ch == '\n' {
                    state = ScanState::Normal;
                }
            }
            ScanState::BlockComment => {
                if ch == '*' && matches!(iter.peek(), Some((_, '/'))) {
                    iter.next();
                    state = ScanState::Normal;
                }
            }
        }
    }

    let statement = sql[start..].trim();
    if !statement.is_empty() {
        visit(statement);
    }
}

fn find_matching_paren(sql: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut state = ScanState::Normal;
    let mut iter = sql.char_indices().peekable();

    while let Some((index, ch)) = iter.next() {
        match state {
            ScanState::Normal => match ch {
                '\'' => state = ScanState::SingleQuote,
                '"' => state = ScanState::DoubleQuote,
                '-' if matches!(iter.peek(), Some((_, '-'))) => {
                    iter.next();
                    state = ScanState::LineComment;
                }
                '/' if matches!(iter.peek(), Some((_, '*'))) => {
                    iter.next();
                    state = ScanState::BlockComment;
                }
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            },
            ScanState::SingleQuote => {
                if ch == '\'' {
                    if matches!(iter.peek(), Some((_, '\''))) {
                        iter.next();
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }
            ScanState::DoubleQuote => {
                if ch == '"' {
                    if matches!(iter.peek(), Some((_, '"'))) {
                        iter.next();
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }
            ScanState::LineComment => {
                if ch == '\n' {
                    state = ScanState::Normal;
                }
            }
            ScanState::BlockComment => {
                if ch == '*' && matches!(iter.peek(), Some((_, '/'))) {
                    iter.next();
                    state = ScanState::Normal;
                }
            }
        }
    }

    None
}

fn normalize(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanState {
    Normal,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
}
