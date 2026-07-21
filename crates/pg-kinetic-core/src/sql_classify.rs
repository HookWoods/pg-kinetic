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
    contains_data_modifying_cte_normalized(&normalized)
}

#[must_use]
pub(crate) fn contains_data_modifying_cte_normalized(normalized: &str) -> bool {
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

    if sql.is_empty() {
        return QueryClass::Unknown;
    }

    if keyword_is(sql, "explain") {
        return classify_explain(sql);
    }

    if keyword_is(sql, "with") {
        let normalized = normalize(sql);
        return if contains_data_modifying_cte_normalized(&normalized) {
            QueryClass::Write
        } else {
            QueryClass::Unknown
        };
    }

    if is_transaction_control(sql) {
        return QueryClass::TransactionControl;
    }

    if is_session_mutation(sql) {
        return QueryClass::SessionMutation;
    }

    if keyword_is(sql, "copy") {
        return classify_copy(sql);
    }

    if keyword_is(sql, "select") {
        let normalized = normalize(sql);
        if contains_data_modifying_cte_normalized(&normalized)
            || contains_select_side_effects(&normalized)
        {
            return QueryClass::Write;
        }

        return QueryClass::ReadCandidate;
    }

    if keyword_is(sql, "values") || keyword_is(sql, "table") {
        return QueryClass::ReadCandidate;
    }

    if keyword_is(sql, "show") {
        return QueryClass::ReadCandidate;
    }

    if is_write_statement(sql) {
        return QueryClass::Write;
    }

    QueryClass::Unknown
}

fn classify_explain(sql: &str) -> QueryClass {
    if !keyword_is(sql, "explain") {
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

fn classify_copy(sql: &str) -> QueryClass {
    if contains_words_ignore_ascii_case(sql, &["from", "stdin"]) {
        QueryClass::Write
    } else if contains_words_ignore_ascii_case(sql, &["to", "stdout"]) {
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

fn is_transaction_control(sql: &str) -> bool {
    starts_with_words_ignore_ascii_case(sql, &["begin"])
        || keyword_is_exact(sql, "commit")
        || keyword_is_exact(sql, "rollback")
        || keyword_is_exact(sql, "abort")
        || keyword_is_exact(sql, "end")
        || starts_with_words_ignore_ascii_case(sql, &["start", "transaction"])
        || starts_with_words_ignore_ascii_case(sql, &["set", "transaction"])
        || keyword_is_followed_by_more(sql, "savepoint")
        || starts_with_words_ignore_ascii_case(sql, &["release", "savepoint"])
        || starts_with_words_ignore_ascii_case(sql, &["rollback", "to"])
}

fn is_session_mutation(sql: &str) -> bool {
    keyword_is_followed_by_more(sql, "set")
        || keyword_is_followed_by_more(sql, "reset")
        || keyword_is_followed_by_more(sql, "discard")
        || keyword_is_followed_by_more(sql, "listen")
        || keyword_is_followed_by_more(sql, "unlisten")
        || keyword_is_followed_by_more(sql, "notify")
        || keyword_is_followed_by_more(sql, "lock")
        || keyword_is_followed_by_more(sql, "declare")
}

fn is_write_statement(sql: &str) -> bool {
    keyword_is(sql, "insert")
        || keyword_is(sql, "update")
        || keyword_is(sql, "delete")
        || keyword_is(sql, "merge")
        || keyword_is(sql, "truncate")
        || keyword_is(sql, "create")
        || keyword_is(sql, "alter")
        || keyword_is(sql, "drop")
        || keyword_is(sql, "call")
        || keyword_is(sql, "do")
        || keyword_is(sql, "vacuum")
        || keyword_is(sql, "analyze")
        || keyword_is(sql, "reindex")
        || keyword_is(sql, "grant")
        || keyword_is(sql, "revoke")
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

fn keyword_is(sql: &str, keyword: &str) -> bool {
    starts_with_words_ignore_ascii_case(sql, &[keyword])
}

fn starts_with_words_ignore_ascii_case(sql: &str, words: &[&str]) -> bool {
    let mut rest = sql;
    for (index, word) in words.iter().enumerate() {
        rest = rest.trim_start();
        let Some(candidate) = rest.get(..word.len()) else {
            return false;
        };
        if !candidate.eq_ignore_ascii_case(word) {
            return false;
        }
        rest = &rest[word.len()..];
        if index + 1 == words.len() {
            return rest.is_empty() || rest.chars().next().is_some_and(|c| c.is_whitespace());
        }
    }

    true
}

fn keyword_is_exact(sql: &str, keyword: &str) -> bool {
    let trimmed = sql.trim_end();
    let Some(candidate) = trimmed.get(..keyword.len()) else {
        return false;
    };
    candidate.eq_ignore_ascii_case(keyword) && trimmed[keyword.len()..].is_empty()
}

fn keyword_is_followed_by_more(sql: &str, keyword: &str) -> bool {
    let rest = sql.trim_start();
    let Some(candidate) = rest.get(..keyword.len()) else {
        return false;
    };
    if !candidate.eq_ignore_ascii_case(keyword) {
        return false;
    }

    let rest = &rest[keyword.len()..];
    rest.chars().next().is_some_and(|c| c.is_whitespace()) && !rest.trim_start().is_empty()
}

fn contains_words_ignore_ascii_case(sql: &str, words: &[&str]) -> bool {
    if words.is_empty() {
        return true;
    }

    let mut index = 0;
    while index < sql.len() {
        let remainder = &sql[index..];
        let trimmed = remainder.trim_start();
        index += remainder.len() - trimmed.len();

        if index >= sql.len() {
            return false;
        }

        if starts_with_words_ignore_ascii_case(&sql[index..], words) {
            return true;
        }

        let token_end = sql[index..]
            .find(char::is_whitespace)
            .map(|offset| index + offset)
            .unwrap_or(sql.len());
        index = token_end;
    }

    false
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanState {
    Normal,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_matches_for_mixed_case_and_whitespace() {
        for (sql, expected) in [
            ("  SELECT 1", QueryClass::ReadCandidate),
            ("\n\tselect * from t", QueryClass::ReadCandidate),
            (
                "WITH x AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM x",
                QueryClass::Write,
            ),
            (
                "WiTh  x  aS  (uPdAtE t SET a=1) select 1",
                QueryClass::Write,
            ),
            ("EXPLAIN ANALYZE SELECT 1", QueryClass::Unknown),
            ("BEGIN", QueryClass::TransactionControl),
        ] {
            assert_eq!(classify_sql(sql), expected, "sql: {sql}");
        }
    }

    #[test]
    fn normalize_is_not_called_twice_for_selects() {
        assert!(contains_data_modifying_cte_normalized(
            "with x as (insert into t values (1)) select 1"
        ));
        assert!(!contains_data_modifying_cte_normalized(
            "with x as (select 1) select * from x"
        ));
    }
}
