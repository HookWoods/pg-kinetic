use std::sync::Arc;

use crate::sharding::{RouteMapValidationInput, ShardedTableDefinition, ShardKey};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardExtraction {
    Unknown,
    Key {
        schema: Option<Arc<str>>,
        table: Arc<str>,
        column: Arc<str>,
        key: ShardKey,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardHint {
    None,
    Shard(Arc<str>),
    Tenant(Arc<str>),
    Route(Arc<str>),
    Unknown,
}

#[must_use]
pub fn extract_shard_key(sql: &str, route_map: &RouteMapValidationInput) -> ShardExtraction {
    let Some(tokens) = tokenize(sql) else {
        return ShardExtraction::Unknown;
    };

    let Some(statement) = parse_statement(&tokens, route_map) else {
        return ShardExtraction::Unknown;
    };

    statement
}

#[must_use]
pub fn extract_shard_hint(sql: &str) -> ShardHint {
    let mut rest = sql.trim_start();
    let mut saw_comment = false;

    loop {
        if let Some(after_comment) = rest.strip_prefix("/*") {
            saw_comment = true;
            let Some(end) = after_comment.find("*/") else {
                return ShardHint::Unknown;
            };
            let comment = after_comment[..end].trim();
            if let Some(hint) = parse_hint_comment(comment) {
                return hint;
            }

            rest = &after_comment[end + 2..];
            rest = rest.trim_start();
            continue;
        }

        if let Some(after_comment) = rest.strip_prefix("--") {
            saw_comment = true;
            let end = after_comment.find('\n').unwrap_or(after_comment.len());
            let comment = after_comment[..end].trim();
            if let Some(hint) = parse_hint_comment(comment) {
                return hint;
            }

            if end == after_comment.len() {
                break;
            }

            rest = &after_comment[end + 1..];
            rest = rest.trim_start();
            continue;
        }

        let trimmed = rest.trim_start();
        if trimmed == rest {
            break;
        }
        rest = trimmed;
    }

    if saw_comment {
        ShardHint::Unknown
    } else {
        ShardHint::None
    }
}

fn parse_statement(
    tokens: &[Token],
    route_map: &RouteMapValidationInput,
) -> Option<ShardExtraction> {
    let first = tokens.first()?;
    let keyword = first.as_keyword()?;

    match keyword {
        "select" => parse_select(tokens, route_map),
        "insert" => parse_insert(tokens, route_map),
        "update" => parse_update(tokens, route_map),
        "delete" => parse_delete(tokens, route_map),
        _ => None,
    }
}

fn parse_select(tokens: &[Token], route_map: &RouteMapValidationInput) -> Option<ShardExtraction> {
    let from_index = find_keyword(tokens, "from", 0)?;
    let table = parse_table_reference(tokens, from_index + 1)?;
    let resolved = resolve_table(&table, route_map)?;
    let where_index = find_keyword(tokens, "where", from_index + 1)?;
    let clause = &tokens[where_index + 1..];
    extract_where_key(clause, &resolved)
}

fn parse_insert(tokens: &[Token], route_map: &RouteMapValidationInput) -> Option<ShardExtraction> {
    let mut index = 1;
    if matches!(tokens.get(index).and_then(Token::as_keyword), Some("into")) {
        index += 1;
    }

    let table = parse_table_reference(tokens, index)?;
    let resolved = resolve_table(&table, route_map)?;
    index = table.next_index;

    let columns = if matches!(tokens.get(index), Some(Token::LParen)) {
        let (columns, next_index) = parse_identifier_list(tokens, index)?;
        index = next_index;
        Some(columns)
    } else {
        None
    };

    if !matches!(tokens.get(index).and_then(Token::as_keyword), Some("values")) {
        return None;
    }

    let columns = columns?;
    extract_insert_values(&tokens[index + 1..], &resolved, &columns)
}

fn parse_update(tokens: &[Token], route_map: &RouteMapValidationInput) -> Option<ShardExtraction> {
    let mut index = 1;
    if matches!(tokens.get(index).and_then(Token::as_keyword), Some("only")) {
        index += 1;
    }

    let table = parse_table_reference(tokens, index)?;
    let resolved = resolve_table(&table, route_map)?;
    index = table.next_index;

    let set_index = find_keyword(tokens, "set", index)?;
    let after_set = set_index + 1;
    let where_index = find_keyword(tokens, "where", after_set)?;
    let set_clause = &tokens[after_set..where_index];
    let where_clause = &tokens[where_index + 1..];
    extract_combined_key(set_clause, where_clause, &resolved)
}

fn parse_delete(tokens: &[Token], route_map: &RouteMapValidationInput) -> Option<ShardExtraction> {
    let mut index = 1;
    if matches!(tokens.get(index).and_then(Token::as_keyword), Some("from")) {
        index += 1;
    }

    let table = parse_table_reference(tokens, index)?;
    let resolved = resolve_table(&table, route_map)?;
    index = table.next_index;

    let where_index = find_keyword(tokens, "where", index)?;
    let clause = &tokens[where_index + 1..];
    extract_where_key(clause, &resolved)
}

#[derive(Clone, Debug)]
struct ParsedTableReference {
    schema: Option<Arc<str>>,
    table: Arc<str>,
    next_index: usize,
}

fn parse_table_reference(tokens: &[Token], start: usize) -> Option<ParsedTableReference> {
    let mut index = start;
    let first = tokens.get(index)?.as_ident()?;
    let mut schema = None;
    let mut table = Arc::<str>::from(first);
    index += 1;

    if matches!(tokens.get(index), Some(Token::Dot)) {
        index += 1;
        let second = tokens.get(index)?.as_ident()?;
        schema = Some(table);
        table = Arc::<str>::from(second);
        index += 1;
    }

    Some(ParsedTableReference {
        schema,
        table,
        next_index: index,
    })
}

fn parse_identifier_list(
    tokens: &[Token],
    start: usize,
) -> Option<(Vec<Arc<str>>, usize)> {
    let mut index = start;
    if !matches!(tokens.get(index), Some(Token::LParen)) {
        return None;
    }

    index += 1;
    let mut values = Vec::new();

    loop {
        let value = Arc::<str>::from(tokens.get(index)?.as_ident()?);
        values.push(value);
        index += 1;

        match tokens.get(index) {
            Some(Token::Comma) => {
                index += 1;
            }
            Some(Token::RParen) => {
                index += 1;
                break;
            }
            _ => return None,
        }
    }

    Some((values, index))
}

fn extract_where_key(
    clause: &[Token],
    table: &ResolvedTable<'_>,
) -> Option<ShardExtraction> {
    let values = collect_key_values(clause, table.shard_key_column.as_deref())?;
    build_extraction(table, values)
}

fn extract_combined_key(
    set_clause: &[Token],
    where_clause: &[Token],
    table: &ResolvedTable<'_>,
) -> Option<ShardExtraction> {
    let mut values = collect_key_values(set_clause, table.shard_key_column.as_deref())?;
    values.extend(collect_key_values(where_clause, table.shard_key_column.as_deref())?);
    build_extraction(table, values)
}

fn extract_insert_values(
    clause: &[Token],
    table: &ResolvedTable<'_>,
    columns: &[Arc<str>],
) -> Option<ShardExtraction> {
    let shard_key_column = table.shard_key_column.as_deref()?;
    let column_index = columns
        .iter()
        .position(|column| identifier_matches(column.as_ref(), shard_key_column))?;
    let mut index = 0;
    let mut values = Vec::new();

    loop {
        if !matches!(clause.get(index), Some(Token::LParen)) {
            break;
        }
        index += 1;

        let mut row_values = Vec::new();
        loop {
            let value = parse_literal(clause, index)?;
            row_values.push(value.0);
            index = value.1;

            match clause.get(index) {
                Some(Token::Comma) => {
                    index += 1;
                }
                Some(Token::RParen) => {
                    index += 1;
                    break;
                }
                _ => return None,
            }
        }

        let row_value = row_values.get(column_index)?.clone();
        values.push(row_value);

        match clause.get(index) {
            Some(Token::Comma) => {
                index += 1;
            }
            Some(Token::Semicolon) | None => break,
            Some(Token::Ident(keyword)) if keyword.eq_ignore_ascii_case("returning") => break,
            _ => {}
        }
    }

    build_extraction(table, values)
}

fn collect_key_values(tokens: &[Token], shard_key_column: Option<&str>) -> Option<Vec<ShardKey>> {
    let shard_key_column = shard_key_column?;
    let mut values = Vec::new();
    let mut index = 0;

    while index < tokens.len() {
        if matches!(tokens.get(index), Some(Token::Ident(keyword)) if keyword.eq_ignore_ascii_case("or"))
        {
            return None;
        }

        if let Some((value, next_index)) = parse_column_equality(tokens, index, shard_key_column) {
            values.push(value);
            index = next_index;
            continue;
        }

        index += 1;
    }

    Some(values)
}

fn parse_column_equality(
    tokens: &[Token],
    start: usize,
    shard_key_column: &str,
) -> Option<(ShardKey, usize)> {
    let mut index = start;
    let mut qualifier_count = 0usize;

    let first = tokens.get(index)?.as_ident()?;
    index += 1;

    if matches!(tokens.get(index), Some(Token::Dot)) {
        qualifier_count += 1;
        index += 1;
        let second = tokens.get(index)?.as_ident()?;
        if !identifier_matches(second, shard_key_column) {
            return None;
        }
        index += 1;
    } else if identifier_matches(first, shard_key_column) {
        // matched single-part name
    } else {
        return None;
    }

    if qualifier_count > 1 {
        return None;
    }

    if !matches!(tokens.get(index), Some(Token::Equals)) {
        return None;
    }
    index += 1;

    let (value, next_index) = parse_literal(tokens, index)?;
    if !is_literal_terminator(tokens.get(next_index)) {
        return None;
    }

    Some((value, next_index))
}

fn parse_literal(tokens: &[Token], index: usize) -> Option<(ShardKey, usize)> {
    match tokens.get(index)? {
        Token::Number(value) => value.parse::<i64>().ok().map(|parsed| (ShardKey::integer(parsed), index + 1)),
        Token::String(value) => Some((ShardKey::text(value.clone()), index + 1)),
        Token::Ident(value) if value.eq_ignore_ascii_case("null") => None,
        Token::Param => None,
        _ => None,
    }
}

fn build_extraction(
    table: &ResolvedTable<'_>,
    values: Vec<ShardKey>,
) -> Option<ShardExtraction> {
    let mut unique_values = Vec::new();

    for value in values {
        if !unique_values.iter().any(|seen: &ShardKey| seen == &value) {
            unique_values.push(value);
        }
    }

    if unique_values.len() != 1 {
        return None;
    }

    let key = unique_values.into_iter().next()?;
    Some(ShardExtraction::Key {
        schema: table.schema.as_ref().map(|value| Arc::from(value.as_ref())),
        table: Arc::from(table.table.as_ref()),
        column: Arc::from(table.shard_key_column.as_deref()?),
        key,
    })
}

#[derive(Clone, Debug)]
struct ResolvedTable<'a> {
    schema: Option<&'a str>,
    table: &'a str,
    shard_key_column: Option<&'a str>,
}

fn resolve_table<'a>(
    table: &ParsedTableReference,
    route_map: &'a RouteMapValidationInput,
) -> Option<ResolvedTable<'a>> {
    let mut matches = route_map
        .sharded_tables
        .iter()
        .filter(|definition| table_matches_definition(table, definition))
        .collect::<Vec<_>>();

    if matches.len() != 1 {
        return None;
    }

    let definition = matches.remove(0);
    let (schema, table_name) = split_table_name(definition.name.as_str())?;
    Some(ResolvedTable {
        schema,
        table: table_name,
        shard_key_column: definition.shard_key_column.as_deref(),
    })
}

fn table_matches_definition(
    table: &ParsedTableReference,
    definition: &ShardedTableDefinition,
) -> bool {
    let Some((schema, name)) = split_table_name(definition.name.as_str()) else {
        return false;
    };

    match (&table.schema, schema) {
        (Some(actual_schema), Some(expected_schema))
            if identifier_matches(actual_schema.as_ref(), expected_schema) => {}
        (None, None) => {}
        (None, Some(_)) | (Some(_), None) => return false,
        (Some(actual_schema), Some(expected_schema))
            if !identifier_matches(actual_schema.as_ref(), expected_schema) =>
        {
            return false;
        }
        _ => {}
    }

    identifier_matches(table.table.as_ref(), name)
}

fn split_table_name(value: &str) -> Option<(Option<&str>, &str)> {
    let mut parts = value.split('.');
    let first = parts.next()?;
    let second = parts.next();
    if parts.next().is_some() {
        return None;
    }

    match second {
        Some(table) if !first.is_empty() && !table.is_empty() => Some((Some(first), table)),
        None if !first.is_empty() => Some((None, first)),
        _ => None,
    }
}

fn identifier_matches(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn is_literal_terminator(token: Option<&Token>) -> bool {
    match token {
        None | Some(Token::Comma) | Some(Token::RParen) | Some(Token::Semicolon) => true,
        Some(Token::Ident(keyword))
            if keyword.eq_ignore_ascii_case("and")
                || keyword.eq_ignore_ascii_case("or")
                || keyword.eq_ignore_ascii_case("where")
                || keyword.eq_ignore_ascii_case("set")
                || keyword.eq_ignore_ascii_case("from")
                || keyword.eq_ignore_ascii_case("returning") =>
        {
            true
        }
        _ => false,
    }
}

fn parse_hint_comment(comment: &str) -> Option<ShardHint> {
    let comment = comment.trim();
    let Some(body) = comment.strip_prefix("pg-kinetic:") else {
        return None;
    };

    let directive = body.trim();
    let (kind, value) = directive.split_once('=')?;
    let kind = kind.trim();
    let value = value.trim();
    if !is_hint_value(value) {
        return Some(ShardHint::Unknown);
    }

    match kind {
        "shard" => Some(ShardHint::Shard(Arc::from(value))),
        "tenant" => Some(ShardHint::Tenant(Arc::from(value))),
        "route" => Some(ShardHint::Route(Arc::from(value))),
        _ => Some(ShardHint::Unknown),
    }
}

fn is_hint_value(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.'))
}

fn find_keyword(tokens: &[Token], keyword: &str, start: usize) -> Option<usize> {
    let mut depth = 0isize;
    let mut index = start;
    while let Some(token) = tokens.get(index) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Ident(value) if depth == 0 && value.eq_ignore_ascii_case(keyword) => {
                return Some(index)
            }
            _ => {}
        }
        index += 1;
    }

    None
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    String(String),
    Param,
    Comma,
    Dot,
    LParen,
    RParen,
    Equals,
    Semicolon,
    ColonColon,
    Star,
}

impl Token {
    fn as_ident(&self) -> Option<&str> {
        match self {
            Self::Ident(value) => Some(value),
            _ => None,
        }
    }

    fn as_keyword(&self) -> Option<&str> {
        self.as_ident()
    }
}

fn tokenize(sql: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(character) = chars.next() {
        match character {
            whitespace if whitespace.is_whitespace() => {}
            '-' if matches!(chars.peek(), Some('-')) => {
                chars.next();
                while let Some(next) = chars.next() {
                    if next == '\n' {
                        break;
                    }
                }
            }
            '/' if matches!(chars.peek(), Some('*')) => {
                chars.next();
                let mut previous = '\0';
                let mut closed = false;
                while let Some(next) = chars.next() {
                    if previous == '*' && next == '/' {
                        closed = true;
                        break;
                    }
                    previous = next;
                }
                if !closed {
                    return None;
                }
            }
            '\'' => {
                let mut value = String::new();
                loop {
                    let Some(next) = chars.next() else {
                        return None;
                    };
                    if next == '\'' {
                        if matches!(chars.peek(), Some('\'')) {
                            chars.next();
                            value.push('\'');
                            continue;
                        }
                        break;
                    }
                    value.push(next);
                }
                tokens.push(Token::String(value));
            }
            '"' => {
                let mut value = String::new();
                loop {
                    let Some(next) = chars.next() else {
                        return None;
                    };
                    if next == '"' {
                        if matches!(chars.peek(), Some('"')) {
                            chars.next();
                            value.push('"');
                            continue;
                        }
                        break;
                    }
                    value.push(next);
                }
                tokens.push(Token::Ident(value));
            }
            '(' => tokens.push(Token::LParen),
            ')' => tokens.push(Token::RParen),
            ',' => tokens.push(Token::Comma),
            '.' => tokens.push(Token::Dot),
            '=' => tokens.push(Token::Equals),
            ';' => tokens.push(Token::Semicolon),
            '*' => tokens.push(Token::Star),
            ':' if matches!(chars.peek(), Some(':')) => {
                chars.next();
                tokens.push(Token::ColonColon);
            }
            '?' => tokens.push(Token::Param),
            '$' => {
                let mut value = String::from("$");
                while matches!(chars.peek(), Some(next) if next.is_ascii_digit()) {
                    value.push(chars.next().unwrap_or_default());
                }
                if value.len() == 1 {
                    return None;
                }
                tokens.push(Token::Param);
            }
            '-' if matches!(chars.peek(), Some(next) if next.is_ascii_digit()) => {
                let mut value = String::from("-");
                while matches!(chars.peek(), Some(next) if next.is_ascii_digit()) {
                    value.push(chars.next().unwrap_or_default());
                }
                tokens.push(Token::Number(value));
            }
            digit if digit.is_ascii_digit() => {
                let mut value = String::from(digit);
                while matches!(chars.peek(), Some(next) if next.is_ascii_digit()) {
                    value.push(chars.next().unwrap_or_default());
                }
                tokens.push(Token::Number(value));
            }
            alphabetic if is_ident_start(alphabetic) => {
                let mut value = String::from(alphabetic);
                while matches!(chars.peek(), Some(next) if is_ident_part(*next)) {
                    value.push(chars.next().unwrap_or_default());
                }
                tokens.push(Token::Ident(value));
            }
            _ => return None,
        }
    }

    Some(tokens)
}

fn is_ident_start(character: char) -> bool {
    character.is_ascii_alphabetic() || character == '_'
}

fn is_ident_part(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '$')
}
