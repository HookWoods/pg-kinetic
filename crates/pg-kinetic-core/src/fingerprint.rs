use crate::protocol::sql_classify::{tokenize_sql, SqlToken};

pub const MAX_FINGERPRINT_BYTES: usize = 256;

#[must_use]
pub fn fingerprint_sql(sql: &str) -> String {
    let tokens = tokenize_sql(sql);
    let mut output = String::new();
    let mut in_list = false;
    let mut list_values = 0usize;

    for token in tokens {
        match token {
            SqlToken::Comment => {}
            SqlToken::Literal => {
                if !in_list {
                    append_token(&mut output, "?");
                } else if list_values == 0 {
                    append_token(&mut output, "?");
                }
                list_values += 1;
            }
            SqlToken::Word(word) => append_token(&mut output, &word.to_ascii_lowercase()),
            SqlToken::Symbol(symbol) => {
                if symbol == "(" && output.ends_with(" in") {
                    in_list = true;
                    list_values = 0;
                    append_token(&mut output, "(");
                } else if in_list && symbol == ")" {
                    append_token(&mut output, ")");
                    in_list = false;
                } else if in_list && symbol == "," {
                    list_values += 1;
                } else {
                    append_token(&mut output, &symbol);
                }
            }
        }
        if output.len() >= MAX_FINGERPRINT_BYTES {
            output.truncate(MAX_FINGERPRINT_BYTES);
            break;
        }
    }

    output
}

fn append_token(output: &mut String, token: &str) {
    let needs_space =
        !output.is_empty() && !output.ends_with('(') && !token.starts_with([')', ',', ';']);
    if needs_space {
        output.push(' ');
    }
    output.push_str(token);
}

#[cfg(test)]
mod tests {
    use super::fingerprint_sql;

    #[test]
    fn normalizes_literals_comments_and_in_lists() {
        assert_eq!(
            fingerprint_sql("SELECT * FROM users WHERE id IN (1, 2, 3) -- secret"),
            "select * from users where id in (?)"
        );
        assert_eq!(
            fingerprint_sql("select * from users where id in ('a')"),
            "select * from users where id in (?)"
        );
    }

    #[test]
    fn is_stable_for_equivalent_query_shapes() {
        assert_eq!(
            fingerprint_sql("/* pii */ SELECT name FROM users WHERE id = 42"),
            fingerprint_sql("SELECT name FROM users WHERE id = 7")
        );
        assert_eq!(
            fingerprint_sql("SELECT $$secret$$ FROM users"),
            "select ? from users"
        );
    }
}
