use std::collections::{HashMap, HashSet};

use pg_kinetic_wire::sqlstate::SqlState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationScope {
    None,
    Backend,
    AllBackends,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedStatement {
    pub client_name: String,
    pub backend_name: String,
    pub query: String,
    pub parameter_type_oids: Vec<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedStatementSnapshot {
    pub session_id: u64,
    pub client_statement_name: String,
    pub backend_statement_name: String,
    pub materialized_backend_count: usize,
    pub invalidation_count: u64,
}

#[derive(Clone, Debug)]
pub struct PreparedCatalog {
    session_id: u64,
    next_statement_id: u64,
    statements: HashMap<String, PreparedStatement>,
    materialized: HashMap<u64, HashSet<String>>,
    invalidation_counts: HashMap<String, u64>,
}

impl PreparedCatalog {
    #[must_use]
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            next_statement_id: 1,
            statements: HashMap::new(),
            materialized: HashMap::new(),
            invalidation_counts: HashMap::new(),
        }
    }

    pub fn upsert(
        &mut self,
        client_name: impl Into<String>,
        query: impl Into<String>,
        parameter_type_oids: Vec<i32>,
    ) -> &PreparedStatement {
        let client_name = client_name.into();
        if let Some(previous_backend_name) = self
            .statements
            .get(&client_name)
            .map(|statement| statement.backend_name.clone())
        {
            self.remove_materialized_statement(&previous_backend_name);
            self.invalidation_counts.remove(&previous_backend_name);
        }

        let backend_name = if client_name.is_empty() {
            String::new()
        } else {
            let name = format!("pgk_{}_{}", self.session_id, self.next_statement_id);
            self.next_statement_id += 1;
            name
        };

        self.statements.insert(
            client_name.clone(),
            PreparedStatement {
                client_name: client_name.clone(),
                backend_name,
                query: query.into(),
                parameter_type_oids,
            },
        );

        self.statements
            .get(&client_name)
            .expect("statement inserted before lookup")
    }

    #[must_use]
    pub fn get(&self, client_name: &str) -> Option<&PreparedStatement> {
        self.statements.get(client_name)
    }

    pub fn remove(&mut self, client_name: &str) -> Option<PreparedStatement> {
        let removed = self.statements.remove(client_name)?;
        self.remove_materialized_statement(&removed.backend_name);
        self.invalidation_counts.remove(&removed.backend_name);
        Some(removed)
    }

    #[must_use]
    pub fn is_materialized(&self, backend_id: u64, statement: &PreparedStatement) -> bool {
        statement.backend_name.is_empty()
            || self
                .materialized
                .get(&backend_id)
                .is_some_and(|names| names.contains(&statement.backend_name))
    }

    pub fn mark_materialized(&mut self, backend_id: u64, statement: &PreparedStatement) {
        if statement.backend_name.is_empty() {
            return;
        }

        self.materialized
            .entry(backend_id)
            .or_default()
            .insert(statement.backend_name.clone());
    }

    pub fn invalidate_for_sqlstate(
        &mut self,
        sqlstate: SqlState,
        backend_id: u64,
    ) -> InvalidationScope {
        match sqlstate {
            SqlState::InvalidSqlStatementName => {
                if let Some(names) = self.materialized.remove(&backend_id) {
                    for backend_name in names {
                        self.increment_invalidation_count(&backend_name);
                    }
                }
                InvalidationScope::Backend
            }
            SqlState::FeatureNotSupported
            | SqlState::UndefinedTable
            | SqlState::UndefinedColumn => {
                let backend_names: Vec<String> = self
                    .materialized
                    .values()
                    .flat_map(|names| names.iter().cloned())
                    .collect();
                for backend_name in backend_names {
                    self.increment_invalidation_count(&backend_name);
                }
                self.materialized.clear();
                InvalidationScope::AllBackends
            }
            _ => InvalidationScope::None,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<PreparedStatementSnapshot> {
        let mut snapshots: Vec<PreparedStatementSnapshot> = self
            .statements
            .values()
            .map(|statement| PreparedStatementSnapshot {
                session_id: self.session_id,
                client_statement_name: statement.client_name.clone(),
                backend_statement_name: statement.backend_name.clone(),
                materialized_backend_count: self.materialized_backend_count(statement),
                invalidation_count: self
                    .invalidation_counts
                    .get(&statement.backend_name)
                    .copied()
                    .unwrap_or_default(),
            })
            .collect();
        snapshots.sort_by(|left, right| {
            left.client_statement_name
                .cmp(&right.client_statement_name)
                .then_with(|| {
                    left.backend_statement_name
                        .cmp(&right.backend_statement_name)
                })
        });
        snapshots
    }

    fn materialized_backend_count(&self, statement: &PreparedStatement) -> usize {
        if statement.backend_name.is_empty() {
            return 0;
        }

        self.materialized
            .values()
            .filter(|names| names.contains(&statement.backend_name))
            .count()
    }

    fn remove_materialized_statement(&mut self, backend_name: &str) {
        if backend_name.is_empty() {
            return;
        }

        for names in self.materialized.values_mut() {
            names.remove(backend_name);
        }
    }

    fn increment_invalidation_count(&mut self, backend_name: &str) {
        if backend_name.is_empty() {
            return;
        }

        *self
            .invalidation_counts
            .entry(backend_name.to_owned())
            .or_default() += 1;
    }
}
