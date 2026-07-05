use std::collections::{HashMap, HashSet};

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

#[derive(Clone, Debug)]
pub struct PreparedCatalog {
    session_id: u64,
    next_statement_id: u64,
    statements: HashMap<String, PreparedStatement>,
    materialized: HashMap<u64, HashSet<String>>,
}

impl PreparedCatalog {
    #[must_use]
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            next_statement_id: 1,
            statements: HashMap::new(),
            materialized: HashMap::new(),
        }
    }

    pub fn upsert(
        &mut self,
        client_name: impl Into<String>,
        query: impl Into<String>,
        parameter_type_oids: Vec<i32>,
    ) -> &PreparedStatement {
        let client_name = client_name.into();
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
        for names in self.materialized.values_mut() {
            names.remove(&removed.backend_name);
        }
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
        sqlstate: &str,
        backend_id: u64,
    ) -> InvalidationScope {
        match sqlstate {
            "26000" => {
                self.materialized.remove(&backend_id);
                InvalidationScope::Backend
            }
            "0A000" | "42P01" | "42703" => {
                self.materialized.clear();
                InvalidationScope::AllBackends
            }
            _ => InvalidationScope::None,
        }
    }
}
