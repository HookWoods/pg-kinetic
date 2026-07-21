use std::collections::HashMap;

use crate::sql_classify::{analyze_sql, SqlAnalysis};
use crate::{
    session::PreparedShardSummary,
    sharding::ShardId,
    sql::{classify, SqlCommand},
};
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
    pub analysis: SqlAnalysis,
    pub command: SqlCommand,
    pub parameter_type_oids: Vec<i32>,
    pub route_map_generation_id: u64,
    pub shard_summary: PreparedShardSummary,
    pub cache_key: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedStatementSnapshot {
    pub session_id: u64,
    pub client_statement_name: String,
    pub backend_statement_name: String,
    pub materialized_backend_count: usize,
    pub invalidation_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MaterializedStatement {
    shard_id: Option<ShardId>,
}

#[derive(Clone, Debug)]
pub struct PreparedCatalog {
    session_id: u64,
    next_statement_id: u64,
    route_map_generation_id: u64,
    statements: HashMap<String, PreparedStatement>,
    materialized: HashMap<u64, HashMap<u64, MaterializedStatement>>,
    invalidation_counts: HashMap<u64, u64>,
}

impl PreparedCatalog {
    #[must_use]
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            next_statement_id: 1,
            route_map_generation_id: 0,
            statements: HashMap::new(),
            materialized: HashMap::new(),
            invalidation_counts: HashMap::new(),
        }
    }

    #[must_use]
    pub const fn route_map_generation_id(&self) -> u64 {
        self.route_map_generation_id
    }

    pub fn set_route_map_generation_id(&mut self, route_map_generation_id: u64) {
        if self.route_map_generation_id == route_map_generation_id {
            return;
        }

        self.route_map_generation_id = route_map_generation_id;
        self.materialized.clear();
        self.invalidation_counts.clear();
    }

    pub fn upsert(
        &mut self,
        client_name: impl Into<String>,
        query: impl Into<String>,
        parameter_type_oids: Vec<i32>,
    ) -> &PreparedStatement {
        let client_name = client_name.into();
        let query = query.into();
        let analysis = analyze_sql(&query);
        let command = classify(&query);
        if let Some(previous_cache_key) = self
            .statements
            .get(&client_name)
            .map(PreparedStatement::cache_key)
        {
            self.remove_materialized_statement(previous_cache_key);
            self.invalidation_counts.remove(&previous_cache_key);
        }

        let (backend_name, cache_key) = if client_name.is_empty() {
            (String::new(), 0)
        } else {
            let cache_key = self.next_statement_id;
            let name = format!("pgk_{}_{}", self.session_id, cache_key);
            self.next_statement_id += 1;
            (name, cache_key)
        };

        self.statements.insert(
            client_name.clone(),
            PreparedStatement {
                client_name: client_name.clone(),
                backend_name,
                query,
                analysis,
                command,
                parameter_type_oids,
                route_map_generation_id: self.route_map_generation_id,
                shard_summary: if client_name.is_empty() {
                    PreparedShardSummary::CurrentShard
                } else {
                    PreparedShardSummary::Deferred
                },
                cache_key,
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

    #[must_use]
    pub fn get_for_current_route_map(&self, client_name: &str) -> Option<&PreparedStatement> {
        self.get(client_name)
            .filter(|statement| self.is_current_route_map(statement))
    }

    pub fn remove(&mut self, client_name: &str) -> Option<PreparedStatement> {
        let removed = self.statements.remove(client_name)?;
        self.remove_materialized_statement(removed.cache_key);
        self.invalidation_counts.remove(&removed.cache_key);
        Some(removed)
    }

    #[must_use]
    pub fn is_materialized(&self, backend_id: u64, statement: &PreparedStatement) -> bool {
        if !self.is_current_route_map(statement) {
            return false;
        }

        if statement.cache_key == 0 {
            return true;
        }

        let statement_shard_id = statement.shard_summary.shard_id();
        self.materialized
            .get(&backend_id)
            .and_then(|statements| statements.get(&statement.cache_key))
            .is_some_and(|materialized| materialized.shard_id.as_ref() == statement_shard_id)
    }

    pub fn mark_materialized(&mut self, backend_id: u64, statement: &PreparedStatement) {
        if statement.cache_key == 0 || !self.is_current_route_map(statement) {
            return;
        }

        self.materialized.entry(backend_id).or_default().insert(
            statement.cache_key,
            MaterializedStatement {
                shard_id: statement.shard_summary.shard_id().cloned(),
            },
        );
    }

    pub fn invalidate_for_sqlstate(
        &mut self,
        sqlstate: SqlState,
        backend_id: u64,
    ) -> InvalidationScope {
        match sqlstate {
            SqlState::InvalidSqlStatementName => {
                if let Some(statements) = self.materialized.remove(&backend_id) {
                    for cache_key in statements.into_keys() {
                        self.increment_invalidation_count(cache_key);
                    }
                }
                InvalidationScope::Backend
            }
            SqlState::FeatureNotSupported
            | SqlState::UndefinedTable
            | SqlState::UndefinedColumn => {
                for statements in std::mem::take(&mut self.materialized).into_values() {
                    for cache_key in statements.into_keys() {
                        self.increment_invalidation_count(cache_key);
                    }
                }
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
                    .get(&statement.cache_key)
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
        if statement.cache_key == 0 {
            return 0;
        }

        let statement_shard_id = statement.shard_summary.shard_id();
        self.materialized
            .values()
            .filter(|statements| {
                statements
                    .get(&statement.cache_key)
                    .is_some_and(|materialized| {
                        materialized.shard_id.as_ref() == statement_shard_id
                    })
            })
            .count()
    }

    fn remove_materialized_statement(&mut self, cache_key: u64) {
        if cache_key == 0 {
            return;
        }

        self.materialized.retain(|_, statements| {
            statements.remove(&cache_key);
            !statements.is_empty()
        });
    }

    fn increment_invalidation_count(&mut self, cache_key: u64) {
        if cache_key == 0 {
            return;
        }

        *self.invalidation_counts.entry(cache_key).or_default() += 1;
    }

    fn is_current_route_map(&self, statement: &PreparedStatement) -> bool {
        statement.route_map_generation_id == self.route_map_generation_id
    }
}

impl PreparedStatement {
    #[must_use]
    pub const fn cache_key(&self) -> u64 {
        self.cache_key
    }

    #[must_use]
    pub const fn analysis(&self) -> SqlAnalysis {
        self.analysis
    }

    #[must_use]
    pub const fn command(&self) -> &SqlCommand {
        &self.command
    }
}
