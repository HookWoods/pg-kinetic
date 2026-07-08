use std::{cmp::Ordering, fmt, str::FromStr, sync::Arc};

use bytes::Bytes;
use thiserror::Error;

use crate::{route::RouteKey, routing::BackendRole};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ShardId(Arc<str>);

impl ShardId {
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, ShardValidationError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ShardValidationError::EmptyShardId);
        }

        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ShardId {
    type Err = ShardValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ShardKeyType {
    Text,
    Integer,
    Bytes,
}

impl ShardKeyType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Integer => "integer",
            Self::Bytes => "bytes",
        }
    }
}

impl fmt::Display for ShardKeyType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ShardKey {
    Text(Arc<str>),
    Integer(i64),
    Bytes(Bytes),
}

impl ShardKey {
    #[must_use]
    pub fn text(value: impl Into<Arc<str>>) -> Self {
        Self::Text(value.into())
    }

    #[must_use]
    pub const fn integer(value: i64) -> Self {
        Self::Integer(value)
    }

    #[must_use]
    pub fn bytes(value: impl Into<Bytes>) -> Self {
        Self::Bytes(value.into())
    }

    #[must_use]
    pub const fn key_type(&self) -> ShardKeyType {
        match self {
            Self::Text(_) => ShardKeyType::Text,
            Self::Integer(_) => ShardKeyType::Integer,
            Self::Bytes(_) => ShardKeyType::Bytes,
        }
    }

    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(*value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ShardStrategy {
    Hash,
    Range,
    List,
}

impl ShardStrategy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Range => "range",
            Self::List => "list",
        }
    }
}

impl fmt::Display for ShardStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashShardRule {
    shard_key_type: ShardKeyType,
    bucket_count: usize,
}

impl HashShardRule {
    #[must_use]
    pub fn new(
        shard_key_type: ShardKeyType,
        bucket_count: usize,
    ) -> Result<Self, ShardValidationError> {
        if bucket_count == 0 {
            return Err(ShardValidationError::InvalidBucketCount);
        }

        Ok(Self {
            shard_key_type,
            bucket_count,
        })
    }

    #[must_use]
    pub const fn shard_key_type(&self) -> ShardKeyType {
        self.shard_key_type
    }

    #[must_use]
    pub const fn bucket_count(&self) -> usize {
        self.bucket_count
    }
}

#[must_use]
fn compare_shard_keys(left: &ShardKey, right: &ShardKey) -> Option<Ordering> {
    match (left, right) {
        (ShardKey::Text(left), ShardKey::Text(right)) => Some(left.as_ref().cmp(right.as_ref())),
        (ShardKey::Integer(left), ShardKey::Integer(right)) => Some(left.cmp(right)),
        (ShardKey::Bytes(left), ShardKey::Bytes(right)) => Some(left.as_ref().cmp(right.as_ref())),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeShardRule {
    lower_bound: ShardKey,
    upper_bound: ShardKey,
}

impl RangeShardRule {
    #[must_use]
    pub fn new(
        lower_bound: ShardKey,
        upper_bound: ShardKey,
    ) -> Result<Self, ShardValidationError> {
        if lower_bound.key_type() != upper_bound.key_type() {
            return Err(ShardValidationError::KeyTypeMismatch {
                expected: lower_bound.key_type(),
                actual: upper_bound.key_type(),
            });
        }

        if matches!(
            compare_shard_keys(&lower_bound, &upper_bound),
            Some(Ordering::Greater)
        ) {
            return Err(ShardValidationError::InvalidRangeBounds);
        }

        Ok(Self {
            lower_bound,
            upper_bound,
        })
    }

    #[must_use]
    pub fn lower_bound(&self) -> &ShardKey {
        &self.lower_bound
    }

    #[must_use]
    pub fn upper_bound(&self) -> &ShardKey {
        &self.upper_bound
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShardRule {
    values: Vec<ShardKey>,
}

impl ListShardRule {
    #[must_use]
    pub fn new(values: impl Into<Vec<ShardKey>>) -> Result<Self, ShardValidationError> {
        let values = values.into();
        if values.is_empty() {
            return Err(ShardValidationError::EmptyShardList);
        }

        let first_key_type = values[0].key_type();
        if values
            .iter()
            .any(|value| value.key_type() != first_key_type)
        {
            return Err(ShardValidationError::KeyTypeMismatch {
                expected: first_key_type,
                actual: values
                    .iter()
                    .find(|value| value.key_type() != first_key_type)
                    .map(ShardKey::key_type)
                    .unwrap_or(first_key_type),
            });
        }

        Ok(Self { values })
    }

    #[must_use]
    pub fn values(&self) -> &[ShardKey] {
        &self.values
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TenantScope {
    tenant_id: Arc<str>,
}

impl TenantScope {
    #[must_use]
    pub fn new(tenant_id: impl Into<Arc<str>>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }

    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TableScope {
    schema: Arc<str>,
    table: Arc<str>,
}

impl TableScope {
    #[must_use]
    pub fn new(schema: impl Into<Arc<str>>, table: impl Into<Arc<str>>) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
        }
    }

    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ShardScope {
    Global,
    Tenant(TenantScope),
    Table(TableScope),
}

impl ShardScope {
    #[must_use]
    pub const fn global() -> Self {
        Self::Global
    }

    #[must_use]
    pub fn tenant(scope: TenantScope) -> Self {
        Self::Tenant(scope)
    }

    #[must_use]
    pub fn table(scope: TableScope) -> Self {
        Self::Table(scope)
    }

    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Tenant(_) => "tenant",
            Self::Table(_) => "table",
        }
    }

    #[must_use]
    pub fn tenant_scope(&self) -> Option<&TenantScope> {
        match self {
            Self::Tenant(scope) => Some(scope),
            _ => None,
        }
    }

    #[must_use]
    pub fn table_scope(&self) -> Option<&TableScope> {
        match self {
            Self::Table(scope) => Some(scope),
            _ => None,
        }
    }
}

impl fmt::Display for ShardScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardTarget {
    route_key: RouteKey,
    backend_role: BackendRole,
    shard_id: ShardId,
}

impl ShardTarget {
    #[must_use]
    pub fn new(route_key: RouteKey, backend_role: BackendRole, shard_id: ShardId) -> Self {
        Self {
            route_key,
            backend_role,
            shard_id,
        }
    }

    #[must_use]
    pub fn route_key(&self) -> &RouteKey {
        &self.route_key
    }

    #[must_use]
    pub const fn backend_role(&self) -> BackendRole {
        self.backend_role
    }

    #[must_use]
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ShardRouteReason {
    AdminOverride,
    HashMatch,
    RangeMatch,
    ListMatch,
    MultiShardRejected,
    ValidationFailed,
    NoMatch,
}

impl ShardRouteReason {
    #[must_use]
    pub const fn admin_label(self) -> &'static str {
        match self {
            Self::AdminOverride => "admin_override",
            Self::HashMatch => "hash_match",
            Self::RangeMatch => "range_match",
            Self::ListMatch => "list_match",
            Self::MultiShardRejected => "multi_shard_rejected",
            Self::ValidationFailed => "validation_failed",
            Self::NoMatch => "no_match",
        }
    }

    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::AdminOverride => "admin_override",
            Self::HashMatch => "hash_match",
            Self::RangeMatch => "range_match",
            Self::ListMatch => "list_match",
            Self::MultiShardRejected => "multi_shard_rejected",
            Self::ValidationFailed => "validation_failed",
            Self::NoMatch => "no_match",
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.admin_label()
    }
}

impl fmt::Display for ShardRouteReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum MultiShardPolicy {
    #[default]
    Reject,
    FirstMatch,
    FanOut,
}

impl MultiShardPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::FirstMatch => "first_match",
            Self::FanOut => "fan_out",
        }
    }
}

impl fmt::Display for MultiShardPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRoute {
    target: ShardTarget,
    reason: ShardRouteReason,
}

impl ShardRoute {
    #[must_use]
    pub fn new(target: ShardTarget, reason: ShardRouteReason) -> Self {
        Self { target, reason }
    }

    #[must_use]
    pub fn target(&self) -> &ShardTarget {
        &self.target
    }

    #[must_use]
    pub const fn reason(&self) -> ShardRouteReason {
        self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRouteMap {
    scope: ShardScope,
    strategy: ShardStrategy,
    policy: MultiShardPolicy,
    routes: Vec<ShardRoute>,
}

impl ShardRouteMap {
    #[must_use]
    pub fn new(
        scope: ShardScope,
        strategy: ShardStrategy,
        policy: MultiShardPolicy,
        routes: impl Into<Vec<ShardRoute>>,
    ) -> Result<Self, ShardValidationError> {
        let routes = routes.into();
        if routes.is_empty() {
            return Err(ShardValidationError::EmptyShardRouteMap);
        }

        if policy == MultiShardPolicy::Reject && routes.len() > 1 {
            return Err(ShardValidationError::MultiShardRejected);
        }

        let mut seen_shards = Vec::with_capacity(routes.len());
        for route in &routes {
            let shard_id = route.target().shard_id();
            if seen_shards.iter().any(|seen: &ShardId| seen == shard_id) {
                return Err(ShardValidationError::DuplicateShardId {
                    shard_id: shard_id.clone(),
                });
            }
            seen_shards.push(shard_id.clone());
        }

        Ok(Self {
            scope,
            strategy,
            policy,
            routes,
        })
    }

    #[must_use]
    pub fn scope(&self) -> &ShardScope {
        &self.scope
    }

    #[must_use]
    pub const fn strategy(&self) -> ShardStrategy {
        self.strategy
    }

    #[must_use]
    pub const fn policy(&self) -> MultiShardPolicy {
        self.policy
    }

    #[must_use]
    pub fn routes(&self) -> &[ShardRoute] {
        &self.routes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRouteDecision {
    route: Option<ShardRoute>,
    reason: ShardRouteReason,
    policy: MultiShardPolicy,
}

impl ShardRouteDecision {
    #[must_use]
    pub const fn new(
        route: Option<ShardRoute>,
        reason: ShardRouteReason,
        policy: MultiShardPolicy,
    ) -> Self {
        Self {
            route,
            reason,
            policy,
        }
    }

    #[must_use]
    pub fn route(&self) -> Option<&ShardRoute> {
        self.route.as_ref()
    }

    #[must_use]
    pub const fn reason(&self) -> ShardRouteReason {
        self.reason
    }

    #[must_use]
    pub const fn policy(&self) -> MultiShardPolicy {
        self.policy
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ShardValidationError {
    #[error("shard id cannot be empty")]
    EmptyShardId,
    #[error("shard route map cannot be empty")]
    EmptyShardRouteMap,
    #[error("shard route policy rejects multi-shard decisions")]
    MultiShardRejected,
    #[error("shard list cannot be empty")]
    EmptyShardList,
    #[error("shard bucket count must be greater than zero")]
    InvalidBucketCount,
    #[error("shard key type mismatch: expected {expected}, found {actual}")]
    KeyTypeMismatch {
        expected: ShardKeyType,
        actual: ShardKeyType,
    },
    #[error("shard range bounds are invalid")]
    InvalidRangeBounds,
    #[error("duplicate shard id {shard_id}")]
    DuplicateShardId { shard_id: ShardId },
}
