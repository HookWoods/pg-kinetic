use std::{fmt, fmt::Display, str::FromStr, sync::Arc, time::Duration};

use thiserror::Error;

const REDACTED_VALUE: &str = "<redacted>";
pub const POLICY_DENY_SQLSTATE: &str = "P0001";

fn validate_non_empty(
    value: Arc<str>,
    code: PolicyValidationErrorCode,
    label: &'static str,
) -> Result<Arc<str>, PolicyValidationError> {
    if value.is_empty() {
        Err(PolicyValidationError::new(
            code,
            format!("{label} cannot be empty"),
        ))
    } else {
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct PolicyValidationError {
    code: PolicyValidationErrorCode,
    message: String,
}

impl PolicyValidationError {
    #[must_use]
    pub fn new(code: PolicyValidationErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> PolicyValidationErrorCode {
        self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyValidationErrorCode {
    EmptyPolicyId,
    InvalidPolicyVersion,
    EmptyTargetId,
}

impl PolicyValidationErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EmptyPolicyId => "empty_policy_id",
            Self::InvalidPolicyVersion => "invalid_policy_version",
            Self::EmptyTargetId => "empty_target_id",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PolicyId(Arc<str>);

impl PolicyId {
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, PolicyValidationError> {
        let value = validate_non_empty(
            value.into(),
            PolicyValidationErrorCode::EmptyPolicyId,
            "policy id",
        )?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for PolicyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PolicyId {
    type Err = PolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PolicyVersion(u64);

impl PolicyVersion {
    #[must_use]
    pub fn new(value: u64) -> Result<Self, PolicyValidationError> {
        if value == 0 {
            Err(PolicyValidationError::new(
                PolicyValidationErrorCode::InvalidPolicyVersion,
                "policy version must be greater than zero",
            ))
        } else {
            Ok(Self(value))
        }
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for PolicyVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for PolicyVersion {
    type Err = PolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = value.parse::<u64>().map_err(|_| {
            PolicyValidationError::new(
                PolicyValidationErrorCode::InvalidPolicyVersion,
                "policy version must be a positive integer",
            )
        })?;
        Self::new(parsed)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PolicyRouteTargetId(Arc<str>);

impl PolicyRouteTargetId {
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, PolicyValidationError> {
        let value = validate_non_empty(
            value.into(),
            PolicyValidationErrorCode::EmptyTargetId,
            "route target id",
        )?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for PolicyRouteTargetId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PolicyRouteTargetId {
    type Err = PolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PolicyShardTargetId(Arc<str>);

impl PolicyShardTargetId {
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, PolicyValidationError> {
        let value = validate_non_empty(
            value.into(),
            PolicyValidationErrorCode::EmptyTargetId,
            "shard target id",
        )?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for PolicyShardTargetId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PolicyShardTargetId {
    type Err = PolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum PolicyMode {
    #[default]
    Disabled,
    Enforce,
    DryRun,
}

impl PolicyMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enforce => "enforce",
            Self::DryRun => "dry_run",
        }
    }
}

impl Display for PolicyMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyHookPoint {
    BeforeRouting,
    AfterRouting,
    BeforeCheckout,
}

impl PolicyHookPoint {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeRouting => "before_routing",
            Self::AfterRouting => "after_routing",
            Self::BeforeCheckout => "before_checkout",
        }
    }
}

impl Display for PolicyHookPoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyDecisionReason {
    PolicyDenied,
    ValidationFailed,
    TargetIdInvalid,
    ContextRedacted,
}

impl PolicyDecisionReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PolicyDenied => "policy_denied",
            Self::ValidationFailed => "validation_failed",
            Self::TargetIdInvalid => "target_id_invalid",
            Self::ContextRedacted => "context_redacted",
        }
    }
}

impl Display for PolicyDecisionReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyEffect {
    NoChange,
    Deny,
    RequirePrimary,
    RequireReplica,
    RouteOverride,
    ShardOverride,
}

impl PolicyEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoChange => "no_change",
            Self::Deny => "deny",
            Self::RequirePrimary => "require_primary",
            Self::RequireReplica => "require_replica",
            Self::RouteOverride => "route_override",
            Self::ShardOverride => "shard_override",
        }
    }
}

impl Display for PolicyEffect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyOutcome {
    Applied,
    Rejected,
    Skipped,
    Redacted,
}

impl PolicyOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Skipped => "skipped",
            Self::Redacted => "redacted",
        }
    }
}

impl Display for PolicyOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyAction {
    Allow,
    Deny {
        reason: PolicyDecisionReason,
        sqlstate: &'static str,
    },
    RequirePrimary,
    RequireReplica,
    RouteOverride {
        target_id: PolicyRouteTargetId,
    },
    ShardOverride {
        target_id: PolicyShardTargetId,
    },
}

impl PolicyAction {
    pub const DENY_REASON: PolicyDecisionReason = PolicyDecisionReason::PolicyDenied;
    pub const DENY_SQLSTATE: &'static str = POLICY_DENY_SQLSTATE;

    #[must_use]
    pub const fn allow() -> Self {
        Self::Allow
    }

    #[must_use]
    pub const fn deny() -> Self {
        Self::Deny {
            reason: Self::DENY_REASON,
            sqlstate: Self::DENY_SQLSTATE,
        }
    }

    #[must_use]
    pub const fn require_primary() -> Self {
        Self::RequirePrimary
    }

    #[must_use]
    pub const fn require_replica() -> Self {
        Self::RequireReplica
    }

    #[must_use]
    pub fn route_override(target_id: PolicyRouteTargetId) -> Self {
        Self::RouteOverride { target_id }
    }

    #[must_use]
    pub fn shard_override(target_id: PolicyShardTargetId) -> Self {
        Self::ShardOverride { target_id }
    }

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny { .. } => "deny",
            Self::RequirePrimary => "require_primary",
            Self::RequireReplica => "require_replica",
            Self::RouteOverride { .. } => "route_override",
            Self::ShardOverride { .. } => "shard_override",
        }
    }

    #[must_use]
    pub const fn effect(&self) -> PolicyEffect {
        match self {
            Self::Allow => PolicyEffect::NoChange,
            Self::Deny { .. } => PolicyEffect::Deny,
            Self::RequirePrimary => PolicyEffect::RequirePrimary,
            Self::RequireReplica => PolicyEffect::RequireReplica,
            Self::RouteOverride { .. } => PolicyEffect::RouteOverride,
            Self::ShardOverride { .. } => PolicyEffect::ShardOverride,
        }
    }

    #[must_use]
    pub const fn overrides_routing(&self) -> bool {
        matches!(
            self,
            Self::RequirePrimary
                | Self::RequireReplica
                | Self::RouteOverride { .. }
                | Self::ShardOverride { .. }
        )
    }
}

impl Display for PolicyAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyDecision {
    pub policy_id: PolicyId,
    pub policy_version: PolicyVersion,
    pub action: PolicyAction,
    pub outcome: PolicyOutcome,
    pub hook_point: PolicyHookPoint,
    pub latency: Duration,
}

impl PolicyDecision {
    #[must_use]
    pub const fn new(
        policy_id: PolicyId,
        policy_version: PolicyVersion,
        action: PolicyAction,
        outcome: PolicyOutcome,
        hook_point: PolicyHookPoint,
        latency: Duration,
    ) -> Self {
        Self {
            policy_id,
            policy_version,
            action,
            outcome,
            hook_point,
            latency,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum PolicyContextField {
    Public { name: Arc<str>, value: Arc<str> },
    Secret { name: Arc<str>, value: Arc<str> },
}

impl PolicyContextField {
    #[must_use]
    pub fn public(name: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        Self::Public {
            name: name.into(),
            value: value.into(),
        }
    }

    #[must_use]
    pub fn secret(name: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        Self::Secret {
            name: name.into(),
            value: value.into(),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Public { name, .. } | Self::Secret { name, .. } => name,
        }
    }

    #[must_use]
    pub fn value(&self) -> &str {
        match self {
            Self::Public { value, .. } | Self::Secret { value, .. } => value,
        }
    }

    #[must_use]
    pub const fn is_secret(&self) -> bool {
        matches!(self, Self::Secret { .. })
    }
}

impl fmt::Debug for PolicyContextField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Public { name, value } => formatter
                .debug_struct("Public")
                .field("name", name)
                .field("value", value)
                .finish(),
            Self::Secret { name, .. } => formatter
                .debug_struct("Secret")
                .field("name", name)
                .field("value", &REDACTED_VALUE)
                .finish(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PolicyContext {
    fields: Vec<PolicyContextField>,
}

impl PolicyContext {
    #[must_use]
    pub fn new(fields: impl Into<Vec<PolicyContextField>>) -> Self {
        Self {
            fields: fields.into(),
        }
    }

    #[must_use]
    pub fn fields(&self) -> &[PolicyContextField] {
        &self.fields
    }

    pub fn push_field(&mut self, field: PolicyContextField) {
        self.fields.push(field);
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyAuditKind {
    Decision,
    Validation,
    Redaction,
    Error,
}

impl PolicyAuditKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Decision => "decision",
            Self::Validation => "validation",
            Self::Redaction => "redaction",
            Self::Error => "error",
        }
    }
}

impl Display for PolicyAuditKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyAuditEvent {
    pub kind: PolicyAuditKind,
    pub decision: PolicyDecision,
    pub context: PolicyContext,
}

impl PolicyAuditEvent {
    #[must_use]
    pub const fn new(
        kind: PolicyAuditKind,
        decision: PolicyDecision,
        context: PolicyContext,
    ) -> Self {
        Self {
            kind,
            decision,
            context,
        }
    }
}
