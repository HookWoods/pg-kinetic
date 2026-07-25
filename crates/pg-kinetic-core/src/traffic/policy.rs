use std::{convert::TryFrom, fmt, fmt::Display, str::FromStr, sync::Arc, time::Duration};

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
pub enum PolicyFailureMode {
    FailClosed,
    FailOpen,
    DisablePolicy,
}

impl PolicyFailureMode {
    #[must_use]
    pub const fn default_for_policy_mode(policy_mode: PolicyMode) -> Self {
        match policy_mode {
            PolicyMode::Enforce => Self::FailClosed,
            PolicyMode::Disabled | PolicyMode::DryRun => Self::DisablePolicy,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailClosed => "fail_closed",
            Self::FailOpen => "fail_open",
            Self::DisablePolicy => "disable_policy",
        }
    }

    #[must_use]
    pub const fn fallback_action(self) -> Option<PolicyAction> {
        match self {
            Self::FailClosed => Some(PolicyAction::deny()),
            Self::FailOpen => Some(PolicyAction::allow()),
            Self::DisablePolicy => None,
        }
    }
}

impl Display for PolicyFailureMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyPluginAbiVersion {
    V1,
}

impl PolicyPluginAbiVersion {
    #[must_use]
    pub const fn current() -> Self {
        Self::V1
    }

    #[must_use]
    pub const fn as_u16(self) -> u16 {
        match self {
            Self::V1 => 1,
        }
    }
}

impl Display for PolicyPluginAbiVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.as_u16())
    }
}

impl std::convert::TryFrom<u16> for PolicyPluginAbiVersion {
    type Error = PolicyPluginError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            other => Err(PolicyPluginError::unknown_abi_version(other)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyPluginAccessRequest {
    pub filesystem: bool,
    pub network: bool,
    pub secret: bool,
}

impl PolicyPluginAccessRequest {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            filesystem: false,
            network: false,
            secret: false,
        }
    }

    #[must_use]
    pub const fn new(filesystem: bool, network: bool, secret: bool) -> Self {
        Self {
            filesystem,
            network,
            secret,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyPluginInput {
    pub abi_version: PolicyPluginAbiVersion,
    pub policy_id: PolicyId,
    pub policy_version: PolicyVersion,
    pub hook_point: PolicyHookPoint,
    pub context: PolicyContext,
    pub requested_access: PolicyPluginAccessRequest,
}

impl PolicyPluginInput {
    pub fn new(
        abi_version: u16,
        policy_id: PolicyId,
        policy_version: PolicyVersion,
        hook_point: PolicyHookPoint,
        context: PolicyContext,
        requested_access: PolicyPluginAccessRequest,
    ) -> Result<Self, PolicyPluginError> {
        Ok(Self {
            abi_version: PolicyPluginAbiVersion::try_from(abi_version)?,
            policy_id,
            policy_version,
            hook_point,
            context,
            requested_access,
        })
    }

    #[must_use]
    pub fn rendered_len_bytes(&self) -> usize {
        self.abi_version.as_u16().to_string().len()
            + self.policy_id.as_str().len()
            + self.policy_version.as_u64().to_string().len()
            + self.hook_point.as_str().len()
            + self.context.rendered_len_bytes()
            + 3
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyPluginAction {
    Allow,
    Deny { reason: Arc<str> },
    RequirePrimary,
    RequireReplica,
    RouteOverride { target_id: PolicyRouteTargetId },
    ShardOverride { target_id: PolicyShardTargetId },
    Unsupported { name: Arc<str> },
}

impl PolicyPluginAction {
    #[must_use]
    pub const fn allow() -> Self {
        Self::Allow
    }

    #[must_use]
    pub fn deny(reason: impl Into<Arc<str>>) -> Self {
        Self::Deny {
            reason: reason.into(),
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
    pub fn unsupported(name: impl Into<Arc<str>>) -> Self {
        Self::Unsupported { name: name.into() }
    }

    #[must_use]
    pub const fn is_supported(&self) -> bool {
        !matches!(self, Self::Unsupported { .. })
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Allow => "allow",
            Self::Deny { .. } => "deny",
            Self::RequirePrimary => "require_primary",
            Self::RequireReplica => "require_replica",
            Self::RouteOverride { .. } => "route_override",
            Self::ShardOverride { .. } => "shard_override",
            Self::Unsupported { name } => name,
        }
    }

    #[must_use]
    pub fn rendered_len_bytes(&self) -> usize {
        match self {
            Self::Allow | Self::RequirePrimary | Self::RequireReplica => self.as_str().len(),
            Self::Deny { reason } => self.as_str().len() + reason.len(),
            Self::RouteOverride { target_id } => self.as_str().len() + target_id.as_str().len(),
            Self::ShardOverride { target_id } => self.as_str().len() + target_id.as_str().len(),
            Self::Unsupported { name } => name.len(),
        }
    }
}

impl Display for PolicyPluginAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyPluginOutput {
    pub abi_version: PolicyPluginAbiVersion,
    pub policy_id: PolicyId,
    pub policy_version: PolicyVersion,
    pub hook_point: PolicyHookPoint,
    pub action: PolicyPluginAction,
    pub outcome: PolicyOutcome,
}

impl PolicyPluginOutput {
    pub fn new(
        abi_version: u16,
        policy_id: PolicyId,
        policy_version: PolicyVersion,
        hook_point: PolicyHookPoint,
        action: PolicyPluginAction,
        outcome: PolicyOutcome,
    ) -> Result<Self, PolicyPluginError> {
        Ok(Self {
            abi_version: PolicyPluginAbiVersion::try_from(abi_version)?,
            policy_id,
            policy_version,
            hook_point,
            action,
            outcome,
        })
    }

    #[must_use]
    pub fn rendered_len_bytes(&self) -> usize {
        self.abi_version.as_u16().to_string().len()
            + self.policy_id.as_str().len()
            + self.policy_version.as_u64().to_string().len()
            + self.hook_point.as_str().len()
            + self.action.rendered_len_bytes()
            + self.outcome.as_str().len()
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct PolicyPluginError {
    code: PolicyPluginErrorCode,
    message: String,
}

impl PolicyPluginError {
    #[must_use]
    fn new(code: PolicyPluginErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> PolicyPluginErrorCode {
        self.code
    }

    #[must_use]
    pub const fn outcome(&self) -> PolicyOutcome {
        match self.code {
            PolicyPluginErrorCode::UnknownAbiVersion
            | PolicyPluginErrorCode::UnsupportedAction
            | PolicyPluginErrorCode::OutputValidationFailed => PolicyOutcome::Rejected,
            PolicyPluginErrorCode::InputTooLarge
            | PolicyPluginErrorCode::OutputTooLarge
            | PolicyPluginErrorCode::EvaluationTimeout
            | PolicyPluginErrorCode::FilesystemAccessDenied
            | PolicyPluginErrorCode::NetworkAccessDenied
            | PolicyPluginErrorCode::SecretAccessDenied => PolicyOutcome::Skipped,
        }
    }

    #[must_use]
    pub fn unknown_abi_version(version: u16) -> Self {
        Self::new(
            PolicyPluginErrorCode::UnknownAbiVersion,
            format!("unknown policy plugin ABI version {version}"),
        )
    }

    #[must_use]
    pub fn input_too_large(bytes: usize, max_bytes: usize) -> Self {
        Self::new(
            PolicyPluginErrorCode::InputTooLarge,
            format!("policy plugin input exceeds {max_bytes} bytes (got {bytes})"),
        )
    }

    #[must_use]
    pub fn output_too_large(bytes: usize, max_bytes: usize) -> Self {
        Self::new(
            PolicyPluginErrorCode::OutputTooLarge,
            format!("policy plugin output exceeds {max_bytes} bytes (got {bytes})"),
        )
    }

    #[must_use]
    pub fn evaluation_timeout(elapsed: Duration, max_duration: Duration) -> Self {
        Self::new(
            PolicyPluginErrorCode::EvaluationTimeout,
            format!("policy plugin evaluation exceeded {max_duration:?} (elapsed {elapsed:?})"),
        )
    }

    #[must_use]
    pub fn filesystem_access_denied() -> Self {
        Self::new(
            PolicyPluginErrorCode::FilesystemAccessDenied,
            "policy plugin filesystem access is not allowed",
        )
    }

    #[must_use]
    pub fn network_access_denied() -> Self {
        Self::new(
            PolicyPluginErrorCode::NetworkAccessDenied,
            "policy plugin network access is not allowed",
        )
    }

    #[must_use]
    pub fn secret_access_denied() -> Self {
        Self::new(
            PolicyPluginErrorCode::SecretAccessDenied,
            "policy plugin secret access is not allowed",
        )
    }

    #[must_use]
    pub fn unsupported_action(action: impl Into<Arc<str>>) -> Self {
        let action = action.into();
        Self::new(
            PolicyPluginErrorCode::UnsupportedAction,
            format!("policy plugin action '{action}' is not supported"),
        )
    }

    #[must_use]
    pub fn output_validation_failed(message: impl Into<String>) -> Self {
        Self::new(PolicyPluginErrorCode::OutputValidationFailed, message)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PolicyPluginErrorCode {
    UnknownAbiVersion,
    InputTooLarge,
    OutputTooLarge,
    EvaluationTimeout,
    FilesystemAccessDenied,
    NetworkAccessDenied,
    SecretAccessDenied,
    UnsupportedAction,
    OutputValidationFailed,
}

impl PolicyPluginErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownAbiVersion => "unknown_abi_version",
            Self::InputTooLarge => "input_too_large",
            Self::OutputTooLarge => "output_too_large",
            Self::EvaluationTimeout => "evaluation_timeout",
            Self::FilesystemAccessDenied => "filesystem_access_denied",
            Self::NetworkAccessDenied => "network_access_denied",
            Self::SecretAccessDenied => "secret_access_denied",
            Self::UnsupportedAction => "unsupported_action",
            Self::OutputValidationFailed => "output_validation_failed",
        }
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
    DryRun,
    Rejected,
    Skipped,
    Redacted,
}

impl PolicyOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::DryRun => "dry_run",
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
    pub fn audit_reason(&self, outcome: PolicyOutcome) -> Arc<str> {
        let reason = match outcome {
            PolicyOutcome::DryRun => match self {
                Self::Allow => "would_allow",
                Self::Deny { .. } => "would_deny",
                Self::RequirePrimary => "would_require_primary",
                Self::RequireReplica => "would_require_replica",
                Self::RouteOverride { .. } | Self::ShardOverride { .. } => "would_override",
            },
            _ => match self {
                Self::Allow => "allow",
                Self::Deny { reason, .. } => reason.as_str(),
                Self::RequirePrimary => "require_primary",
                Self::RequireReplica => "require_replica",
                Self::RouteOverride { .. } => "route_override",
                Self::ShardOverride { .. } => "shard_override",
            },
        };

        Arc::from(reason)
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
    pub fn rendered_value(&self) -> &str {
        match self {
            Self::Public { value, .. } => value,
            Self::Secret { .. } => REDACTED_VALUE,
        }
    }

    #[must_use]
    pub fn rendered_len_bytes(&self) -> usize {
        self.name().len() + 1 + self.rendered_value().len()
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

impl Display for PolicyContextField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}={}", self.name(), self.rendered_value())
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

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn push_field(&mut self, field: PolicyContextField) {
        self.fields.push(field);
    }

    #[must_use]
    pub fn rendered_len_bytes(&self) -> usize {
        match self.fields.split_first() {
            Some((first, rest)) => {
                first.rendered_len_bytes()
                    + rest
                        .iter()
                        .map(PolicyContextField::rendered_len_bytes)
                        .sum::<usize>()
                    + rest.len() * 2
            }
            None => 0,
        }
    }
}

impl Display for PolicyContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, field) in self.fields.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{field}")?;
        }
        Ok(())
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
    pub policy_id: PolicyId,
    pub policy_version: PolicyVersion,
    pub hook_point: PolicyHookPoint,
    pub action: PolicyAction,
    pub outcome: PolicyOutcome,
    pub reason: Option<Arc<str>>,
    pub route: Option<Arc<str>>,
    pub shard: Option<Arc<str>>,
    pub target_role: Option<Arc<str>>,
    pub decision: PolicyDecision,
    pub context: PolicyContext,
}

impl PolicyAuditEvent {
    #[must_use]
    pub fn new(kind: PolicyAuditKind, decision: PolicyDecision, context: PolicyContext) -> Self {
        let reason = Some(decision.action.audit_reason(decision.outcome));
        let route = context_field_value(&context, "route");
        let shard = context_field_value(&context, "shard");
        let target_role = context_field_value(&context, "backend_role");

        Self {
            kind,
            policy_id: decision.policy_id.clone(),
            policy_version: decision.policy_version,
            hook_point: decision.hook_point,
            action: decision.action.clone(),
            outcome: decision.outcome,
            reason,
            route,
            shard,
            target_role,
            decision,
            context,
        }
    }
}

fn context_field_value(context: &PolicyContext, field_name: &str) -> Option<Arc<str>> {
    context
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .map(|field| Arc::from(field.rendered_value()))
}
