use std::{fmt, sync::Arc};

use thiserror::Error;

use crate::{
    policy::{PolicyDecisionReason, PolicyHookPoint, PolicyRouteTargetId, PolicyShardTargetId},
    routing::{BackendRole, QueryClass},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyRuleContext {
    pub database: Arc<str>,
    pub user: Arc<str>,
    pub application_name: Option<Arc<str>>,
    pub route: Option<Arc<str>>,
    pub shard: Option<Arc<str>>,
    pub backend_role: BackendRole,
    pub query_class: QueryClass,
    pub hook_point: PolicyHookPoint,
    pub read_only_transaction: bool,
    pub has_shard_key: bool,
    pub policy_tags: Vec<Arc<str>>,
}

impl PolicyRuleContext {
    #[must_use]
    pub fn new(
        database: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        application_name: Option<Arc<str>>,
        route: Option<Arc<str>>,
        shard: Option<Arc<str>>,
        backend_role: BackendRole,
        query_class: QueryClass,
        hook_point: PolicyHookPoint,
        read_only_transaction: bool,
        has_shard_key: bool,
        policy_tags: impl Into<Vec<Arc<str>>>,
    ) -> Self {
        Self {
            database: database.into(),
            user: user.into(),
            application_name: application_name.map(Into::into),
            route: route.map(Into::into),
            shard: shard.map(Into::into),
            backend_role,
            query_class,
            hook_point,
            read_only_transaction,
            has_shard_key,
            policy_tags: policy_tags.into(),
        }
    }
}

impl Default for PolicyRuleContext {
    fn default() -> Self {
        Self {
            database: Arc::from(""),
            user: Arc::from(""),
            application_name: None,
            route: None,
            shard: None,
            backend_role: BackendRole::Unknown,
            query_class: QueryClass::Unknown,
            hook_point: PolicyHookPoint::BeforeRouting,
            read_only_transaction: false,
            has_shard_key: false,
            policy_tags: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PolicyRuleMatch {
    pub databases: Vec<Arc<str>>,
    pub users: Vec<Arc<str>>,
    pub application_names: Vec<Arc<str>>,
    pub routes: Vec<Arc<str>>,
    pub shards: Vec<Arc<str>>,
    pub backend_roles: Vec<BackendRole>,
    pub query_classes: Vec<QueryClass>,
    pub hook_points: Vec<PolicyHookPoint>,
    pub read_only_transaction: Option<bool>,
    pub has_shard_key: Option<bool>,
    pub policy_tags: Vec<Arc<str>>,
    pub raw_sql_text: Vec<Arc<str>>,
}

impl PolicyRuleMatch {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn database(mut self, value: impl Into<Arc<str>>) -> Self {
        self.databases.push(value.into());
        self
    }

    #[must_use]
    pub fn user(mut self, value: impl Into<Arc<str>>) -> Self {
        self.users.push(value.into());
        self
    }

    #[must_use]
    pub fn application_name(mut self, value: impl Into<Arc<str>>) -> Self {
        self.application_names.push(value.into());
        self
    }

    #[must_use]
    pub fn route(mut self, value: impl Into<Arc<str>>) -> Self {
        self.routes.push(value.into());
        self
    }

    #[must_use]
    pub fn shard(mut self, value: impl Into<Arc<str>>) -> Self {
        self.shards.push(value.into());
        self
    }

    #[must_use]
    pub fn backend_role(mut self, value: BackendRole) -> Self {
        self.backend_roles.push(value);
        self
    }

    #[must_use]
    pub fn query_class(mut self, value: QueryClass) -> Self {
        self.query_classes.push(value);
        self
    }

    #[must_use]
    pub fn hook_point(mut self, value: PolicyHookPoint) -> Self {
        self.hook_points.push(value);
        self
    }

    #[must_use]
    pub fn read_only_transaction(mut self, value: bool) -> Self {
        self.read_only_transaction = Some(value);
        self
    }

    #[must_use]
    pub fn has_shard_key(mut self, value: bool) -> Self {
        self.has_shard_key = Some(value);
        self
    }

    #[must_use]
    pub fn policy_tag(mut self, value: impl Into<Arc<str>>) -> Self {
        self.policy_tags.push(value.into());
        self
    }

    #[must_use]
    pub fn raw_sql_text(mut self, value: impl Into<Arc<str>>) -> Self {
        self.raw_sql_text.push(value.into());
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.databases.is_empty()
            && self.users.is_empty()
            && self.application_names.is_empty()
            && self.routes.is_empty()
            && self.shards.is_empty()
            && self.backend_roles.is_empty()
            && self.query_classes.is_empty()
            && self.hook_points.is_empty()
            && self.read_only_transaction.is_none()
            && self.has_shard_key.is_none()
            && self.policy_tags.is_empty()
            && self.raw_sql_text.is_empty()
    }

    #[must_use]
    pub fn matches(&self, context: &PolicyRuleContext) -> bool {
        matches_text_field(&self.databases, context.database.as_ref())
            && matches_text_field(&self.users, context.user.as_ref())
            && matches_optional_text_field(&self.application_names, context.application_name.as_deref())
            && matches_optional_text_field(&self.routes, context.route.as_deref())
            && matches_optional_text_field(&self.shards, context.shard.as_deref())
            && matches_one_of(&self.backend_roles, context.backend_role)
            && matches_one_of(&self.query_classes, context.query_class)
            && matches_one_of(&self.hook_points, context.hook_point)
            && matches_optional_bool(self.read_only_transaction, context.read_only_transaction)
            && matches_optional_bool(self.has_shard_key, context.has_shard_key)
            && matches_policy_tags(&self.policy_tags, &context.policy_tags)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyRuleAction {
    Allow {
        audit: bool,
        terminal: bool,
    },
    Deny {
        reason: PolicyDecisionReason,
        audit: bool,
        terminal: bool,
    },
    RequirePrimary {
        audit: bool,
        terminal: bool,
    },
    RequireReplica {
        audit: bool,
        terminal: bool,
    },
    RouteOverride {
        target_id: PolicyRouteTargetId,
        audit: bool,
        terminal: bool,
    },
    ShardOverride {
        target_id: PolicyShardTargetId,
        audit: bool,
        terminal: bool,
    },
}

impl PolicyRuleAction {
    #[must_use]
    pub const fn allow() -> Self {
        Self::Allow {
            audit: false,
            terminal: true,
        }
    }

    #[must_use]
    pub const fn deny() -> Self {
        Self::Deny {
            reason: PolicyDecisionReason::PolicyDenied,
            audit: false,
            terminal: true,
        }
    }

    #[must_use]
    pub const fn deny_with_reason(reason: PolicyDecisionReason) -> Self {
        Self::Deny {
            reason,
            audit: false,
            terminal: true,
        }
    }

    #[must_use]
    pub const fn require_primary() -> Self {
        Self::RequirePrimary {
            audit: false,
            terminal: true,
        }
    }

    #[must_use]
    pub const fn require_replica() -> Self {
        Self::RequireReplica {
            audit: false,
            terminal: true,
        }
    }

    #[must_use]
    pub fn route_override(target_id: PolicyRouteTargetId) -> Self {
        Self::RouteOverride {
            target_id,
            audit: false,
            terminal: true,
        }
    }

    #[must_use]
    pub fn shard_override(target_id: PolicyShardTargetId) -> Self {
        Self::ShardOverride {
            target_id,
            audit: false,
            terminal: true,
        }
    }

    #[must_use]
    pub fn with_audit(self, audit: bool) -> Self {
        match self {
            Self::Allow { terminal, .. } => Self::Allow { audit, terminal },
            Self::Deny {
                reason, terminal, ..
            } => Self::Deny {
                reason,
                audit,
                terminal,
            },
            Self::RequirePrimary { terminal, .. } => Self::RequirePrimary { audit, terminal },
            Self::RequireReplica { terminal, .. } => Self::RequireReplica { audit, terminal },
            Self::RouteOverride {
                target_id, terminal, ..
            } => Self::RouteOverride {
                target_id,
                audit,
                terminal,
            },
            Self::ShardOverride {
                target_id, terminal, ..
            } => Self::ShardOverride {
                target_id,
                audit,
                terminal,
            },
        }
    }

    #[must_use]
    pub fn with_terminal(self, terminal: bool) -> Self {
        match self {
            Self::Allow { audit, .. } => Self::Allow { audit, terminal },
            Self::Deny {
                reason, audit, ..
            } => Self::Deny {
                reason,
                audit,
                terminal,
            },
            Self::RequirePrimary { audit, .. } => Self::RequirePrimary { audit, terminal },
            Self::RequireReplica { audit, .. } => Self::RequireReplica { audit, terminal },
            Self::RouteOverride {
                target_id, audit, ..
            } => Self::RouteOverride {
                target_id,
                audit,
                terminal,
            },
            Self::ShardOverride {
                target_id, audit, ..
            } => Self::ShardOverride {
                target_id,
                audit,
                terminal,
            },
        }
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        match self {
            Self::Allow { terminal, .. }
            | Self::Deny { terminal, .. }
            | Self::RequirePrimary { terminal, .. }
            | Self::RequireReplica { terminal, .. }
            | Self::RouteOverride { terminal, .. }
            | Self::ShardOverride { terminal, .. } => *terminal,
        }
    }

    #[must_use]
    pub const fn records_audit(&self) -> bool {
        match self {
            Self::Allow { audit, .. }
            | Self::Deny { audit, .. }
            | Self::RequirePrimary { audit, .. }
            | Self::RequireReplica { audit, .. }
            | Self::RouteOverride { audit, .. }
            | Self::ShardOverride { audit, .. } => *audit,
        }
    }

    #[must_use]
    pub const fn effect(&self) -> crate::policy::PolicyEffect {
        match self {
            Self::Allow { .. } => crate::policy::PolicyEffect::NoChange,
            Self::Deny { .. } => crate::policy::PolicyEffect::Deny,
            Self::RequirePrimary { .. } => crate::policy::PolicyEffect::RequirePrimary,
            Self::RequireReplica { .. } => crate::policy::PolicyEffect::RequireReplica,
            Self::RouteOverride { .. } => crate::policy::PolicyEffect::RouteOverride,
            Self::ShardOverride { .. } => crate::policy::PolicyEffect::ShardOverride,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Allow { .. } => "allow",
            Self::Deny { .. } => "deny",
            Self::RequirePrimary { .. } => "require_primary",
            Self::RequireReplica { .. } => "require_replica",
            Self::RouteOverride { .. } => "route_override",
            Self::ShardOverride { .. } => "shard_override",
        }
    }
}

impl fmt::Display for PolicyRuleAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyRule {
    pub id: Arc<str>,
    pub matches: PolicyRuleMatch,
    pub action: PolicyRuleAction,
}

impl PolicyRule {
    #[must_use]
    pub fn new(
        id: impl Into<Arc<str>>,
        matches: PolicyRuleMatch,
        action: PolicyRuleAction,
    ) -> Self {
        Self {
            id: id.into(),
            matches,
            action,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PolicyRuleSet {
    pub rules: Vec<PolicyRule>,
}

impl PolicyRuleSet {
    #[must_use]
    pub fn new(rules: impl Into<Vec<PolicyRule>>) -> Self {
        Self {
            rules: rules.into(),
        }
    }

    pub fn validate(&self) -> Result<(), PolicyRuleValidationError> {
        PolicyRuleValidator::default().validate(self)
    }

    #[must_use]
    pub fn evaluate(&self, context: &PolicyRuleContext) -> PolicyRuleEvalResult {
        let mut result = PolicyRuleEvalResult::default();

        for (rule_index, rule) in self.rules.iter().enumerate() {
            if !rule.matches.matches(context) {
                continue;
            }

            result.matched_rule_indices.push(rule_index);
            result.matched_rule_ids.push(rule.id.clone());
            result.applied_rule_index = Some(rule_index);
            result.applied_rule_id = Some(rule.id.clone());
            result.applied_action = Some(rule.action.clone());

            if rule.action.records_audit() {
                result.audit_rule_ids.push(rule.id.clone());
            }

            if rule.action.is_terminal() {
                result.terminal_rule_index = Some(rule_index);
                result.terminal_rule_id = Some(rule.id.clone());
                break;
            }
        }

        result
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PolicyRuleEvalResult {
    pub matched_rule_indices: Vec<usize>,
    pub matched_rule_ids: Vec<Arc<str>>,
    pub audit_rule_ids: Vec<Arc<str>>,
    pub applied_rule_index: Option<usize>,
    pub applied_rule_id: Option<Arc<str>>,
    pub applied_action: Option<PolicyRuleAction>,
    pub terminal_rule_index: Option<usize>,
    pub terminal_rule_id: Option<Arc<str>>,
}

impl PolicyRuleEvalResult {
    #[must_use]
    pub fn was_terminated(&self) -> bool {
        self.terminal_rule_index.is_some()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PolicyRuleValidator;

impl PolicyRuleValidator {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn validate(&self, rule_set: &PolicyRuleSet) -> Result<(), PolicyRuleValidationError> {
        if rule_set.rules.is_empty() {
            return Err(PolicyRuleValidationError::EmptyRuleSet);
        }

        let mut seen_rule_ids: Vec<Arc<str>> = Vec::with_capacity(rule_set.rules.len());

        for rule in &rule_set.rules {
            if rule.id.is_empty() {
                return Err(PolicyRuleValidationError::EmptyRuleId);
            }

            if seen_rule_ids.iter().any(|seen| seen.as_ref() == rule.id.as_ref()) {
                return Err(PolicyRuleValidationError::DuplicateRuleId {
                    rule_id: rule.id.clone(),
                });
            }
            seen_rule_ids.push(rule.id.clone());

            self.validate_rule(rule)?;
        }

        Ok(())
    }

    fn validate_rule(&self, rule: &PolicyRule) -> Result<(), PolicyRuleValidationError> {
        if rule.matches.is_empty() {
            return Err(PolicyRuleValidationError::EmptyMatch { rule_id: rule.id.clone() });
        }

        validate_text_values(&rule.id, "database", &rule.matches.databases)?;
        validate_text_values(&rule.id, "user", &rule.matches.users)?;
        validate_text_values(
            &rule.id,
            "application_name",
            &rule.matches.application_names,
        )?;
        validate_text_values(&rule.id, "route", &rule.matches.routes)?;
        validate_text_values(&rule.id, "shard", &rule.matches.shards)?;
        validate_text_values(&rule.id, "policy_tag", &rule.matches.policy_tags)?;
        if !rule.matches.raw_sql_text.is_empty() {
            return Err(PolicyRuleValidationError::RawSqlTextUnsupported);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PolicyRuleValidationError {
    #[error("policy rule set cannot be empty")]
    EmptyRuleSet,
    #[error("policy rule id cannot be empty")]
    EmptyRuleId,
    #[error("duplicate policy rule id '{rule_id}'")]
    DuplicateRuleId { rule_id: Arc<str> },
    #[error("policy rule '{rule_id}' must define at least one match criterion")]
    EmptyMatch { rule_id: Arc<str> },
    #[error("policy rule '{rule_id}' match field '{field}' cannot be empty")]
    EmptyMatchValue {
        rule_id: Arc<str>,
        field: &'static str,
    },
    #[error("raw sql text matches are not supported by policy rules")]
    RawSqlTextUnsupported,
}

fn validate_text_values(
    rule_id: &Arc<str>,
    field: &'static str,
    values: &[Arc<str>],
) -> Result<(), PolicyRuleValidationError> {
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(PolicyRuleValidationError::EmptyMatchValue {
            rule_id: rule_id.clone(),
            field,
        });
    }

    Ok(())
}

fn matches_text_field(values: &[Arc<str>], actual: &str) -> bool {
    values.is_empty() || values.iter().any(|value| value.as_ref() == actual)
}

fn matches_optional_text_field(values: &[Arc<str>], actual: Option<&str>) -> bool {
    match actual {
        Some(actual) => matches_text_field(values, actual),
        None => values.is_empty(),
    }
}

fn matches_one_of<T: PartialEq>(values: &[T], actual: T) -> bool
where
    T: Copy,
{
    values.is_empty() || values.iter().any(|value| *value == actual)
}

fn matches_optional_bool(expected: Option<bool>, actual: bool) -> bool {
    match expected {
        Some(expected) => expected == actual,
        None => true,
    }
}

fn matches_policy_tags(expected: &[Arc<str>], actual: &[Arc<str>]) -> bool {
    if expected.is_empty() {
        return true;
    }

    actual.iter().any(|actual_tag| {
        expected
            .iter()
            .any(|expected_tag| expected_tag.as_ref() == actual_tag.as_ref())
    })
}
