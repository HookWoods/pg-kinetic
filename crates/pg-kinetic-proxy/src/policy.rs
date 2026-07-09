use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
    time::Duration,
};
#[cfg(feature = "policy-wasm")]
use std::time::Instant;

use thiserror::Error;

use pg_kinetic_core::{
    lsn::FreshnessStatus,
    policy::{
        PolicyAction, PolicyAuditEvent, PolicyAuditKind, PolicyContext, PolicyContextField,
        PolicyDecision, PolicyHookPoint, PolicyId, PolicyMode, PolicyOutcome, PolicyPluginAction,
        PolicyPluginError, PolicyPluginInput, PolicyPluginOutput, PolicyVersion,
    },
    routing::{BackendRole, QueryClass, RoutingDecision},
    session::TransactionAccessMode,
    sharding::ShardRouteDecision,
};

use crate::config::{InlinePolicyActionConfig, InlinePolicyConfig, PolicyConfig};
#[cfg(feature = "policy-wasm")]
use crate::policy_wasm::WasmPolicyEvaluator;
use crate::snapshot::SnapshotStore;

const REDACTED_VALUE: &str = "<redacted>";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyRuntime {
    policy_eval_timeout: Duration,
    policy_max_context_bytes: usize,
    policy_mode: PolicyMode,
    policy_audit_enabled: bool,
    policy_audit_sample_rate_bits: u64,
    policy_wasm_enabled: bool,
}

impl PolicyRuntime {
    #[must_use]
    pub fn new(policy_eval_timeout: Duration, policy_max_context_bytes: usize) -> Self {
        Self {
            policy_eval_timeout,
            policy_max_context_bytes,
            policy_mode: PolicyMode::Disabled,
            policy_audit_enabled: true,
            policy_audit_sample_rate_bits: 1.0f64.to_bits(),
            policy_wasm_enabled: false,
        }
    }

    #[must_use]
    pub fn from_config(config: &PolicyConfig) -> Self {
        Self::new(
            Duration::from_millis(config.policy_eval_timeout_ms),
            config.policy_max_context_bytes,
        )
        .with_policy_mode(config.policy_mode)
        .with_policy_audit_enabled(config.policy_audit.policy_audit_enabled)
        .with_policy_audit_sample_rate(config.policy_audit.policy_audit_sample_rate)
        .with_policy_wasm_enabled(config.policy_wasm.policy_wasm_enabled)
    }

    #[must_use]
    pub const fn policy_eval_timeout(&self) -> Duration {
        self.policy_eval_timeout
    }

    #[must_use]
    pub const fn policy_max_context_bytes(&self) -> usize {
        self.policy_max_context_bytes
    }

    #[must_use]
    pub const fn policy_mode(&self) -> PolicyMode {
        self.policy_mode
    }

    #[must_use]
    pub const fn policy_audit_enabled(&self) -> bool {
        self.policy_audit_enabled
    }

    #[must_use]
    pub const fn policy_wasm_enabled(&self) -> bool {
        self.policy_wasm_enabled
    }

    #[must_use]
    pub fn policy_audit_sample_rate(&self) -> f64 {
        f64::from_bits(self.policy_audit_sample_rate_bits)
    }

    #[must_use]
    pub fn context_builder(&self) -> PolicyContextBuilder {
        PolicyContextBuilder::new(self.policy_max_context_bytes)
    }

    #[must_use]
    pub fn plugin_host_limits(&self) -> PolicyPluginHostLimits {
        PolicyPluginHostLimits::new(
            self.policy_max_context_bytes,
            self.policy_max_context_bytes,
            self.policy_eval_timeout,
        )
    }

    #[must_use]
    pub fn build_audit_event(
        &self,
        kind: PolicyAuditKind,
        decision: PolicyDecision,
        context: PolicyContext,
    ) -> PolicyAuditEvent {
        PolicyAuditEvent::new(kind, decision, context)
    }

    #[must_use]
    pub fn build_audit_event_from_input(
        &self,
        kind: PolicyAuditKind,
        decision: PolicyDecision,
        input: &PolicyEvalInput,
    ) -> PolicyAuditEvent {
        let context = self.context_builder().build(input).context;
        self.build_audit_event(kind, decision, context)
    }

    #[must_use]
    pub fn should_sample_audit_event(&self, event: &PolicyAuditEvent) -> bool {
        if !self.policy_audit_enabled {
            return false;
        }

        let sample_rate = self.policy_audit_sample_rate();
        if sample_rate <= 0.0 {
            return false;
        }
        if sample_rate >= 1.0 {
            return true;
        }

        let mut hasher = DefaultHasher::new();
        event.kind.as_str().hash(&mut hasher);
        event.policy_id.as_str().hash(&mut hasher);
        event.policy_version.as_u64().hash(&mut hasher);
        event.hook_point.as_str().hash(&mut hasher);
        event.action.as_str().hash(&mut hasher);
        event.outcome.as_str().hash(&mut hasher);
        event.reason.as_deref().unwrap_or_default().hash(&mut hasher);
        event.route.as_deref().unwrap_or_default().hash(&mut hasher);
        event.shard.as_deref().unwrap_or_default().hash(&mut hasher);
        event.target_role.as_deref().unwrap_or_default().hash(&mut hasher);

        let sample = (hasher.finish() as f64) / (u64::MAX as f64);
        sample < sample_rate
    }

    #[must_use]
    pub fn record_audit_event(
        &self,
        snapshot_store: &SnapshotStore,
        event: &PolicyAuditEvent,
    ) -> bool {
        if !self.should_sample_audit_event(event) {
            return false;
        }

        snapshot_store.record_policy_audit_event(event.clone());
        crate::metrics::record_policy_audit_event(self.policy_mode, event);
        true
    }

    #[must_use]
    pub fn with_policy_mode(mut self, policy_mode: PolicyMode) -> Self {
        self.policy_mode = policy_mode;
        self
    }

    #[must_use]
    pub fn with_policy_audit_enabled(mut self, policy_audit_enabled: bool) -> Self {
        self.policy_audit_enabled = policy_audit_enabled;
        self
    }

    #[must_use]
    pub fn with_policy_audit_sample_rate(mut self, sample_rate: f64) -> Self {
        self.policy_audit_sample_rate_bits = clamp_sample_rate(sample_rate).to_bits();
        self
    }

    #[must_use]
    pub fn with_policy_wasm_enabled(mut self, policy_wasm_enabled: bool) -> Self {
        self.policy_wasm_enabled = policy_wasm_enabled;
        self
    }

    #[cfg(feature = "policy-wasm")]
    pub fn evaluate_wasm_policy(
        &self,
        rule: &InlinePolicyConfig,
        input: &PolicyEvalInput,
    ) -> Result<PolicyDecision, PolicyPluginError> {
        if !self.policy_wasm_enabled {
            return Err(PolicyPluginError::output_validation_failed(
                "wasm policies require policy_wasm_enabled to be true",
            ));
        }

        let module_path = match &rule.action {
            InlinePolicyActionConfig::Wasm { module_path } => module_path,
            _ => {
                return Err(PolicyPluginError::output_validation_failed(
                    "inline rule is not a wasm policy",
                ));
            }
        };

        let evaluator = WasmPolicyEvaluator::load(module_path, self.plugin_host_limits())?;
        let started_at = Instant::now();
        let result = evaluator.evaluate(
            rule.policy_id.clone(),
            PolicyVersion::new(1).expect("valid wasm policy version"),
            rule.hook_point,
            input,
            self.policy_mode,
        );
        let elapsed = started_at.elapsed();

        match &result {
            Ok(decision) => crate::metrics::record_policy_wasm_eval(
                "wasm",
                self.policy_mode,
                rule.hook_point,
                decision.outcome,
                None,
                elapsed,
            ),
            Err(error) => crate::metrics::record_policy_wasm_eval(
                "wasm",
                self.policy_mode,
                rule.hook_point,
                error.outcome(),
                Some(error.code().as_str()),
                elapsed,
            ),
        }

        result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolicyPluginHostLimits {
    max_input_bytes: usize,
    max_output_bytes: usize,
    max_evaluation_duration: Duration,
}

impl PolicyPluginHostLimits {
    #[must_use]
    pub const fn new(
        max_input_bytes: usize,
        max_output_bytes: usize,
        max_evaluation_duration: Duration,
    ) -> Self {
        Self {
            max_input_bytes,
            max_output_bytes,
            max_evaluation_duration,
        }
    }

    #[must_use]
    pub const fn max_input_bytes(&self) -> usize {
        self.max_input_bytes
    }

    #[must_use]
    pub const fn max_output_bytes(&self) -> usize {
        self.max_output_bytes
    }

    #[must_use]
    pub const fn max_evaluation_duration(&self) -> Duration {
        self.max_evaluation_duration
    }

    #[must_use]
    pub const fn filesystem_access_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn network_access_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn secret_access_allowed(&self) -> bool {
        false
    }

    pub fn validate_input(&self, input: &PolicyPluginInput) -> Result<(), PolicyPluginError> {
        if input.rendered_len_bytes() > self.max_input_bytes {
            return Err(PolicyPluginError::input_too_large(
                input.rendered_len_bytes(),
                self.max_input_bytes,
            ));
        }

        if input.requested_filesystem_access {
            return Err(PolicyPluginError::filesystem_access_denied());
        }
        if input.requested_network_access {
            return Err(PolicyPluginError::network_access_denied());
        }
        if input.requested_secret_access {
            return Err(PolicyPluginError::secret_access_denied());
        }

        Ok(())
    }

    pub fn validate_output(&self, output: &PolicyPluginOutput) -> Result<(), PolicyPluginError> {
        if output.rendered_len_bytes() > self.max_output_bytes {
            return Err(PolicyPluginError::output_too_large(
                output.rendered_len_bytes(),
                self.max_output_bytes,
            ));
        }

        Ok(())
    }

    pub fn validate_evaluation_duration(
        &self,
        elapsed: Duration,
    ) -> Result<(), PolicyPluginError> {
        if elapsed > self.max_evaluation_duration {
            return Err(PolicyPluginError::evaluation_timeout(
                elapsed,
                self.max_evaluation_duration,
            ));
        }

        Ok(())
    }

    pub fn validate_output_like_declarative_policy_output<R, S>(
        &self,
        output: &PolicyPluginOutput,
        active_routes: R,
        sharding_enabled: bool,
        active_shards: S,
    ) -> Result<(), PolicyPluginError>
    where
        R: IntoIterator,
        R::Item: AsRef<str>,
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        self.validate_output(output)?;

        let action = policy_plugin_action_to_inline_action(&output.action)?;
        let mut policy = PolicyConfig::default();
        policy.inline_rules = vec![InlinePolicyConfig {
            policy_id: output.policy_id.clone(),
            hook_point: output.hook_point,
            action,
        }];

        policy
            .validate_with_context(active_routes, sharding_enabled, active_shards)
            .map_err(PolicyPluginError::output_validation_failed)
    }
}

fn policy_plugin_action_to_inline_action(
    action: &PolicyPluginAction,
) -> Result<InlinePolicyActionConfig, PolicyPluginError> {
    match action {
        PolicyPluginAction::Allow => Ok(InlinePolicyActionConfig::Allow),
        PolicyPluginAction::Deny { reason } => Ok(InlinePolicyActionConfig::Deny {
            reason: reason.to_string(),
        }),
        PolicyPluginAction::RequirePrimary => Ok(InlinePolicyActionConfig::RequirePrimary),
        PolicyPluginAction::RequireReplica => Ok(InlinePolicyActionConfig::RequireReplica),
        PolicyPluginAction::RouteOverride { target_id } => {
            Ok(InlinePolicyActionConfig::RouteOverride {
                target_id: target_id.clone(),
            })
        }
        PolicyPluginAction::ShardOverride { target_id } => {
            Ok(InlinePolicyActionConfig::ShardOverride {
                target_id: target_id.clone(),
            })
        }
        PolicyPluginAction::Unsupported { name } => Err(PolicyPluginError::unsupported_action(
            name.clone(),
        )),
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PolicyGeneration(u64);

impl PolicyGeneration {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn initial() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<u64> for PolicyGeneration {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<PolicyGeneration> for u64 {
    fn from(value: PolicyGeneration) -> Self {
        value.as_u64()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyReloadErrorCode {
    InvalidPolicyConfiguration,
    RouteReferenceMissing,
    ShardReferenceMissing,
}

impl PolicyReloadErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidPolicyConfiguration => "invalid_policy_configuration",
            Self::RouteReferenceMissing => "route_reference_missing",
            Self::ShardReferenceMissing => "shard_reference_missing",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyReloadResult {
    pub success: bool,
    pub policy_generation_id: u64,
    pub error_code: Option<PolicyReloadErrorCode>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PolicySnapshot {
    pub generation: PolicyGeneration,
    pub config: PolicyConfig,
    pub runtime: PolicyRuntime,
}

impl PolicySnapshot {
    #[must_use]
    pub fn new(generation: PolicyGeneration, config: PolicyConfig) -> Self {
        let runtime = PolicyRuntime::from_config(&config);
        Self {
            generation,
            config,
            runtime,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyPreviewEvaluation {
    pub policy_mode: PolicyMode,
    pub action: PolicyAction,
    pub deny_reason: Option<Arc<str>>,
    pub audit_event: PolicyAuditEvent,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct PolicyPreviewError {
    pub code: String,
    pub message: String,
}

impl PolicyPreviewError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[must_use]
pub fn preview_policy(
    policy: &PolicyConfig,
    sharding_enabled: bool,
    input: &PolicyEvalInput,
) -> Result<PolicyPreviewEvaluation, PolicyPreviewError> {
    let policy_store = PolicyStore::new(policy.clone());
    let validation_result = policy_store.reload(
        policy,
        synthetic_routes(&policy.inline_rules, input),
        sharding_enabled,
        synthetic_shards(&policy.inline_rules, input),
    );
    if !validation_result.success {
        return Err(PolicyPreviewError::new(
            validation_result
                .error_code
                .map(|code| code.as_str())
                .unwrap_or("invalid_policy_configuration"),
            validation_result
                .error
                .unwrap_or_else(|| String::from("policy validation failed")),
        ));
    }

    let runtime = policy_store.runtime();
    let selected_rule = select_preview_rule(&policy.inline_rules, PolicyHookPoint::BeforeRouting);
    let (action, deny_reason, policy_id) = match selected_rule {
        Some(rule) => preview_action_from_rule(rule)?,
        None => (
            PolicyAction::allow(),
            None,
            PolicyId::new("policy-preview").expect("preview policy id"),
        ),
    };
    let decision = PolicyDecision::new(
        policy_id,
        PolicyVersion::new(1).expect("preview policy version"),
        action.clone(),
        PolicyOutcome::DryRun,
        PolicyHookPoint::BeforeRouting,
        Duration::from_millis(0),
    );
    let audit_event = runtime.build_audit_event_from_input(PolicyAuditKind::Decision, decision, input);
    crate::metrics::record_policy_decision(PolicyMode::DryRun, &audit_event);

    Ok(PolicyPreviewEvaluation {
        policy_mode: runtime.policy_mode(),
        action,
        deny_reason,
        audit_event,
    })
}

fn select_preview_rule<'a>(
    rules: &'a [InlinePolicyConfig],
    hook_point: PolicyHookPoint,
) -> Option<&'a InlinePolicyConfig> {
    rules.iter().find(|rule| rule.hook_point == hook_point)
}

fn preview_action_from_rule(
    rule: &InlinePolicyConfig,
) -> Result<(PolicyAction, Option<Arc<str>>, PolicyId), PolicyPreviewError> {
    let policy_id = rule.policy_id.clone();
    let action = match &rule.action {
        InlinePolicyActionConfig::Allow => PolicyAction::allow(),
        InlinePolicyActionConfig::Deny { reason } => {
            let deny_reason = Arc::from(reason.as_str());
            return Ok((PolicyAction::deny(), Some(deny_reason), policy_id));
        }
        InlinePolicyActionConfig::RequirePrimary => PolicyAction::require_primary(),
        InlinePolicyActionConfig::RequireReplica => PolicyAction::require_replica(),
        InlinePolicyActionConfig::RouteOverride { target_id } => {
            PolicyAction::route_override(target_id.clone())
        }
        InlinePolicyActionConfig::ShardOverride { target_id } => {
            PolicyAction::shard_override(target_id.clone())
        }
        InlinePolicyActionConfig::Wasm { module_path } => {
            return Err(PolicyPreviewError::new(
                "invalid_policy_configuration",
                format!(
                    "policy preview does not support wasm policy module {}",
                    module_path.display()
                ),
            ));
        }
    };

    Ok((action, None, policy_id))
}

fn synthetic_routes(rules: &[InlinePolicyConfig], input: &PolicyEvalInput) -> Vec<Arc<str>> {
    let mut routes = Vec::new();
    if let Some(route) = input.route.as_ref() {
        routes.push(route.clone());
    }
    for rule in rules {
        if let InlinePolicyActionConfig::RouteOverride { target_id } = &rule.action {
            routes.push(Arc::from(target_id.as_str()));
        }
    }
    routes
}

fn synthetic_shards(rules: &[InlinePolicyConfig], input: &PolicyEvalInput) -> Vec<Arc<str>> {
    let mut shards = Vec::new();
    if let Some(shard) = input.shard.as_ref() {
        shards.push(shard.clone());
    }
    for rule in rules {
        if let InlinePolicyActionConfig::ShardOverride { target_id } = &rule.action {
            shards.push(Arc::from(target_id.as_str()));
        }
    }
    shards
}

#[derive(Clone, Debug)]
pub struct PolicyStore {
    inner: Arc<PolicyStoreInner>,
}

#[derive(Debug)]
struct PolicyStoreInner {
    current: RwLock<Arc<PolicySnapshot>>,
}

impl Default for PolicyStore {
    fn default() -> Self {
        Self::new(PolicyConfig::default())
    }
}

impl PolicyStore {
    #[must_use]
    pub fn new(config: PolicyConfig) -> Self {
        Self {
            inner: Arc::new(PolicyStoreInner {
                current: RwLock::new(Arc::new(PolicySnapshot::new(
                    PolicyGeneration::initial(),
                    config,
                ))),
            }),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<PolicySnapshot> {
        self.inner
            .current
            .read()
            .expect("policy store poisoned")
            .clone()
    }

    #[must_use]
    pub fn generation(&self) -> PolicyGeneration {
        self.snapshot().generation
    }

    #[must_use]
    pub fn config(&self) -> PolicyConfig {
        self.snapshot().config.clone()
    }

    #[must_use]
    pub fn runtime(&self) -> PolicyRuntime {
        self.snapshot().runtime.clone()
    }

    #[must_use]
    pub fn policy_mode(&self) -> PolicyMode {
        self.runtime().policy_mode()
    }

    #[must_use]
    pub fn reload<R, S>(
        &self,
        next_config: &PolicyConfig,
        active_routes: R,
        sharding_enabled: bool,
        active_shards: S,
    ) -> PolicyReloadResult
    where
        R: IntoIterator,
        R::Item: AsRef<str>,
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        let mut current_snapshot = self
            .inner
            .current
            .write()
            .expect("policy store poisoned");
        let current_generation = current_snapshot.generation;
        let policy_source = policy_source_label(next_config);

        if let Err(error) =
            next_config.validate_with_context(active_routes, sharding_enabled, active_shards)
        {
            let result = PolicyReloadResult {
                success: false,
                policy_generation_id: current_generation.as_u64(),
                error_code: Some(policy_reload_error_code(&error)),
                error: Some(error),
            };
            crate::metrics::record_policy_reload(
                policy_source,
                next_config.policy_mode,
                false,
                result.error_code.as_ref().map(|code| code.as_str()),
            );
            return result;
        }

        let next_generation = current_generation.next();
        *current_snapshot = Arc::new(PolicySnapshot::new(next_generation, next_config.clone()));

        let result = PolicyReloadResult {
            success: true,
            policy_generation_id: next_generation.as_u64(),
            error_code: None,
            error: None,
        };
        crate::metrics::record_policy_reload(policy_source, next_config.policy_mode, true, None);
        result
    }
}

fn policy_reload_error_code(error: &str) -> PolicyReloadErrorCode {
    if error.contains("route override target") {
        PolicyReloadErrorCode::RouteReferenceMissing
    } else if error.contains("shard override target") {
        PolicyReloadErrorCode::ShardReferenceMissing
    } else {
        PolicyReloadErrorCode::InvalidPolicyConfiguration
    }
}

fn policy_source_label(config: &PolicyConfig) -> &'static str {
    let has_inline_rules = !config.inline_rules.is_empty();
    let has_policy_files = !config.policy_files.is_empty();
    let has_wasm_rules = config
        .inline_rules
        .iter()
        .any(|rule| matches!(rule.action, InlinePolicyActionConfig::Wasm { .. }));

    match (has_policy_files, has_wasm_rules, has_inline_rules) {
        (false, false, false) => "disabled",
        (true, false, false) => "file",
        (false, true, false) => "wasm",
        (false, false, true) => "inline",
        _ => "mixed",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyEvalInput {
    pub database: Arc<str>,
    pub user: Arc<str>,
    pub application_name: Option<Arc<str>>,
    pub route: Option<Arc<str>>,
    pub shard: Option<Arc<str>>,
    pub backend_role: BackendRole,
    pub query_class: QueryClass,
    pub transaction_mode: TransactionAccessMode,
    pub freshness_state: FreshnessStatus,
    pub routing_decision: Option<RoutingDecision>,
    pub shard_route_decision: Option<ShardRouteDecision>,
    pub password: Option<Arc<str>>,
    pub bind_values: Vec<Arc<str>>,
    pub tls_certificate_body: Option<Arc<str>>,
    pub raw_sql_text: Option<Arc<str>>,
    pub secrets: Vec<Arc<str>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyEvalOutput {
    pub context: PolicyContext,
    pub rendered_context: String,
    pub rendered_context_bytes: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyContextBuilder {
    policy_max_context_bytes: usize,
}

impl PolicyContextBuilder {
    #[must_use]
    pub const fn new(policy_max_context_bytes: usize) -> Self {
        Self {
            policy_max_context_bytes,
        }
    }

    #[must_use]
    pub fn build(&self, input: &PolicyEvalInput) -> PolicyEvalOutput {
        let fields = self.build_fields(input);
        let (fields, truncated) = fit_context_fields(fields, self.policy_max_context_bytes);
        let context = PolicyContext::new(fields);
        let rendered_context = context.to_string();
        let rendered_context_bytes = rendered_context.len();

        PolicyEvalOutput {
            context,
            rendered_context,
            rendered_context_bytes,
            truncated,
        }
    }

    fn build_fields(&self, input: &PolicyEvalInput) -> Vec<PolicyContextField> {
        let mut fields = vec![
            PolicyContextField::public("database", input.database.clone()),
            PolicyContextField::public("user", input.user.clone()),
            PolicyContextField::public(
                "application_name",
                input
                    .application_name
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| Arc::from("<none>")),
            ),
            PolicyContextField::public(
                "route",
                input
                    .route
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| Arc::from("<none>")),
            ),
            PolicyContextField::public(
                "shard",
                input
                    .shard
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| Arc::from("<none>")),
            ),
            PolicyContextField::public("backend_role", input.backend_role.as_str()),
            PolicyContextField::public("query_class", input.query_class.as_str()),
            PolicyContextField::public(
                "transaction_mode",
                transaction_mode_label(input.transaction_mode),
            ),
            PolicyContextField::public(
                "freshness_state",
                freshness_state_label(input.freshness_state),
            ),
        ];

        if let Some(routing_decision) = input.routing_decision.as_ref() {
            fields.push(PolicyContextField::public(
                "route_target_role",
                routing_decision.target_role.as_str(),
            ));
            fields.push(PolicyContextField::public(
                "routing_hint",
                routing_decision.hint.as_str(),
            ));
            fields.push(PolicyContextField::public(
                "routing_reason",
                routing_decision.reason.as_str(),
            ));
            fields.push(PolicyContextField::public(
                "fallback_policy",
                routing_decision.fallback_policy.as_str(),
            ));
            fields.push(PolicyContextField::public(
                "freshness_policy",
                routing_decision.freshness_requirement.as_str(),
            ));
        }

        if let Some(shard_route_decision) = input.shard_route_decision.as_ref() {
            fields.push(PolicyContextField::public(
                "multi_shard_policy",
                shard_route_decision.policy().as_str(),
            ));
            fields.push(PolicyContextField::public(
                "shard_route_reason",
                shard_route_decision.reason().as_str(),
            ));

            if let Some(route) = shard_route_decision.route() {
                fields.push(PolicyContextField::public(
                    "route_key",
                    route.target().route_key().metric_label(),
                ));
                fields.push(PolicyContextField::public(
                    "route_target_role",
                    route.target().backend_role().as_str(),
                ));
                fields.push(PolicyContextField::public(
                    "shard_target",
                    route.target().shard_id().as_str(),
                ));
            }
        }

        if input.password.is_some()
            || !input.bind_values.is_empty()
            || input.tls_certificate_body.is_some()
            || input.raw_sql_text.is_some()
            || !input.secrets.is_empty()
        {
            fields.push(PolicyContextField::public(
                "sensitive_inputs",
                redact_value("redacted"),
            ));
        }

        fields
    }
}

#[must_use]
pub fn redact_value(_value: impl AsRef<str>) -> Arc<str> {
    Arc::from(REDACTED_VALUE)
}

#[must_use]
pub fn redact_values(values: &[Arc<str>]) -> Vec<Arc<str>> {
    values.iter().map(|_| Arc::from(REDACTED_VALUE)).collect()
}

#[must_use]
pub fn redact_optional_value(value: Option<&str>) -> Option<Arc<str>> {
    value.map(|_| Arc::from(REDACTED_VALUE))
}

fn fit_context_fields(
    mut fields: Vec<PolicyContextField>,
    max_bytes: usize,
) -> (Vec<PolicyContextField>, bool) {
    if max_bytes == 0 || fields.is_empty() {
        return (Vec::new(), !fields.is_empty());
    }

    let mut truncated = false;

    while rendered_fields_len_bytes(&fields) > max_bytes && fields.len() > 1 {
        fields.pop();
        truncated = true;
    }

    while rendered_fields_len_bytes(&fields) > max_bytes {
        let last_index = fields.len() - 1;
        let available_bytes = max_bytes.saturating_sub(rendered_prefix_len_bytes(&fields, last_index));
        let field_name = fields[last_index].name().to_owned();
        let field_name_len = field_name.len() + 1;
        if available_bytes <= field_name_len {
            if fields.len() == 1 {
                fields.clear();
                return (fields, true);
            }

            fields.pop();
            truncated = true;
            continue;
        }

        let max_value_len = available_bytes - field_name_len;
        let rendered_value = fields[last_index].rendered_value();
        if rendered_value.len() > max_value_len {
            let truncated_value = truncate_to_byte_len(rendered_value, max_value_len);
            fields[last_index] = PolicyContextField::public(field_name, truncated_value);
            truncated = true;
        } else if fields.len() > 1 {
            fields.pop();
            truncated = true;
        } else {
            break;
        }
    }

    (fields, truncated)
}

fn rendered_fields_len_bytes(fields: &[PolicyContextField]) -> usize {
    match fields.split_first() {
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

fn rendered_prefix_len_bytes(fields: &[PolicyContextField], end_index: usize) -> usize {
    let prefix = &fields[..end_index];
    rendered_fields_len_bytes(prefix) + if prefix.is_empty() { 0 } else { 2 }
}

fn truncate_to_byte_len(value: &str, max_bytes: usize) -> Arc<str> {
    if value.len() <= max_bytes {
        return Arc::from(value);
    }

    let mut end = 0;
    for (index, character) in value.char_indices() {
        let next = index + character.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }

    Arc::from(&value[..end])
}

fn transaction_mode_label(mode: TransactionAccessMode) -> &'static str {
    match mode {
        TransactionAccessMode::ReadOnly => "read_only",
        TransactionAccessMode::ReadWrite => "read_write",
    }
}

fn freshness_state_label(state: FreshnessStatus) -> &'static str {
    match state {
        FreshnessStatus::Satisfied => "satisfied",
        FreshnessStatus::Waiting => "waiting",
        FreshnessStatus::Stale => "stale",
        FreshnessStatus::Unknown => "unknown",
        FreshnessStatus::Unavailable => "unavailable",
    }
}

fn clamp_sample_rate(sample_rate: f64) -> f64 {
    if sample_rate.is_nan() {
        0.0
    } else {
        sample_rate.clamp(0.0, 1.0)
    }
}
