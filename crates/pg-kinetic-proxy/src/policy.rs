use std::{sync::Arc, time::Duration};

use pg_kinetic_core::{
    lsn::FreshnessStatus,
    policy::{PolicyContext, PolicyContextField},
    routing::{BackendRole, QueryClass, RoutingDecision},
    session::TransactionAccessMode,
    sharding::ShardRouteDecision,
};

use crate::config::PolicyConfig;

const REDACTED_VALUE: &str = "<redacted>";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyRuntime {
    policy_eval_timeout: Duration,
    policy_max_context_bytes: usize,
}

impl PolicyRuntime {
    #[must_use]
    pub fn new(policy_eval_timeout: Duration, policy_max_context_bytes: usize) -> Self {
        Self {
            policy_eval_timeout,
            policy_max_context_bytes,
        }
    }

    #[must_use]
    pub fn from_config(config: &PolicyConfig) -> Self {
        Self::new(
            Duration::from_millis(config.policy_eval_timeout_ms),
            config.policy_max_context_bytes,
        )
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
    pub fn context_builder(&self) -> PolicyContextBuilder {
        PolicyContextBuilder::new(self.policy_max_context_bytes)
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
