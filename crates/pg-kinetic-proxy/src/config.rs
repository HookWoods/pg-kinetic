use std::{
    collections::HashSet, fmt, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration,
};

use clap::{Args, Parser, ValueEnum};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use pg_kinetic_core::{
    adaptive::{AdaptiveMode, TunableKnob},
    cleanup::PoolMode as CorePoolMode,
    constants::{BufferDefaults, QosDefaults, TimeoutDefaults},
    mirror::MirrorMode,
    policy::{PolicyHookPoint, PolicyId, PolicyMode, PolicyRouteTargetId, PolicyShardTargetId},
    recovery::RecoveryMode,
    routing::{FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
    runtime::{NodeId, RuntimeEngine},
    security::{
        AuthMode as CoreAuthMode, BackendTlsMode as CoreBackendTlsMode,
        ClientTlsMode as CoreClientTlsMode,
    },
    sharding::ShardId,
};

#[cfg(feature = "policy-wasm")]
use crate::policy_wasm::WasmPolicyEvaluator;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum ClientTlsMode {
    Disable,
    Allow,
    Require,
    VerifyClient,
}

impl ClientTlsMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Allow => "allow",
            Self::Require => "require",
            Self::VerifyClient => "verify_client",
        }
    }
}

impl From<ClientTlsMode> for CoreClientTlsMode {
    fn from(mode: ClientTlsMode) -> Self {
        match mode {
            ClientTlsMode::Disable => Self::Disable,
            ClientTlsMode::Allow => Self::Allow,
            ClientTlsMode::Require => Self::Require,
            ClientTlsMode::VerifyClient => Self::VerifyClient,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum BackendTlsMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl BackendTlsMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify_ca",
            Self::VerifyFull => "verify_full",
        }
    }
}

impl From<BackendTlsMode> for CoreBackendTlsMode {
    fn from(mode: BackendTlsMode) -> Self {
        match mode {
            BackendTlsMode::Disable => Self::Disable,
            BackendTlsMode::Prefer => Self::Prefer,
            BackendTlsMode::Require => Self::Require,
            BackendTlsMode::VerifyCa => Self::VerifyCa,
            BackendTlsMode::VerifyFull => Self::VerifyFull,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum AuthMode {
    PassThrough,
    Trust,
    #[serde(rename = "scram_sha_256", alias = "scram_sha256")]
    #[value(name = "scram_sha_256", alias = "scram_sha256")]
    ScramSha256,
    Md5,
}

impl AuthMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PassThrough => "pass_through",
            Self::Trust => "trust",
            Self::ScramSha256 => "scram_sha_256",
            Self::Md5 => "md5",
        }
    }
}

impl From<AuthMode> for CoreAuthMode {
    fn from(mode: AuthMode) -> Self {
        match mode {
            AuthMode::PassThrough => Self::PassThrough,
            AuthMode::Trust => Self::Trust,
            AuthMode::ScramSha256 => Self::ScramSha256,
            AuthMode::Md5 => Self::Md5,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum PoolMode {
    #[default]
    Transaction,
    Session,
}

impl From<PoolMode> for CorePoolMode {
    fn from(mode: PoolMode) -> Self {
        match mode {
            PoolMode::Transaction => Self::Transaction,
            PoolMode::Session => Self::Session,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum AuthFailureMessageMode {
    Generic,
    Detailed,
}

impl AuthFailureMessageMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Detailed => "detailed",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Parser, Serialize)]
#[serde(default)]
#[command(name = "pg-kinetic")]
#[command(about = "Low-overhead PostgreSQL wire proxy")]
pub struct Config {
    #[command(flatten)]
    pub connection: ConnectionConfig,

    #[arg(skip)]
    pub routes: Vec<RouteConfig>,

    #[serde(deserialize_with = "deserialize_pools")]
    #[arg(skip)]
    pub pools: Vec<PoolConfig>,

    #[command(flatten)]
    pub runtime: RuntimeConfig,

    #[command(flatten)]
    pub capacity: CapacityConfig,

    #[command(flatten)]
    pub pool_lifecycle: PoolLifecycleConfig,

    #[command(flatten)]
    pub performance: PerformanceConfig,

    #[command(flatten)]
    pub qos: QosConfig,

    #[command(flatten)]
    pub admin: AdminConfig,

    #[command(flatten)]
    pub observability: ObservabilityConfig,

    #[command(flatten)]
    pub tls: TlsConfig,

    #[command(flatten)]
    pub auth: AuthConfig,

    #[command(flatten)]
    pub reload: ReloadConfig,

    #[command(flatten)]
    pub drain: DrainConfig,

    #[command(flatten)]
    pub health: HealthConfig,

    #[command(flatten)]
    pub socket: SocketConfig,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct RuntimeConfig {
    #[command(flatten)]
    pub lifecycle: LifecycleConfig,

    #[command(flatten)]
    pub node: NodeConfig,

    #[command(flatten)]
    pub engine: RuntimeEngineConfig,

    #[command(flatten)]
    pub production: ProductionConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct MirrorConfig {
    #[arg(
        long = "mirror-enabled",
        env = "PG_KINETIC_MIRROR_ENABLED",
        default_value_t = false
    )]
    pub mirroring_enabled: bool,

    #[arg(
        long = "mirror-mode",
        env = "PG_KINETIC_MIRROR_MODE",
        value_parser = parse_mirror_mode,
        default_value = "off"
    )]
    #[serde(
        default = "default_mirror_mode",
        deserialize_with = "deserialize_mirror_mode",
        serialize_with = "serialize_mirror_mode"
    )]
    pub mirror_mode: MirrorMode,

    #[arg(
        long = "mirror-timeout-ms",
        env = "PG_KINETIC_MIRROR_TIMEOUT_MS",
        default_value_t = 100
    )]
    pub mirror_timeout_ms: u64,

    #[arg(
        long = "mirror-max-in-flight",
        env = "PG_KINETIC_MIRROR_MAX_IN_FLIGHT",
        default_value_t = 128
    )]
    pub mirror_max_in_flight: usize,

    #[command(flatten)]
    pub target: MirrorTargetConfig,

    #[command(flatten)]
    pub sampling: MirrorSamplingConfig,

    #[command(flatten)]
    pub safety: MirrorSafetyConfig,
}

impl MirrorConfig {
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.mirroring_enabled && self.mirror_mode.is_enabled()
    }

    pub fn validate(&self, production_target: SocketAddr) -> Result<(), String> {
        if self.is_enabled() && !self.target.is_configured() {
            return Err(String::from("mirror target must be explicitly configured"));
        }

        self.target.validate_against(
            production_target,
            self.safety.mirror_require_isolated_target,
        )
    }
}

impl Default for MirrorConfig {
    fn default() -> Self {
        Self {
            mirroring_enabled: false,
            mirror_mode: MirrorMode::Off,
            mirror_timeout_ms: 100,
            mirror_max_in_flight: 128,
            target: MirrorTargetConfig::default(),
            sampling: MirrorSamplingConfig::default(),
            safety: MirrorSafetyConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct MirrorTargetConfig {
    #[arg(
        long = "mirror-target-address",
        env = "PG_KINETIC_MIRROR_TARGET_ADDRESS"
    )]
    pub address: Option<SocketAddr>,

    #[arg(
        long = "mirror-target-isolated",
        env = "PG_KINETIC_MIRROR_TARGET_ISOLATED",
        default_value_t = false
    )]
    pub isolated: bool,
}

impl MirrorTargetConfig {
    #[must_use]
    pub const fn is_configured(&self) -> bool {
        self.address.is_some()
    }

    pub fn validate_against(
        &self,
        production_target: SocketAddr,
        require_isolated_target: bool,
    ) -> Result<(), String> {
        match self.address {
            Some(address)
                if require_isolated_target && !self.isolated && address == production_target =>
            {
                Err(String::from(
                    "mirror target must be marked isolated when it matches the production target",
                ))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct MirrorSamplingConfig {
    #[arg(
        long = "mirror-sample-rate",
        env = "PG_KINETIC_MIRROR_SAMPLE_RATE",
        default_value_t = 0.0
    )]
    pub mirror_sample_rate: f64,
}

impl MirrorSamplingConfig {
    #[must_use]
    pub fn sample_rate(&self) -> f64 {
        pg_kinetic_core::mirror::MirrorSample::new(self.mirror_sample_rate).rate()
    }
}

impl Default for MirrorSamplingConfig {
    fn default() -> Self {
        Self {
            mirror_sample_rate: 0.0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct MirrorSafetyConfig {
    #[arg(
        long = "mirror-writes-enabled",
        env = "PG_KINETIC_MIRROR_WRITES_ENABLED",
        default_value_t = false
    )]
    pub mirror_writes_enabled: bool,

    #[arg(
        long = "mirror-transactions-enabled",
        env = "PG_KINETIC_MIRROR_TRANSACTIONS_ENABLED",
        default_value_t = false
    )]
    pub mirror_transactions_enabled: bool,

    #[arg(
        long = "mirror-copy-enabled",
        env = "PG_KINETIC_MIRROR_COPY_ENABLED",
        default_value_t = false
    )]
    pub mirror_copy_enabled: bool,

    #[arg(
        long = "mirror-listen-notify-enabled",
        env = "PG_KINETIC_MIRROR_LISTEN_NOTIFY_ENABLED",
        default_value_t = false
    )]
    pub mirror_listen_notify_enabled: bool,

    #[arg(
        long = "mirror-temp-table-enabled",
        env = "PG_KINETIC_MIRROR_TEMP_TABLE_ENABLED",
        default_value_t = false
    )]
    pub mirror_temp_table_enabled: bool,

    #[arg(
        long = "mirror-session-mutation-enabled",
        env = "PG_KINETIC_MIRROR_SESSION_MUTATION_ENABLED",
        default_value_t = false
    )]
    pub mirror_session_mutation_enabled: bool,

    #[arg(
        long = "mirror-require-isolated-target",
        env = "PG_KINETIC_MIRROR_REQUIRE_ISOLATED_TARGET",
        default_value_t = true
    )]
    pub mirror_require_isolated_target: bool,
}

impl Default for MirrorSafetyConfig {
    fn default() -> Self {
        Self {
            mirror_writes_enabled: false,
            mirror_transactions_enabled: false,
            mirror_copy_enabled: false,
            mirror_listen_notify_enabled: false,
            mirror_temp_table_enabled: false,
            mirror_session_mutation_enabled: false,
            mirror_require_isolated_target: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct LifecycleConfig {
    #[arg(long, env = "PG_KINETIC_STARTUP_GRACE_MS", default_value_t = 30_000)]
    pub startup_grace_ms: u64,

    #[arg(long, env = "PG_KINETIC_SHUTDOWN_GRACE_MS", default_value_t = 30_000)]
    pub shutdown_grace_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_READINESS_FAIL_DURING_DRAIN",
        action = clap::ArgAction::Set,
        default_value_t = true
    )]
    pub readiness_fail_during_drain: bool,

    #[arg(
        long,
        env = "PG_KINETIC_PRE_STOP_DRAIN_ENABLED",
        action = clap::ArgAction::Set,
        default_value_t = true
    )]
    pub pre_stop_drain_enabled: bool,

    #[arg(
        long,
        env = "PG_KINETIC_PRE_STOP_DRAIN_ENDPOINT",
        default_value = "/drain"
    )]
    pub pre_stop_drain_endpoint: String,

    #[arg(
        long,
        env = "PG_KINETIC_STARTUP_BACKEND_CHECKS_ENABLED",
        action = clap::ArgAction::Set,
        default_value_t = true
    )]
    pub startup_backend_checks_enabled: bool,

    #[arg(
        long,
        env = "PG_KINETIC_TERMINATION_GRACE_PERIOD_SECONDS",
        default_value_t = 65
    )]
    pub termination_grace_period_seconds: u64,
}

impl LifecycleConfig {
    #[must_use]
    pub const fn startup_grace(&self) -> Duration {
        Duration::from_millis(self.startup_grace_ms)
    }

    #[must_use]
    pub const fn shutdown_grace(&self) -> Duration {
        Duration::from_millis(self.shutdown_grace_ms)
    }

    #[must_use]
    pub const fn termination_grace_period(&self) -> Duration {
        Duration::from_secs(self.termination_grace_period_seconds)
    }
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            startup_grace_ms: 30_000,
            shutdown_grace_ms: 30_000,
            readiness_fail_during_drain: true,
            pre_stop_drain_enabled: true,
            pre_stop_drain_endpoint: String::from("/drain"),
            startup_backend_checks_enabled: true,
            termination_grace_period_seconds: 65,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct NodeConfig {
    #[arg(
        long,
        env = "PG_KINETIC_NODE_ID",
        default_value_t = default_node_id()
    )]
    #[serde(
        default = "default_node_id",
        deserialize_with = "deserialize_node_id",
        serialize_with = "serialize_node_id"
    )]
    pub node_id: NodeId,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct RuntimeEngineConfig {
    #[arg(
        long,
        env = "PG_KINETIC_RUNTIME_ENGINE",
        value_parser = parse_runtime_engine,
        default_value = "thread_per_core"
    )]
    #[serde(
        default = "default_runtime_engine",
        deserialize_with = "deserialize_runtime_engine",
        serialize_with = "serialize_runtime_engine"
    )]
    pub runtime_engine: RuntimeEngine,

    #[arg(
        long,
        env = "PG_KINETIC_EXPERIMENTAL_RUNTIME_ENABLED",
        default_value_t = false
    )]
    pub experimental_runtime_enabled: bool,

    #[arg(long, env = "PG_KINETIC_RUNTIME_SHARDS")]
    pub runtime_shards: Option<usize>,
}

impl RuntimeEngineConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.runtime_engine.is_experimental() && !self.experimental_runtime_enabled {
            return Err(format!(
                "runtime engine '{}' requires experimental_runtime_enabled = true",
                self.runtime_engine
            ));
        }

        if self.runtime_shards == Some(0) {
            return Err(String::from("runtime_shards must be greater than zero"));
        }

        Ok(())
    }
}

impl Default for RuntimeEngineConfig {
    fn default() -> Self {
        Self {
            runtime_engine: RuntimeEngine::TokioDefault,
            experimental_runtime_enabled: false,
            runtime_shards: None,
        }
    }
}

impl<'de> Deserialize<'de> for RuntimeEngineConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(default)]
        struct RawRuntimeEngineConfig {
            #[serde(
                default = "default_runtime_engine",
                deserialize_with = "deserialize_runtime_engine"
            )]
            runtime_engine: RuntimeEngine,
            experimental_runtime_enabled: bool,
            runtime_shards: Option<usize>,
        }

        impl Default for RawRuntimeEngineConfig {
            fn default() -> Self {
                Self {
                    runtime_engine: default_runtime_engine(),
                    experimental_runtime_enabled: false,
                    runtime_shards: None,
                }
            }
        }

        let raw = RawRuntimeEngineConfig::deserialize(deserializer)?;
        let config = Self {
            runtime_engine: raw.runtime_engine,
            experimental_runtime_enabled: raw.experimental_runtime_enabled,
            runtime_shards: raw.runtime_shards,
        };
        config.validate().map_err(serde::de::Error::custom)?;
        Ok(config)
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct ProductionConfig {
    #[arg(
        long,
        env = "PG_KINETIC_CONTROL_PLANE_ENABLED",
        default_value_t = false
    )]
    pub control_plane_enabled: bool,

    #[arg(long, env = "PG_KINETIC_MIRRORING_ENABLED", default_value_t = false)]
    pub mirroring_enabled: bool,

    #[arg(long, env = "PG_KINETIC_ADAPTIVE_ENABLED", default_value_t = false)]
    pub adaptive_enabled: bool,

    #[command(flatten)]
    #[serde(flatten)]
    pub adaptive: AdaptiveConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct AdaptiveConfig {
    #[arg(
        long = "adaptive-mode",
        env = "PG_KINETIC_ADAPTIVE_MODE",
        value_parser = parse_adaptive_mode,
        default_value = "recommend"
    )]
    #[serde(
        default = "default_adaptive_mode",
        deserialize_with = "deserialize_adaptive_mode",
        serialize_with = "serialize_adaptive_mode"
    )]
    pub adaptive_mode: AdaptiveMode,

    #[arg(
        long = "adaptive-window-ms",
        env = "PG_KINETIC_ADAPTIVE_WINDOW_MS",
        default_value_t = 60_000
    )]
    pub adaptive_window_ms: u64,

    #[arg(
        long = "adaptive-min-confidence",
        env = "PG_KINETIC_ADAPTIVE_MIN_CONFIDENCE",
        default_value_t = 0.8
    )]
    pub adaptive_min_confidence: f64,

    #[command(flatten)]
    #[serde(flatten)]
    pub apply: AdaptiveApplyConfig,

    #[command(flatten)]
    #[serde(flatten)]
    pub guardrail: AdaptiveGuardrailConfig,
}

impl AdaptiveConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.adaptive_min_confidence.is_finite()
            || !(0.0..=1.0).contains(&self.adaptive_min_confidence)
        {
            return Err(String::from(
                "adaptive_min_confidence must be between 0.0 and 1.0",
            ));
        }

        if self.adaptive_window_ms == 0 {
            return Err(String::from("adaptive_window_ms must be greater than zero"));
        }

        self.guardrail.validate()?;
        self.apply.validate()?;

        if self.adaptive_mode.is_apply() {
            if !self.apply.adaptive_apply_enabled {
                return Err(String::from(
                    "adaptive mode 'apply' requires adaptive_apply_enabled = true",
                ));
            }

            if self.apply.adaptive_apply_allowlist.is_empty() {
                return Err(String::from(
                    "adaptive mode 'apply' requires an explicit allowlist of tunable knobs",
                ));
            }
        }

        Ok(())
    }

    #[must_use]
    pub const fn recommendation_window(&self) -> Duration {
        Duration::from_millis(self.adaptive_window_ms)
    }

    pub fn evaluate(
        &self,
        recommendation: &pg_kinetic_core::adaptive::AdaptiveRecommendation,
    ) -> Result<
        pg_kinetic_core::adaptive::AdaptiveOutcome,
        pg_kinetic_core::adaptive::AdaptiveApplyError,
    > {
        if recommendation.confidence() < self.adaptive_min_confidence {
            return Err(pg_kinetic_core::adaptive::AdaptiveApplyError::new(
                pg_kinetic_core::adaptive::AdaptiveGuardrail::ConfidenceFloor,
                format!(
                    "adaptive recommendation confidence {:.3} is below the minimum {:.3}",
                    recommendation.confidence(),
                    self.adaptive_min_confidence
                ),
            ));
        }

        if self.adaptive_mode.is_apply() {
            self.apply.evaluate(recommendation, &self.guardrail)
        } else {
            Ok(pg_kinetic_core::adaptive::AdaptiveOutcome::Recommended)
        }
    }
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            adaptive_mode: AdaptiveMode::Recommend,
            adaptive_window_ms: 60_000,
            adaptive_min_confidence: 0.8,
            apply: AdaptiveApplyConfig::default(),
            guardrail: AdaptiveGuardrailConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct AdaptiveApplyConfig {
    #[arg(
        long = "adaptive-apply-enabled",
        env = "PG_KINETIC_ADAPTIVE_APPLY_ENABLED",
        default_value_t = false
    )]
    pub adaptive_apply_enabled: bool,

    #[arg(
        long = "adaptive-apply-allowlist",
        env = "PG_KINETIC_ADAPTIVE_APPLY_ALLOWLIST",
        value_parser = parse_tunable_knob,
        value_delimiter = ','
    )]
    #[serde(
        default,
        deserialize_with = "deserialize_tunable_knob_list",
        serialize_with = "serialize_tunable_knob_list"
    )]
    pub adaptive_apply_allowlist: Vec<TunableKnob>,
}

impl AdaptiveApplyConfig {
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.adaptive_apply_enabled
    }

    #[must_use]
    pub fn allows(&self, knob: TunableKnob) -> bool {
        self.adaptive_apply_allowlist.contains(&knob)
    }

    pub fn validate(&self) -> Result<(), String> {
        let mut unique_knobs = HashSet::new();
        if self
            .adaptive_apply_allowlist
            .iter()
            .any(|knob| !unique_knobs.insert(*knob))
        {
            return Err(String::from(
                "adaptive_apply_allowlist cannot contain duplicate knobs",
            ));
        }

        Ok(())
    }

    pub fn evaluate(
        &self,
        recommendation: &pg_kinetic_core::adaptive::AdaptiveRecommendation,
        guardrail: &AdaptiveGuardrailConfig,
    ) -> Result<
        pg_kinetic_core::adaptive::AdaptiveOutcome,
        pg_kinetic_core::adaptive::AdaptiveApplyError,
    > {
        if !self.adaptive_apply_enabled {
            return Ok(pg_kinetic_core::adaptive::AdaptiveOutcome::Skipped);
        }

        if !self.allows(recommendation.knob()) {
            return Err(pg_kinetic_core::adaptive::AdaptiveApplyError::new(
                pg_kinetic_core::adaptive::AdaptiveGuardrail::Allowlist,
                format!(
                    "adaptive knob '{}' is not on the apply allowlist",
                    recommendation.knob()
                ),
            ));
        }

        match recommendation.safety_bound() {
            pg_kinetic_core::adaptive::TuningBound::Unbounded => {
                Err(pg_kinetic_core::adaptive::AdaptiveApplyError::new(
                    pg_kinetic_core::adaptive::AdaptiveGuardrail::UnboundedChange,
                    "adaptive apply rejected an unbounded change",
                ))
            }
            pg_kinetic_core::adaptive::TuningBound::Percent(change_percent)
                if change_percent > guardrail.adaptive_max_change_percent =>
            {
                Err(pg_kinetic_core::adaptive::AdaptiveApplyError::new(
                    pg_kinetic_core::adaptive::AdaptiveGuardrail::MaxChangePercent,
                    format!(
                        "adaptive change percent {change_percent} exceeds the configured limit {}",
                        guardrail.adaptive_max_change_percent
                    ),
                ))
            }
            pg_kinetic_core::adaptive::TuningBound::Percent(_) => {
                Ok(pg_kinetic_core::adaptive::AdaptiveOutcome::Applied)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct AdaptiveGuardrailConfig {
    #[arg(
        long = "adaptive-max-change-percent",
        env = "PG_KINETIC_ADAPTIVE_MAX_CHANGE_PERCENT",
        default_value_t = 10
    )]
    pub adaptive_max_change_percent: u8,
}

impl AdaptiveGuardrailConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.adaptive_max_change_percent == 0 || self.adaptive_max_change_percent > 100 {
            return Err(String::from(
                "adaptive_max_change_percent must be between 1 and 100",
            ));
        }

        Ok(())
    }

    #[must_use]
    pub const fn safety_bound(&self) -> pg_kinetic_core::adaptive::TuningBound {
        pg_kinetic_core::adaptive::TuningBound::percent(self.adaptive_max_change_percent)
    }
}

impl Default for AdaptiveGuardrailConfig {
    fn default() -> Self {
        Self {
            adaptive_max_change_percent: 10,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct ConnectionConfig {
    #[arg(long, env = "PG_KINETIC_LISTEN_ADDR", default_value = "127.0.0.1:6543")]
    pub listen_addr: SocketAddr,

    #[arg(
        long,
        env = "PG_KINETIC_BACKEND_ADDR",
        default_value = "127.0.0.1:5432"
    )]
    pub backend_addr: SocketAddr,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PoolConfig {
    pub database: String,
    pub user: String,
    pub backend_addr: SocketAddr,
    pub max_backends: Option<usize>,
}

fn deserialize_pools<'de, D>(deserializer: D) -> Result<Vec<PoolConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let pools = Vec::<PoolConfig>::deserialize(deserializer)?;
    validate_pool_configs(&pools).map_err(serde::de::Error::custom)?;
    Ok(pools)
}

fn validate_pool_configs(pools: &[PoolConfig]) -> Result<(), String> {
    let mut identities = HashSet::with_capacity(pools.len());
    for pool in pools {
        if pool.max_backends == Some(0) {
            return Err(format!(
                "pool max_backends must be greater than zero for database/user {}/{}",
                pool.database, pool.user
            ));
        }
        if !identities.insert((&pool.database, &pool.user)) {
            return Err(format!(
                "duplicate pool for database/user {}/{}",
                pool.database, pool.user
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteConfig {
    pub primary: BackendEndpointConfig,

    #[serde(default)]
    pub replicas: Vec<ReplicaConfig>,

    #[serde(default)]
    pub read_routing: ReadRoutingConfig,

    #[serde(default)]
    pub freshness: FreshnessConfig,

    #[serde(default)]
    pub ha: HaConfig,
}

impl RouteConfig {
    #[must_use]
    pub fn from_backend_addr(address: SocketAddr) -> Self {
        Self {
            primary: BackendEndpointConfig {
                address,
                ..BackendEndpointConfig::default()
            },
            replicas: Vec::new(),
            read_routing: ReadRoutingConfig::default(),
            freshness: FreshnessConfig::default(),
            ha: HaConfig::default(),
        }
    }
}

impl Default for RouteConfig {
    fn default() -> Self {
        Self::from_backend_addr(BackendEndpointConfig::default().address)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendEndpointConfig {
    pub address: SocketAddr,
    pub connect_timeout_ms: u64,
    pub tls_mode: BackendTlsMode,
}

impl Default for BackendEndpointConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:5432"
                .parse()
                .expect("valid default backend addr"),
            connect_timeout_ms: 1_000,
            tls_mode: BackendTlsMode::Disable,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaConfig {
    pub address: SocketAddr,
    pub connect_timeout_ms: u64,
    pub tls_mode: BackendTlsMode,

    #[serde(default = "default_replica_weight")]
    pub weight: u32,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:5432"
                .parse()
                .expect("valid default backend addr"),
            connect_timeout_ms: 1_000,
            tls_mode: BackendTlsMode::Disable,
            weight: default_replica_weight(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadRoutingConfig {
    #[serde(
        default = "default_read_routing_mode",
        deserialize_with = "deserialize_read_routing_mode",
        serialize_with = "serialize_read_routing_mode"
    )]
    pub read_routing_mode: ReadRoutingMode,

    #[serde(
        default = "default_fallback_policy",
        deserialize_with = "deserialize_fallback_policy",
        serialize_with = "serialize_fallback_policy"
    )]
    pub fallback_policy: FallbackPolicy,
}

impl Default for ReadRoutingConfig {
    fn default() -> Self {
        Self {
            read_routing_mode: default_read_routing_mode(),
            fallback_policy: default_fallback_policy(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FreshnessConfig {
    #[serde(
        default = "default_freshness_policy",
        deserialize_with = "deserialize_freshness_policy",
        serialize_with = "serialize_freshness_policy"
    )]
    pub freshness_policy: FreshnessPolicy,

    #[serde(default = "default_max_replica_lag_ms")]
    pub max_replica_lag_ms: u64,

    #[serde(default = "default_read_after_write_timeout_ms")]
    pub read_after_write_timeout_ms: u64,
}

impl Default for FreshnessConfig {
    fn default() -> Self {
        Self {
            freshness_policy: default_freshness_policy(),
            max_replica_lag_ms: default_max_replica_lag_ms(),
            read_after_write_timeout_ms: default_read_after_write_timeout_ms(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HaConfig {
    #[serde(default = "default_replica_health_interval_ms")]
    pub replica_health_interval_ms: u64,

    #[serde(default = "default_replica_health_timeout_ms")]
    pub replica_health_timeout_ms: u64,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            replica_health_interval_ms: default_replica_health_interval_ms(),
            replica_health_timeout_ms: default_replica_health_timeout_ms(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Parser, Serialize)]
#[serde(default)]
pub struct ShardingConfig {
    #[arg(long, env = "PG_KINETIC_SHARDING_ENABLED", default_value_t = false)]
    pub sharding_enabled: bool,

    #[arg(
        long,
        env = "PG_KINETIC_MULTI_SHARD_POLICY",
        value_enum,
        default_value_t = MultiShardPolicyConfig::Reject
    )]
    pub multi_shard_policy: MultiShardPolicyConfig,

    #[arg(
        long,
        env = "PG_KINETIC_ROUTE_MAP_RELOAD_STRICT",
        default_value_t = true
    )]
    pub route_map_reload_strict: bool,

    #[arg(
        long,
        env = "PG_KINETIC_ROUTE_PREVIEW_ENABLED",
        default_value_t = false
    )]
    pub route_preview_enabled: bool,

    #[serde(default)]
    #[arg(skip)]
    pub route_maps: Vec<RouteMapConfig>,
}

impl ShardingConfig {
    fn validate(&self) -> Result<(), String> {
        for route_map in &self.route_maps {
            route_map.validate()?;
        }

        for (left_index, left_route_map) in self.route_maps.iter().enumerate() {
            for (right_index, right_route_map) in
                self.route_maps.iter().enumerate().skip(left_index + 1)
            {
                if left_route_map.scope.overlaps(&right_route_map.scope)
                    && left_route_map.priority.is_none()
                    && right_route_map.priority.is_none()
                {
                    return Err(format!(
                        "route maps {left_index} and {right_index} overlap without explicit priority"
                    ));
                }
            }
        }

        Ok(())
    }
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            sharding_enabled: false,
            multi_shard_policy: MultiShardPolicyConfig::Reject,
            route_map_reload_strict: true,
            route_preview_enabled: false,
            route_maps: Vec::new(),
        }
    }
}

impl<'de> Deserialize<'de> for ShardingConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = ShardingConfigRaw::deserialize(deserializer)?;
        let config = Self {
            sharding_enabled: raw.sharding_enabled,
            multi_shard_policy: raw.multi_shard_policy,
            route_map_reload_strict: raw.route_map_reload_strict,
            route_preview_enabled: raw.route_preview_enabled,
            route_maps: raw.route_maps,
        };

        config.validate().map_err(serde::de::Error::custom)?;
        Ok(config)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
struct ShardingConfigRaw {
    #[serde(default)]
    sharding_enabled: bool,

    #[serde(default)]
    multi_shard_policy: MultiShardPolicyConfig,

    #[serde(default)]
    route_map_reload_strict: bool,

    #[serde(default)]
    route_preview_enabled: bool,

    #[serde(default)]
    route_maps: Vec<RouteMapConfig>,
}

impl Default for ShardingConfigRaw {
    fn default() -> Self {
        Self {
            sharding_enabled: false,
            multi_shard_policy: MultiShardPolicyConfig::Reject,
            route_map_reload_strict: true,
            route_preview_enabled: false,
            route_maps: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteMapConfig {
    pub scope: ShardScopeConfig,

    pub strategy: ShardStrategyConfig,

    pub targets: Vec<ShardTargetConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<RouteMapPriority>,
}

impl RouteMapConfig {
    fn validate(&self) -> Result<(), String> {
        if self.targets.is_empty() {
            return Err(String::from("route map must define at least one target"));
        }

        let mut seen_shard_ids = HashSet::new();
        for target in &self.targets {
            let shard_id =
                ShardId::new(target.shard_id().to_owned()).map_err(|error| error.to_string())?;
            if !seen_shard_ids.insert(shard_id.clone()) {
                return Err(format!("duplicate shard id '{shard_id}'"));
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RouteMapPriority(pub u32);

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ShardScopeConfig {
    DatabaseUser { database: String, user: String },
    ApplicationName { application_name: String },
    SchemaTable { schema: String, table: String },
    TenantKey { tenant_key: String },
}

impl ShardScopeConfig {
    fn overlaps(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Debug for ShardScopeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatabaseUser { database, user } => formatter
                .debug_struct("DatabaseUser")
                .field("database", database)
                .field("user", user)
                .finish(),
            Self::ApplicationName { application_name } => formatter
                .debug_struct("ApplicationName")
                .field("application_name", application_name)
                .finish(),
            Self::SchemaTable { schema, table } => formatter
                .debug_struct("SchemaTable")
                .field("schema", schema)
                .field("table", table)
                .finish(),
            Self::TenantKey { .. } => formatter
                .debug_struct("TenantKey")
                .field("tenant_key", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum MultiShardPolicyConfig {
    #[default]
    Reject,
    FirstMatch,
    FanOut,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ShardStrategyConfig {
    Hash,
    Range,
    List,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ShardTargetConfig {
    Primary { shard_id: String },
    Replicas { shard_id: String },
}

impl ShardTargetConfig {
    fn shard_id(&self) -> &str {
        match self {
            Self::Primary { shard_id } | Self::Replicas { shard_id } => shard_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Parser, Serialize)]
#[serde(default)]
pub struct PolicyConfig {
    #[arg(
        long,
        env = "PG_KINETIC_POLICY_MODE",
        value_parser = parse_policy_mode,
        default_value = "disabled"
    )]
    #[serde(
        default = "default_policy_mode",
        deserialize_with = "deserialize_policy_mode",
        serialize_with = "serialize_policy_mode"
    )]
    pub policy_mode: PolicyMode,

    #[serde(default)]
    #[arg(skip)]
    pub policy_files: Vec<PolicyFileConfig>,

    #[serde(default)]
    #[arg(skip)]
    pub inline_rules: Vec<InlinePolicyConfig>,

    #[serde(default, flatten)]
    #[command(flatten)]
    pub policy_audit: PolicyAuditConfig,

    #[serde(default, flatten)]
    #[command(flatten)]
    pub policy_wasm: PolicyWasmConfig,

    #[arg(
        long,
        env = "PG_KINETIC_POLICY_EVAL_TIMEOUT_MS",
        default_value_t = default_policy_eval_timeout_ms()
    )]
    #[serde(default = "default_policy_eval_timeout_ms")]
    pub policy_eval_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_POLICY_MAX_CONTEXT_BYTES",
        default_value_t = default_policy_max_context_bytes()
    )]
    #[serde(default = "default_policy_max_context_bytes")]
    pub policy_max_context_bytes: usize,
}

impl PolicyConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.validate_internal()
    }

    pub fn validate_routes<R>(&self, existing_routes: R) -> Result<(), String>
    where
        R: IntoIterator,
        R::Item: AsRef<str>,
    {
        self.validate_with_context(existing_routes, false, std::iter::empty::<&str>())
    }

    pub fn validate_shards<S>(
        &self,
        sharding_enabled: bool,
        existing_shards: S,
    ) -> Result<(), String>
    where
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        self.validate_with_context(
            std::iter::empty::<&str>(),
            sharding_enabled,
            existing_shards,
        )
    }

    pub fn validate_with_context<R, S>(
        &self,
        existing_routes: R,
        sharding_enabled: bool,
        existing_shards: S,
    ) -> Result<(), String>
    where
        R: IntoIterator,
        R::Item: AsRef<str>,
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        self.validate_internal()?;

        let existing_routes = existing_routes
            .into_iter()
            .map(|route| route.as_ref().to_owned())
            .collect::<HashSet<_>>();
        let existing_shards = existing_shards
            .into_iter()
            .map(|shard| shard.as_ref().to_owned())
            .collect::<HashSet<_>>();

        for inline_rule in &self.inline_rules {
            match &inline_rule.action {
                InlinePolicyActionConfig::RouteOverride { target_id }
                    if !existing_routes.contains(target_id.as_str()) =>
                {
                    return Err(format!(
                        "route override target '{}' does not reference an existing route",
                        target_id.as_str()
                    ));
                }
                InlinePolicyActionConfig::ShardOverride { target_id }
                    if sharding_enabled && !existing_shards.contains(target_id.as_str()) =>
                {
                    return Err(format!(
                        "shard override target '{}' does not reference an existing shard",
                        target_id.as_str()
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn validate_internal(&self) -> Result<(), String> {
        for inline_rule in &self.inline_rules {
            match &inline_rule.action {
                InlinePolicyActionConfig::Deny { reason } if reason.trim().is_empty() => {
                    return Err(String::from("deny action requires a reason"));
                }
                InlinePolicyActionConfig::Wasm { .. } if !self.policy_wasm.policy_wasm_enabled => {
                    return Err(String::from(
                        "wasm policies require policy_wasm_enabled to be true",
                    ));
                }
                InlinePolicyActionConfig::Wasm { module_path } => {
                    #[cfg(feature = "policy-wasm")]
                    {
                        WasmPolicyEvaluator::validate_module_path(module_path).map_err(
                            |error| {
                                format!(
                                    "wasm policy module {} failed validation: {error}",
                                    module_path.display()
                                )
                            },
                        )?;
                    }

                    #[cfg(not(feature = "policy-wasm"))]
                    {
                        let _ = module_path;
                        return Err(String::from(
                            "wasm policies require the crate feature 'policy-wasm' to be enabled",
                        ));
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            policy_mode: default_policy_mode(),
            policy_files: Vec::new(),
            inline_rules: Vec::new(),
            policy_audit: PolicyAuditConfig::default(),
            policy_wasm: PolicyWasmConfig::default(),
            policy_eval_timeout_ms: default_policy_eval_timeout_ms(),
            policy_max_context_bytes: default_policy_max_context_bytes(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PolicyFileConfig {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InlinePolicyConfig {
    #[serde(
        default = "default_policy_id",
        deserialize_with = "deserialize_policy_id",
        serialize_with = "serialize_policy_id"
    )]
    pub policy_id: PolicyId,

    #[serde(
        default = "default_policy_hook_point",
        deserialize_with = "deserialize_policy_hook_point",
        serialize_with = "serialize_policy_hook_point"
    )]
    pub hook_point: PolicyHookPoint,

    #[serde(flatten)]
    pub action: InlinePolicyActionConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InlinePolicyActionConfig {
    Allow,
    Deny {
        reason: String,
    },
    RequirePrimary,
    RequireReplica,
    RouteOverride {
        #[serde(
            deserialize_with = "deserialize_policy_route_target_id",
            serialize_with = "serialize_policy_route_target_id"
        )]
        target_id: PolicyRouteTargetId,
    },
    ShardOverride {
        #[serde(
            deserialize_with = "deserialize_policy_shard_target_id",
            serialize_with = "serialize_policy_shard_target_id"
        )]
        target_id: PolicyShardTargetId,
    },
    Wasm {
        module_path: PathBuf,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct PolicyAuditConfig {
    #[arg(long, env = "PG_KINETIC_POLICY_AUDIT_ENABLED", default_value_t = true)]
    #[serde(default = "default_policy_audit_enabled")]
    pub policy_audit_enabled: bool,

    #[arg(
        long,
        env = "PG_KINETIC_POLICY_AUDIT_SAMPLE_RATE",
        default_value_t = 1.0
    )]
    #[serde(default = "default_policy_audit_sample_rate")]
    pub policy_audit_sample_rate: f64,
}

impl Default for PolicyAuditConfig {
    fn default() -> Self {
        Self {
            policy_audit_enabled: default_policy_audit_enabled(),
            policy_audit_sample_rate: default_policy_audit_sample_rate(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct PolicyWasmConfig {
    #[arg(long, env = "PG_KINETIC_POLICY_WASM_ENABLED", default_value_t = false)]
    #[serde(default)]
    pub policy_wasm_enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct CapacityConfig {
    #[arg(long, env = "PG_KINETIC_MAX_CLIENTS", default_value_t = 10_000)]
    pub max_clients: usize,

    #[arg(long, env = "PG_KINETIC_MAX_BACKENDS", default_value_t = 100)]
    pub max_backends: usize,

    #[arg(long, env = "PG_KINETIC_MAX_CHECKOUT_WAITERS", default_value_t = 1_000)]
    pub max_checkout_waiters: usize,
}

impl CapacityConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_backends == 0 {
            return Err(String::from(
                "capacity.max_backends must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct PoolLifecycleConfig {
    #[arg(
        long = "pool-max-size",
        env = "PG_KINETIC_POOL_MAX_SIZE",
        default_value_t = 100
    )]
    pub max_size: usize,

    #[arg(
        long = "pool-min-idle",
        env = "PG_KINETIC_POOL_MIN_IDLE",
        default_value_t = 0
    )]
    pub min_idle: usize,

    #[arg(
        long = "pool-idle-timeout-ms",
        env = "PG_KINETIC_POOL_IDLE_TIMEOUT_MS",
        default_value = "1800000",
        value_parser = parse_duration_ms
    )]
    #[serde(
        deserialize_with = "deserialize_duration_ms",
        serialize_with = "serialize_duration_ms"
    )]
    pub idle_timeout: Duration,

    #[arg(
        long = "pool-max-lifetime-ms",
        env = "PG_KINETIC_POOL_MAX_LIFETIME_MS",
        default_value = "0",
        value_parser = parse_duration_ms
    )]
    #[serde(
        deserialize_with = "deserialize_duration_ms",
        serialize_with = "serialize_duration_ms"
    )]
    pub max_lifetime: Duration,
}

impl PoolLifecycleConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_size == 0 {
            return Err(String::from("pool_max_size must be greater than zero"));
        }
        if self.min_idle > self.max_size {
            return Err(String::from("pool_min_idle cannot exceed pool_max_size"));
        }
        Ok(())
    }
}

impl Default for PoolLifecycleConfig {
    fn default() -> Self {
        Self {
            max_size: 100,
            min_idle: 0,
            idle_timeout: Duration::from_millis(1_800_000),
            max_lifetime: Duration::ZERO,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct PerformanceConfig {
    #[arg(long, env = "PG_KINETIC_CHECKOUT_TIMEOUT_MS", default_value_t = 1_000)]
    pub checkout_timeout_ms: u64,

    #[arg(
        long = "pool-mode",
        env = "PG_KINETIC_POOL_MODE",
        value_enum,
        default_value_t = PoolMode::Transaction
    )]
    pub pool_mode: PoolMode,

    #[arg(
        long,
        env = "PG_KINETIC_RECOVERY_MODE",
        value_enum,
        default_value_t = RecoveryMode::Recover
    )]
    #[serde(
        deserialize_with = "deserialize_recovery_mode",
        serialize_with = "serialize_recovery_mode"
    )]
    pub recovery_mode: RecoveryMode,

    #[arg(long, env = "PG_KINETIC_RECOVERY_TIMEOUT_MS", default_value_t = 5_000)]
    pub recovery_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_BACKEND_RESET_QUERY",
        default_value = "DISCARD ALL"
    )]
    pub backend_reset_query: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct AdminConfig {
    #[arg(long, env = "PG_KINETIC_ADMIN_ADDR")]
    pub admin_addr: Option<SocketAddr>,

    #[arg(long, env = "PG_KINETIC_ADMIN_REQUIRE_TLS")]
    pub admin_require_tls: bool,

    #[arg(long, env = "PG_KINETIC_ADMIN_ALLOWED_USER")]
    pub admin_allowed_user: Option<String>,

    #[arg(
        long,
        env = "PG_KINETIC_ADMIN_QUERY_TIMEOUT_MS",
        default_value_t = 1_000
    )]
    pub admin_query_timeout_ms: u64,

    #[arg(long, env = "PG_KINETIC_ADMIN_MAX_CLIENTS", default_value_t = 8)]
    pub admin_max_clients: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct QosConfig {
    #[arg(
        long,
        env = "PG_KINETIC_MAX_ROUTE_IN_FLIGHT",
        default_value_t = QosDefaults::MAX_ROUTE_IN_FLIGHT
    )]
    pub max_route_in_flight: usize,

    #[arg(
        long,
        env = "PG_KINETIC_MAX_ROUTE_WAITERS",
        default_value_t = QosDefaults::MAX_ROUTE_WAITERS
    )]
    pub max_route_waiters: usize,

    #[arg(
        long,
        env = "PG_KINETIC_QUERY_TIMEOUT_MS",
        default_value_t = TimeoutDefaults::QUERY_TIMEOUT_MS
    )]
    pub query_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_IDLE_CLIENT_TIMEOUT_MS",
        default_value_t = TimeoutDefaults::IDLE_CLIENT_TIMEOUT_MS
    )]
    pub idle_client_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_IDLE_TRANSACTION_TIMEOUT_MS",
        default_value_t = TimeoutDefaults::IDLE_TRANSACTION_TIMEOUT_MS
    )]
    pub idle_transaction_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_MAX_CLIENT_BUFFER_BYTES",
        default_value_t = BufferDefaults::MAX_CLIENT_BUFFER_BYTES
    )]
    pub max_client_buffer_bytes: usize,

    #[arg(
        long,
        env = "PG_KINETIC_MAX_BACKEND_BUFFER_BYTES",
        default_value_t = BufferDefaults::MAX_BACKEND_BUFFER_BYTES
    )]
    pub max_backend_buffer_bytes: usize,

    #[arg(long, env = "PG_KINETIC_OVERLOAD_ERROR_CODE", default_value = "53300")]
    pub overload_error_code: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    #[arg(long, env = "PG_KINETIC_METRICS_ADDR")]
    pub metrics_addr: Option<SocketAddr>,

    #[arg(
        long,
        env = "PG_KINETIC_DEBUG_TRACE_SAMPLING_RATE",
        default_value_t = 0.0
    )]
    pub debug_trace_sampling_rate: f64,

    #[arg(
        long,
        env = "PG_KINETIC_PHASE_TIMING_SAMPLE_RATE",
        default_value_t = 0.0
    )]
    pub phase_timing_sample_rate: f64,

    #[arg(long, env = "PG_KINETIC_OTEL_ENABLED")]
    pub otel_enabled: bool,

    #[arg(long, env = "PG_KINETIC_OTEL_ENDPOINT")]
    pub otel_endpoint: Option<String>,

    #[arg(
        long,
        env = "PG_KINETIC_OTEL_SERVICE_NAME",
        default_value = "pg-kinetic"
    )]
    pub otel_service_name: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_addr: None,
            debug_trace_sampling_rate: 0.0,
            phase_timing_sample_rate: 0.0,
            otel_enabled: false,
            otel_endpoint: None,
            otel_service_name: String::from("pg-kinetic"),
        }
    }
}

impl ObservabilityConfig {
    #[must_use]
    pub fn trace_sampling_ratio(&self) -> f64 {
        if self.debug_trace_sampling_rate.is_finite() {
            self.debug_trace_sampling_rate.clamp(0.0, 1.0)
        } else {
            0.0
        }
    }

    #[must_use]
    pub fn phase_timing_sample_rate(&self) -> f64 {
        if self.phase_timing_sample_rate.is_finite() {
            self.phase_timing_sample_rate.clamp(0.0, 1.0)
        } else {
            0.0
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct TlsConfig {
    #[arg(
        long,
        env = "PG_KINETIC_CLIENT_TLS_MODE",
        value_enum,
        default_value_t = ClientTlsMode::Disable
    )]
    pub client_tls_mode: ClientTlsMode,

    #[arg(long, env = "PG_KINETIC_CLIENT_TLS_CERT_PATH")]
    pub client_cert_path: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_CLIENT_TLS_KEY_PATH")]
    pub client_key_path: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_CLIENT_TLS_CA_PATH")]
    pub client_ca_path: Option<PathBuf>,

    #[arg(
        long,
        env = "PG_KINETIC_BACKEND_TLS_MODE",
        value_enum,
        default_value_t = BackendTlsMode::Disable
    )]
    pub backend_tls_mode: BackendTlsMode,

    #[arg(long, env = "PG_KINETIC_BACKEND_TLS_CA_PATH")]
    pub backend_ca_path: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_BACKEND_TLS_SERVER_NAME")]
    pub backend_server_name: Option<String>,
}

impl TlsConfig {
    #[must_use]
    pub fn client_tls_mode_core(&self) -> CoreClientTlsMode {
        self.client_tls_mode.into()
    }

    #[must_use]
    pub fn backend_tls_mode_core(&self) -> CoreBackendTlsMode {
        self.backend_tls_mode.into()
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            client_tls_mode: ClientTlsMode::Disable,
            client_cert_path: None,
            client_key_path: None,
            client_ca_path: None,
            backend_tls_mode: BackendTlsMode::Disable,
            backend_ca_path: None,
            backend_server_name: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct AuthConfig {
    #[arg(
        long,
        env = "PG_KINETIC_AUTH_MODE",
        value_enum,
        default_value_t = AuthMode::PassThrough
    )]
    pub auth_mode: AuthMode,

    #[arg(long, env = "PG_KINETIC_AUTH_USERS_FILE")]
    pub auth_users_file: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_BACKEND_USER")]
    pub backend_user: Option<String>,

    #[arg(long, env = "PG_KINETIC_BACKEND_PASSWORD_ENV_VAR_NAME")]
    pub backend_password_env_var_name: Option<String>,

    #[arg(long, env = "PG_KINETIC_AUTH_QUERY_ENABLED")]
    pub auth_query_enabled: bool,

    #[arg(
        long,
        env = "PG_KINETIC_AUTH_QUERY",
        default_value = "SELECT usename, passwd FROM pg_shadow WHERE usename = $1"
    )]
    pub auth_query: String,

    #[arg(
        long,
        env = "PG_KINETIC_AUTH_QUERY_CACHE_TTL_MS",
        default_value_t = 60_000
    )]
    pub auth_query_cache_ttl_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_AUTH_FAILURE_MESSAGE_MODE",
        value_enum,
        default_value_t = AuthFailureMessageMode::Generic
    )]
    pub auth_failure_message_mode: AuthFailureMessageMode,
}

impl AuthConfig {
    #[must_use]
    pub fn auth_mode_core(&self) -> CoreAuthMode {
        self.auth_mode.into()
    }

    #[must_use]
    pub const fn auth_query_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.auth_query_cache_ttl_ms)
    }

    pub fn validate(&self) -> Result<(), String> {
        match (
            self.backend_user.as_deref(),
            self.backend_password_env_var_name.as_deref(),
        ) {
            (Some(_), Some(_)) if self.auth_mode == AuthMode::PassThrough => {
                return Err(String::from(
                    "backend service credentials are incompatible with auth_mode=pass_through",
                ));
            }
            (Some(_), None) => {
                return Err(String::from(
                    "auth.backend_user requires auth.backend_password_env_var_name",
                ));
            }
            (None, Some(_)) => {
                return Err(String::from(
                    "auth.backend_password_env_var_name requires auth.backend_user",
                ));
            }
            _ => {}
        }

        if self.auth_query_enabled {
            if self.backend_user.is_none() || self.backend_password_env_var_name.is_none() {
                return Err(String::from(
                    "auth_query_enabled requires backend service credentials",
                ));
            }
            if self.auth_query.trim().is_empty() {
                return Err(String::from("auth_query must not be empty"));
            }
            if self.auth_query.match_indices("$1").count() != 1 {
                return Err(String::from(
                    "auth_query must contain exactly one $1 placeholder",
                ));
            }
        }

        Ok(())
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            auth_mode: AuthMode::PassThrough,
            auth_users_file: None,
            backend_user: None,
            backend_password_env_var_name: None,
            auth_query_enabled: false,
            auth_query: String::from("SELECT usename, passwd FROM pg_shadow WHERE usename = $1"),
            auth_query_cache_ttl_ms: 60_000,
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct ReloadConfig {
    #[arg(long, env = "PG_KINETIC_CONFIG_FILE")]
    pub config_file: Option<PathBuf>,

    #[arg(
        long,
        env = "PG_KINETIC_CONFIG_RELOAD_INTERVAL_MS",
        default_value_t = 5_000
    )]
    pub config_reload_interval_ms: u64,

    #[arg(long, env = "PG_KINETIC_CONFIG_RELOAD_ENABLED")]
    pub reload_enabled: bool,
}

impl ReloadConfig {
    #[must_use]
    pub const fn config_reload_interval(&self) -> Duration {
        Duration::from_millis(self.config_reload_interval_ms)
    }
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            config_file: None,
            config_reload_interval_ms: 5_000,
            reload_enabled: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct DrainConfig {
    #[arg(
        long,
        visible_alias = "drain-grace-ms",
        env = "PG_KINETIC_DRAIN_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    #[serde(alias = "drain_grace_ms")]
    pub drain_timeout_ms: u64,

    #[arg(long, env = "PG_KINETIC_REJECT_NEW_CLIENTS_DURING_DRAIN")]
    pub reject_new_clients_during_drain: bool,
}

impl DrainConfig {
    #[must_use]
    pub const fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.drain_timeout_ms)
    }

    #[must_use]
    pub const fn drain_grace(&self) -> Duration {
        self.drain_timeout()
    }
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            drain_timeout_ms: 30_000,
            reject_new_clients_during_drain: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct HealthConfig {
    #[arg(long, env = "PG_KINETIC_HEALTH_ADDR")]
    pub health_addr: Option<SocketAddr>,

    #[arg(
        long,
        env = "PG_KINETIC_READINESS_BACKEND_CHECK_INTERVAL_MS",
        default_value_t = 1_000
    )]
    pub readiness_backend_check_interval_ms: u64,

    #[arg(long, env = "PG_KINETIC_READINESS_TIMEOUT_MS", default_value_t = 5_000)]
    pub readiness_timeout_ms: u64,
}

impl HealthConfig {
    #[must_use]
    pub const fn readiness_backend_check_interval(&self) -> Duration {
        Duration::from_millis(self.readiness_backend_check_interval_ms)
    }

    #[must_use]
    pub const fn readiness_timeout(&self) -> Duration {
        Duration::from_millis(self.readiness_timeout_ms)
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            health_addr: None,
            readiness_backend_check_interval_ms: 1_000,
            readiness_timeout_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Args, Serialize)]
#[serde(default)]
pub struct SocketConfig {
    #[arg(long, env = "PG_KINETIC_TCP_NODELAY", default_value_t = true)]
    pub tcp_nodelay: bool,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE")]
    pub tcp_keepalive: bool,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE_IDLE_MS")]
    pub tcp_keepalive_idle_ms: Option<u64>,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE_INTERVAL_MS")]
    pub tcp_keepalive_interval_ms: Option<u64>,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE_RETRIES")]
    pub tcp_keepalive_retries: Option<u32>,

    #[arg(long, env = "PG_KINETIC_TCP_USER_TIMEOUT_MS")]
    pub tcp_user_timeout_ms: Option<u64>,

    #[arg(long, env = "PG_KINETIC_TCP_SEND_BUFFER_BYTES")]
    pub tcp_send_buffer_bytes: Option<usize>,

    #[arg(long, env = "PG_KINETIC_TCP_RECV_BUFFER_BYTES")]
    pub tcp_recv_buffer_bytes: Option<usize>,

    #[arg(long, env = "PG_KINETIC_STRICT_SOCKET_OPTION_MODE")]
    pub strict_socket_option_mode: bool,
}

impl SocketConfig {
    #[must_use]
    pub fn tcp_keepalive_idle(&self) -> Option<Duration> {
        self.tcp_keepalive_idle_ms.map(Duration::from_millis)
    }

    #[must_use]
    pub fn tcp_keepalive_interval(&self) -> Option<Duration> {
        self.tcp_keepalive_interval_ms.map(Duration::from_millis)
    }

    #[must_use]
    pub fn tcp_user_timeout(&self) -> Option<Duration> {
        self.tcp_user_timeout_ms.map(Duration::from_millis)
    }
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            tcp_keepalive: false,
            tcp_keepalive_idle_ms: None,
            tcp_keepalive_interval_ms: None,
            tcp_keepalive_retries: None,
            tcp_user_timeout_ms: None,
            tcp_send_buffer_bytes: None,
            tcp_recv_buffer_bytes: None,
            strict_socket_option_mode: false,
        }
    }
}

impl Config {
    #[must_use]
    pub fn parse_args() -> Self {
        Self::try_parse_from_args(std::env::args_os()).unwrap_or_else(|error| error.exit())
    }

    pub fn try_parse_from_args<I, T>(args: I) -> clap::error::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let config = Self::try_parse_from(args)?;
        config.capacity.validate().map_err(|message| {
            clap::Error::raw(clap::error::ErrorKind::ValueValidation, message)
        })?;
        config.runtime.engine.validate().map_err(|message| {
            clap::Error::raw(clap::error::ErrorKind::ValueValidation, message)
        })?;
        Ok(config)
    }

    #[must_use]
    pub fn route_configs(&self) -> Vec<RouteConfig> {
        self.effective_routes()
    }

    #[must_use]
    pub fn effective_routes(&self) -> Vec<RouteConfig> {
        if self.routes.is_empty() {
            vec![RouteConfig::from_backend_addr(self.connection.backend_addr)]
        } else {
            self.routes.clone()
        }
    }

    pub fn validate_pool_configs(&self) -> Result<(), String> {
        validate_pool_configs(&self.pools)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.capacity.validate()?;
        self.auth.validate()?;
        self.validate_pool_configs()
    }

    #[must_use]
    pub fn is_reload_compatible_with(&self, next: &Self) -> bool {
        self.connection == next.connection
            && self.routes == next.routes
            && self.pools == next.pools
            && self.capacity == next.capacity
            && self.pool_lifecycle == next.pool_lifecycle
            && self.performance == next.performance
            && self.qos.max_route_in_flight == next.qos.max_route_in_flight
            && self.qos.max_route_waiters == next.qos.max_route_waiters
            && self.qos.idle_client_timeout_ms == next.qos.idle_client_timeout_ms
            && self.qos.idle_transaction_timeout_ms == next.qos.idle_transaction_timeout_ms
            && self.qos.max_client_buffer_bytes == next.qos.max_client_buffer_bytes
            && self.qos.max_backend_buffer_bytes == next.qos.max_backend_buffer_bytes
            && self.qos.overload_error_code == next.qos.overload_error_code
            && self.admin == next.admin
            && self.observability == next.observability
            && self.tls.client_tls_mode == next.tls.client_tls_mode
            && self.tls.client_cert_path == next.tls.client_cert_path
            && self.tls.client_key_path == next.tls.client_key_path
            && self.tls.client_ca_path == next.tls.client_ca_path
            && self.tls.backend_tls_mode == next.tls.backend_tls_mode
            && self.tls.backend_ca_path == next.tls.backend_ca_path
            && self.tls.backend_server_name == next.tls.backend_server_name
            && self.auth.auth_mode == next.auth.auth_mode
            && self.auth.auth_query_enabled == next.auth.auth_query_enabled
            && self.auth.auth_query == next.auth.auth_query
            && self.auth.auth_query_cache_ttl_ms == next.auth.auth_query_cache_ttl_ms
            && self.auth.auth_failure_message_mode == next.auth.auth_failure_message_mode
            && self.reload == next.reload
            && self.drain == next.drain
            && self.health == next.health
            && self.runtime == next.runtime
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6543".parse().expect("valid default listen addr"),
            backend_addr: "127.0.0.1:5432"
                .parse()
                .expect("valid default backend addr"),
        }
    }
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            max_clients: 10_000,
            max_backends: 100,
            max_checkout_waiters: 1_000,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            checkout_timeout_ms: 1_000,
            pool_mode: PoolMode::Transaction,
            recovery_mode: RecoveryMode::Recover,
            recovery_timeout_ms: 5_000,
            backend_reset_query: String::from("DISCARD ALL"),
        }
    }
}

impl Default for QosConfig {
    fn default() -> Self {
        Self {
            max_route_in_flight: QosDefaults::MAX_ROUTE_IN_FLIGHT,
            max_route_waiters: QosDefaults::MAX_ROUTE_WAITERS,
            query_timeout_ms: TimeoutDefaults::QUERY_TIMEOUT_MS,
            idle_client_timeout_ms: TimeoutDefaults::IDLE_CLIENT_TIMEOUT_MS,
            idle_transaction_timeout_ms: TimeoutDefaults::IDLE_TRANSACTION_TIMEOUT_MS,
            max_client_buffer_bytes: BufferDefaults::MAX_CLIENT_BUFFER_BYTES,
            max_backend_buffer_bytes: BufferDefaults::MAX_BACKEND_BUFFER_BYTES,
            overload_error_code: String::from("53300"),
        }
    }
}

impl PerformanceConfig {
    #[must_use]
    pub const fn checkout_timeout(&self) -> Duration {
        Duration::from_millis(self.checkout_timeout_ms)
    }

    #[must_use]
    pub const fn recovery_timeout(&self) -> Duration {
        Duration::from_millis(self.recovery_timeout_ms)
    }
}

impl QosConfig {
    #[must_use]
    pub const fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout_ms)
    }

    #[must_use]
    pub const fn idle_client_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_client_timeout_ms)
    }

    #[must_use]
    pub const fn idle_transaction_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_transaction_timeout_ms)
    }
}

fn deserialize_recovery_mode<'de, D>(deserializer: D) -> Result<RecoveryMode, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_recovery_mode(&value).map_err(serde::de::Error::custom)
}

fn serialize_recovery_mode<S>(mode: &RecoveryMode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match mode {
        RecoveryMode::Recover => "recover",
        RecoveryMode::RollbackOnly => "rollback_only",
        RecoveryMode::Drop => "drop",
    })
}

fn parse_recovery_mode(value: &str) -> Result<RecoveryMode, String> {
    match value {
        "recover" => Ok(RecoveryMode::Recover),
        "rollback_only" => Ok(RecoveryMode::RollbackOnly),
        "drop" => Ok(RecoveryMode::Drop),
        other => Err(format!("invalid recovery mode '{other}'")),
    }
}

fn default_replica_weight() -> u32 {
    1
}

fn default_read_routing_mode() -> ReadRoutingMode {
    ReadRoutingMode::Off
}

fn default_fallback_policy() -> FallbackPolicy {
    FallbackPolicy::Primary
}

fn default_freshness_policy() -> FreshnessPolicy {
    FreshnessPolicy::SessionWriteLsn
}

fn default_max_replica_lag_ms() -> u64 {
    1_000
}

fn default_read_after_write_timeout_ms() -> u64 {
    500
}

fn default_replica_health_interval_ms() -> u64 {
    1_000
}

fn default_replica_health_timeout_ms() -> u64 {
    500
}

fn deserialize_read_routing_mode<'de, D>(deserializer: D) -> Result<ReadRoutingMode, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_read_routing_mode(&value).map_err(serde::de::Error::custom)
}

fn serialize_read_routing_mode<S>(mode: &ReadRoutingMode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(mode.as_str())
}

fn parse_read_routing_mode(value: &str) -> Result<ReadRoutingMode, String> {
    match value {
        "off" => Ok(ReadRoutingMode::Off),
        "prefer_replica" => Ok(ReadRoutingMode::PreferReplica),
        "require_replica" => Ok(ReadRoutingMode::RequireReplica),
        "primary_only" => Ok(ReadRoutingMode::PrimaryOnly),
        other => Err(format!("invalid read routing mode '{other}'")),
    }
}

fn deserialize_fallback_policy<'de, D>(deserializer: D) -> Result<FallbackPolicy, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_fallback_policy(&value).map_err(serde::de::Error::custom)
}

fn serialize_fallback_policy<S>(mode: &FallbackPolicy, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(mode.as_str())
}

fn parse_fallback_policy(value: &str) -> Result<FallbackPolicy, String> {
    match value {
        "primary" => Ok(FallbackPolicy::Primary),
        "reject" => Ok(FallbackPolicy::Reject),
        "wait" => Ok(FallbackPolicy::Wait),
        other => Err(format!("invalid fallback policy '{other}'")),
    }
}

fn deserialize_freshness_policy<'de, D>(deserializer: D) -> Result<FreshnessPolicy, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_freshness_policy(&value).map_err(serde::de::Error::custom)
}

fn serialize_freshness_policy<S>(mode: &FreshnessPolicy, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(mode.as_str())
}

fn parse_freshness_policy(value: &str) -> Result<FreshnessPolicy, String> {
    match value {
        "none" => Ok(FreshnessPolicy::None),
        "session_write_lsn" => Ok(FreshnessPolicy::SessionWriteLsn),
        "max_replica_lag" => Ok(FreshnessPolicy::MaxReplicaLag),
        "session_write_lsn_and_max_lag" => Ok(FreshnessPolicy::SessionWriteLsnAndMaxLag),
        other => Err(format!("invalid freshness policy '{other}'")),
    }
}

fn default_policy_mode() -> PolicyMode {
    PolicyMode::Disabled
}

fn default_policy_audit_enabled() -> bool {
    true
}

fn default_policy_audit_sample_rate() -> f64 {
    1.0
}

fn default_policy_eval_timeout_ms() -> u64 {
    5
}

fn default_policy_max_context_bytes() -> usize {
    8_192
}

fn default_policy_id() -> PolicyId {
    PolicyId::new("policy").expect("valid default policy id")
}

fn default_policy_hook_point() -> PolicyHookPoint {
    PolicyHookPoint::BeforeRouting
}

fn deserialize_policy_mode<'de, D>(deserializer: D) -> Result<PolicyMode, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_policy_mode(&value).map_err(serde::de::Error::custom)
}

fn parse_duration_ms(value: &str) -> Result<Duration, String> {
    value
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|error| format!("invalid duration in milliseconds: {error}"))
}

fn deserialize_duration_ms<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    u64::deserialize(deserializer)
        .map(Duration::from_millis)
        .map_err(serde::de::Error::custom)
}

fn serialize_duration_ms<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u64(duration.as_millis().try_into().unwrap_or(u64::MAX))
}

fn serialize_policy_mode<S>(mode: &PolicyMode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(mode.as_str())
}

fn parse_policy_mode(value: &str) -> Result<PolicyMode, String> {
    match value {
        "disabled" => Ok(PolicyMode::Disabled),
        "enforce" => Ok(PolicyMode::Enforce),
        "dry_run" => Ok(PolicyMode::DryRun),
        other => Err(format!("invalid policy mode '{other}'")),
    }
}

fn deserialize_policy_hook_point<'de, D>(deserializer: D) -> Result<PolicyHookPoint, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_policy_hook_point(&value).map_err(serde::de::Error::custom)
}

fn serialize_policy_hook_point<S>(
    hook_point: &PolicyHookPoint,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(hook_point.as_str())
}

fn parse_policy_hook_point(value: &str) -> Result<PolicyHookPoint, String> {
    match value {
        "before_routing" => Ok(PolicyHookPoint::BeforeRouting),
        "after_routing" => Ok(PolicyHookPoint::AfterRouting),
        "before_checkout" => Ok(PolicyHookPoint::BeforeCheckout),
        other => Err(format!("invalid policy hook point '{other}'")),
    }
}

fn deserialize_policy_id<'de, D>(deserializer: D) -> Result<PolicyId, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    PolicyId::from_str(&value).map_err(|error| serde::de::Error::custom(error.to_string()))
}

fn serialize_policy_id<S>(policy_id: &PolicyId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(policy_id.as_str())
}

fn deserialize_policy_route_target_id<'de, D>(
    deserializer: D,
) -> Result<PolicyRouteTargetId, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    PolicyRouteTargetId::from_str(&value)
        .map_err(|error| serde::de::Error::custom(error.to_string()))
}

fn serialize_policy_route_target_id<S>(
    target_id: &PolicyRouteTargetId,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(target_id.as_str())
}

fn deserialize_policy_shard_target_id<'de, D>(
    deserializer: D,
) -> Result<PolicyShardTargetId, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    PolicyShardTargetId::from_str(&value)
        .map_err(|error| serde::de::Error::custom(error.to_string()))
}

fn serialize_policy_shard_target_id<S>(
    target_id: &PolicyShardTargetId,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(target_id.as_str())
}

fn default_node_id() -> NodeId {
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| String::from("localhost"));
    let hostname = hostname
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    NodeId::new(format!("{hostname}-{}", std::process::id())).expect("generated node id is valid")
}

fn deserialize_node_id<'de, D>(deserializer: D) -> Result<NodeId, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    NodeId::from_str(&value).map_err(serde::de::Error::custom)
}

fn serialize_node_id<S>(node_id: &NodeId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(node_id.as_str())
}

const fn default_runtime_engine() -> RuntimeEngine {
    RuntimeEngine::ThreadPerCore
}

fn parse_runtime_engine(value: &str) -> Result<RuntimeEngine, String> {
    match value {
        "tokio_default" => Ok(RuntimeEngine::TokioDefault),
        "tokio_current_thread" => Ok(RuntimeEngine::TokioCurrentThread),
        "thread_per_core" => Ok(RuntimeEngine::ThreadPerCore),
        "experimental_io_uring" => Ok(RuntimeEngine::ExperimentalIoUring),
        _ => Err(format!(
            "unsupported runtime engine '{value}', expected one of: tokio_default, tokio_current_thread, thread_per_core, experimental_io_uring"
        )),
    }
}

fn deserialize_runtime_engine<'de, D>(deserializer: D) -> Result<RuntimeEngine, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_runtime_engine(&value).map_err(serde::de::Error::custom)
}

fn serialize_runtime_engine<S>(engine: &RuntimeEngine, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(engine.as_str())
}

fn default_adaptive_mode() -> AdaptiveMode {
    AdaptiveMode::Recommend
}

fn parse_adaptive_mode(value: &str) -> Result<AdaptiveMode, String> {
    value.parse()
}

fn deserialize_adaptive_mode<'de, D>(deserializer: D) -> Result<AdaptiveMode, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_adaptive_mode(&value).map_err(serde::de::Error::custom)
}

fn serialize_adaptive_mode<S>(mode: &AdaptiveMode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(mode.as_str())
}

fn parse_tunable_knob(value: &str) -> Result<TunableKnob, String> {
    value.parse()
}

fn deserialize_tunable_knob_list<'de, D>(deserializer: D) -> Result<Vec<TunableKnob>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<String>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(|value| parse_tunable_knob(&value).map_err(serde::de::Error::custom))
        .collect()
}

fn serialize_tunable_knob_list<S>(knobs: &[TunableKnob], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_seq(knobs.iter().map(|knob| knob.as_str()))
}

fn default_mirror_mode() -> MirrorMode {
    MirrorMode::Off
}

fn parse_mirror_mode(value: &str) -> Result<MirrorMode, String> {
    match value {
        "off" => Ok(MirrorMode::Off),
        "read_only" => Ok(MirrorMode::ReadOnly),
        "explicit" => Ok(MirrorMode::Explicit),
        _ => Err(format!("invalid mirror mode '{value}'")),
    }
}

fn deserialize_mirror_mode<'de, D>(deserializer: D) -> Result<MirrorMode, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_mirror_mode(&value).map_err(serde::de::Error::custom)
}

fn serialize_mirror_mode<S>(mode: &MirrorMode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(mode.as_str())
}

#[cfg(test)]
mod tests {
    use super::{
        AuthFailureMessageMode, AuthMode, BackendEndpointConfig, BackendTlsMode, ClientTlsMode,
        Config, FallbackPolicy, FreshnessConfig, FreshnessPolicy, HaConfig, PoolLifecycleConfig,
        PoolMode, ReadRoutingConfig, ReadRoutingMode, ReplicaConfig, RouteConfig, SocketConfig,
    };
    use crate::snapshot::SettingsSnapshot;
    use clap::Parser;
    use std::{net::SocketAddr, path::PathBuf, time::Duration};

    #[test]
    fn config_parses_defaults() {
        let config = Config::try_parse_from(["pg-kinetic"]).expect("defaults parse");

        assert_eq!(
            config.connection.listen_addr,
            "127.0.0.1:6543"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(
            config.connection.backend_addr,
            "127.0.0.1:5432"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(config.capacity.max_clients, 10_000);
        assert_eq!(config.capacity.max_backends, 100);
        assert_eq!(config.capacity.max_checkout_waiters, 1_000);
        assert_eq!(config.pool_lifecycle, PoolLifecycleConfig::default());
        assert_eq!(config.routes.len(), 0);
        assert_eq!(config.pools.len(), 0);
        assert!(!config.runtime.node.node_id.as_str().is_empty());
        assert_eq!(
            config.runtime.engine.runtime_engine,
            pg_kinetic_core::runtime::RuntimeEngine::ThreadPerCore
        );
        assert!(!config.runtime.engine.experimental_runtime_enabled);
        assert_eq!(config.runtime.engine.runtime_shards, None);
        assert_eq!(config.runtime.lifecycle.startup_grace_ms, 30_000);
        assert_eq!(config.drain.drain_grace(), Duration::from_secs(30));
        assert_eq!(config.runtime.lifecycle.shutdown_grace_ms, 30_000);
        assert!(config.runtime.lifecycle.readiness_fail_during_drain);
        assert!(config.runtime.lifecycle.pre_stop_drain_enabled);
        assert_eq!(config.runtime.lifecycle.pre_stop_drain_endpoint, "/drain");
        assert!(config.runtime.lifecycle.startup_backend_checks_enabled);
        assert_eq!(
            config.runtime.lifecycle.termination_grace_period(),
            Duration::from_secs(65)
        );
        assert!(!config.runtime.production.control_plane_enabled);
        assert!(!config.runtime.production.mirroring_enabled);
        assert!(!config.runtime.production.adaptive_enabled);
        assert_eq!(
            config.performance.checkout_timeout(),
            Duration::from_secs(1)
        );
        assert_eq!(config.performance.pool_mode, PoolMode::Transaction);
        assert_eq!(
            config.performance.recovery_mode,
            pg_kinetic_core::recovery::RecoveryMode::Recover
        );
        assert_eq!(
            config.performance.recovery_timeout(),
            Duration::from_secs(5)
        );
        assert_eq!(config.performance.backend_reset_query, "DISCARD ALL");
        assert_eq!(config.qos.max_route_in_flight, 100);
        assert_eq!(config.qos.max_route_waiters, 1_000);
        assert_eq!(config.qos.query_timeout(), Duration::from_secs(30));
        assert_eq!(config.qos.idle_client_timeout(), Duration::from_secs(300));
        assert_eq!(
            config.qos.idle_transaction_timeout(),
            Duration::from_secs(60)
        );
        assert_eq!(config.qos.max_client_buffer_bytes, 1_048_576);
        assert_eq!(config.qos.max_backend_buffer_bytes, 4_194_304);
        assert_eq!(config.qos.overload_error_code, "53300");
        assert_eq!(config.admin.admin_addr, None);
        assert!(!config.admin.admin_require_tls);
        assert_eq!(config.admin.admin_allowed_user, None);
        assert_eq!(config.admin.admin_query_timeout_ms, 1_000);
        assert_eq!(config.admin.admin_max_clients, 8);
        assert_eq!(config.observability.metrics_addr, None);
        assert_eq!(config.observability.debug_trace_sampling_rate, 0.0);
        assert_eq!(config.observability.phase_timing_sample_rate, 0.0);
        assert!(!config.observability.otel_enabled);
        assert_eq!(config.observability.otel_endpoint, None);
        assert_eq!(config.observability.otel_service_name, "pg-kinetic");

        assert_eq!(config.tls.client_tls_mode, ClientTlsMode::Disable);
        assert_eq!(config.tls.client_cert_path, None);
        assert_eq!(config.tls.client_key_path, None);
        assert_eq!(config.tls.client_ca_path, None);
        assert_eq!(config.tls.backend_tls_mode, BackendTlsMode::Disable);
        assert_eq!(config.tls.backend_ca_path, None);
        assert_eq!(config.tls.backend_server_name, None);

        assert_eq!(config.auth.auth_mode, AuthMode::PassThrough);
        assert_eq!(config.auth.auth_users_file, None);
        assert_eq!(config.auth.backend_user, None);
        assert_eq!(config.auth.backend_password_env_var_name, None);
        assert_eq!(
            config.auth.auth_failure_message_mode,
            AuthFailureMessageMode::Generic
        );

        assert_eq!(config.reload.config_file, None);
        assert_eq!(config.reload.config_reload_interval_ms, 5_000);
        assert!(!config.reload.reload_enabled);

        assert_eq!(config.drain.drain_timeout_ms, 30_000);
        assert!(!config.drain.reject_new_clients_during_drain);

        assert_eq!(config.health.health_addr, None);
        assert_eq!(config.health.readiness_backend_check_interval_ms, 1_000);
        assert_eq!(config.health.readiness_timeout_ms, 5_000);

        assert!(config.socket.tcp_nodelay);
        assert!(!config.socket.tcp_keepalive);
        assert_eq!(config.socket.tcp_keepalive_idle_ms, None);
        assert_eq!(config.socket.tcp_keepalive_interval_ms, None);
        assert_eq!(config.socket.tcp_keepalive_retries, None);
        assert_eq!(config.socket.tcp_user_timeout_ms, None);
        assert_eq!(config.socket.tcp_send_buffer_bytes, None);
        assert_eq!(config.socket.tcp_recv_buffer_bytes, None);
        assert!(!config.socket.strict_socket_option_mode);
    }

    #[test]
    fn reload_compatibility_rejects_restart_required_pool_settings() {
        let current = Config::default();

        let mut next = current.clone();
        next.pool_lifecycle.max_size += 1;
        assert!(!current.is_reload_compatible_with(&next));

        let mut next = current.clone();
        next.performance.checkout_timeout_ms += 1;
        assert!(!current.is_reload_compatible_with(&next));

        let mut next = current.clone();
        next.qos.max_route_in_flight += 1;
        assert!(!current.is_reload_compatible_with(&next));

        let mut next = current.clone();
        next.tls.client_cert_path = Some("changed-cert.pem".into());
        assert!(!current.is_reload_compatible_with(&next));

        let mut next = current.clone();
        next.tls.client_key_path = Some("changed-key.pem".into());
        assert!(!current.is_reload_compatible_with(&next));

        let mut next = current.clone();
        next.tls.client_ca_path = Some("changed-ca.pem".into());
        assert!(!current.is_reload_compatible_with(&next));
    }

    #[test]
    fn reload_compatibility_allows_query_timeout_changes() {
        let current = Config::default();
        let mut next = current.clone();
        next.qos.query_timeout_ms += 1;

        assert!(current.is_reload_compatible_with(&next));
    }

    #[test]
    fn rejects_zero_global_backend_capacity() {
        let mut config = Config::default();
        config.capacity.max_backends = 0;

        let error = config
            .validate()
            .expect_err("zero backend capacity must fail");

        assert_eq!(error, "capacity.max_backends must be greater than zero");
    }

    #[test]
    fn runtime_config_parses_lifecycle_and_production_settings() {
        let config = toml::from_str::<Config>(
            r#"
            [runtime.lifecycle]
            startup_grace_ms = 1_000
            shutdown_grace_ms = 3_000
            readiness_fail_during_drain = false
            pre_stop_drain_enabled = false
            pre_stop_drain_endpoint = "/lifecycle/drain"
            startup_backend_checks_enabled = false
            termination_grace_period_seconds = 90

            [runtime.node]
            node_id = "proxy-a"

            [runtime.engine]
            runtime_engine = "tokio_current_thread"

            [runtime.production]
            control_plane_enabled = true
            mirroring_enabled = true
            adaptive_enabled = true

            [drain]
            drain_grace_ms = 2_000
            "#,
        )
        .expect("runtime config parses");

        assert_eq!(
            config.runtime.lifecycle.startup_grace(),
            Duration::from_secs(1)
        );
        assert_eq!(config.drain.drain_grace(), Duration::from_secs(2));
        assert_eq!(
            config.runtime.lifecycle.shutdown_grace(),
            Duration::from_secs(3)
        );
        assert!(!config.runtime.lifecycle.readiness_fail_during_drain);
        assert!(!config.runtime.lifecycle.pre_stop_drain_enabled);
        assert_eq!(
            config.runtime.lifecycle.pre_stop_drain_endpoint,
            "/lifecycle/drain"
        );
        assert!(!config.runtime.lifecycle.startup_backend_checks_enabled);
        assert_eq!(
            config.runtime.lifecycle.termination_grace_period(),
            Duration::from_secs(90)
        );
        assert_eq!(config.runtime.node.node_id.as_str(), "proxy-a");
        assert_eq!(
            config.runtime.engine.runtime_engine,
            pg_kinetic_core::runtime::RuntimeEngine::TokioCurrentThread
        );
        assert!(config.runtime.production.control_plane_enabled);
        assert!(config.runtime.production.mirroring_enabled);
        assert!(config.runtime.production.adaptive_enabled);
    }

    #[test]
    fn experimental_runtime_engines_require_explicit_config_gate() {
        let error = toml::from_str::<Config>(
            r#"
            [runtime.engine]
            runtime_engine = "experimental_io_uring"
            "#,
        )
        .expect_err("ungated experimental runtime is rejected");
        assert!(error.to_string().contains("experimental_runtime_enabled"));

        let error = toml::from_str::<Config>(
            r#"
            [runtime.engine]
            runtime_engine = "thread_per_core"
            runtime_shards = 0
            "#,
        )
        .expect_err("zero runtime shards are rejected");
        assert!(error.to_string().contains("runtime_shards"));
    }

    #[test]
    fn stable_thread_per_core_parses_without_experimental_config_gate() {
        let config = toml::from_str::<Config>(
            r#"
            [runtime.engine]
            runtime_engine = "thread_per_core"
            runtime_shards = 2
            "#,
        )
        .expect("stable thread-per-core runtime parses");

        assert_eq!(
            config.runtime.engine.runtime_engine,
            pg_kinetic_core::runtime::RuntimeEngine::ThreadPerCore
        );
        assert!(!config.runtime.engine.experimental_runtime_enabled);
        assert_eq!(config.runtime.engine.runtime_shards, Some(2));
    }

    #[test]
    fn runtime_flags_parse_through_the_single_config_tree() {
        let config = Config::try_parse_from_args([
            "pg-kinetic",
            "--node-id",
            "proxy-b",
            "--startup-grace-ms",
            "1000",
            "--drain-grace-ms",
            "2000",
            "--shutdown-grace-ms",
            "3000",
            "--readiness-fail-during-drain=false",
            "--pre-stop-drain-enabled=false",
            "--pre-stop-drain-endpoint",
            "/lifecycle/drain",
            "--startup-backend-checks-enabled=false",
            "--termination-grace-period-seconds",
            "90",
            "--control-plane-enabled",
            "--mirroring-enabled",
            "--runtime-shards",
            "2",
            "--adaptive-enabled",
        ])
        .expect("runtime flags parse");

        assert_eq!(config.runtime.node.node_id.as_str(), "proxy-b");
        assert_eq!(config.runtime.lifecycle.startup_grace_ms, 1_000);
        assert_eq!(config.drain.drain_timeout_ms, 2_000);
        assert_eq!(config.runtime.lifecycle.shutdown_grace_ms, 3_000);
        assert!(!config.runtime.lifecycle.readiness_fail_during_drain);
        assert!(!config.runtime.lifecycle.pre_stop_drain_enabled);
        assert_eq!(
            config.runtime.lifecycle.pre_stop_drain_endpoint,
            "/lifecycle/drain"
        );
        assert!(!config.runtime.lifecycle.startup_backend_checks_enabled);
        assert_eq!(
            config.runtime.lifecycle.termination_grace_period_seconds,
            90
        );
        assert!(config.runtime.production.control_plane_enabled);
        assert!(config.runtime.production.mirroring_enabled);
        assert_eq!(config.runtime.engine.runtime_shards, Some(2));
        assert!(config.runtime.production.adaptive_enabled);
    }

    #[test]
    fn config_maps_single_backend_to_default_route() {
        let config = Config::try_parse_from(["pg-kinetic"]).expect("defaults parse");

        let routes = config.route_configs();
        assert_eq!(routes.len(), 1);

        let route = &routes[0];
        assert_eq!(
            route.primary,
            BackendEndpointConfig {
                address: config.connection.backend_addr,
                connect_timeout_ms: 1_000,
                tls_mode: BackendTlsMode::Disable,
            }
        );
        assert!(route.replicas.is_empty());
        assert_eq!(route.read_routing.read_routing_mode, ReadRoutingMode::Off);
        assert_eq!(route.read_routing.fallback_policy, FallbackPolicy::Primary);
        assert_eq!(
            route.freshness,
            FreshnessConfig {
                freshness_policy: FreshnessPolicy::SessionWriteLsn,
                max_replica_lag_ms: 1_000,
                read_after_write_timeout_ms: 500,
            }
        );
        assert_eq!(
            route.ha,
            HaConfig {
                replica_health_interval_ms: 1_000,
                replica_health_timeout_ms: 500,
            }
        );
    }

    #[test]
    fn config_parses_explicit_flags() {
        let config = Config::try_parse_from([
            "pg-kinetic",
            "--listen-addr",
            "0.0.0.0:6432",
            "--backend-addr",
            "127.0.0.1:5433",
            "--max-clients",
            "500",
            "--max-backends",
            "25",
            "--max-checkout-waiters",
            "12",
            "--pool-max-size",
            "8",
            "--pool-min-idle",
            "2",
            "--pool-idle-timeout-ms",
            "1500",
            "--pool-max-lifetime-ms",
            "9000",
            "--checkout-timeout-ms",
            "250",
            "--pool-mode",
            "session",
            "--recovery-mode",
            "drop",
            "--recovery-timeout-ms",
            "7500",
            "--backend-reset-query",
            "DISCARD TEMP",
            "--max-route-in-flight",
            "7",
            "--max-route-waiters",
            "9",
            "--query-timeout-ms",
            "1234",
            "--idle-client-timeout-ms",
            "5678",
            "--idle-transaction-timeout-ms",
            "9012",
            "--max-client-buffer-bytes",
            "111",
            "--max-backend-buffer-bytes",
            "222",
            "--overload-error-code",
            "53301",
            "--admin-addr",
            "127.0.0.1:7000",
            "--admin-require-tls",
            "--admin-allowed-user",
            "admin",
            "--admin-query-timeout-ms",
            "2222",
            "--admin-max-clients",
            "16",
            "--debug-trace-sampling-rate",
            "0.25",
            "--phase-timing-sample-rate",
            "0.75",
            "--otel-enabled",
            "--otel-endpoint",
            "http://otel.example.com:4318",
            "--otel-service-name",
            "pg-kinetic-proxy",
            "--client-tls-mode",
            "verify_client",
            "--client-cert-path",
            "client-cert.pem",
            "--client-key-path",
            "client-key.pem",
            "--client-ca-path",
            "client-ca.pem",
            "--backend-tls-mode",
            "verify_full",
            "--backend-ca-path",
            "backend-ca.pem",
            "--backend-server-name",
            "db.example.com",
            "--auth-mode",
            "scram_sha_256",
            "--auth-users-file",
            "auth-users.toml",
            "--backend-user",
            "proxy_user",
            "--backend-password-env-var-name",
            "PG_KINETIC_BACKEND_PASSWORD",
            "--auth-failure-message-mode",
            "detailed",
            "--config-file",
            "pg-kinetic.toml",
            "--config-reload-interval-ms",
            "7500",
            "--reload-enabled",
            "--drain-timeout-ms",
            "45000",
            "--reject-new-clients-during-drain",
            "--health-addr",
            "127.0.0.1:9091",
            "--readiness-backend-check-interval-ms",
            "333",
            "--readiness-timeout-ms",
            "4444",
            "--tcp-keepalive",
            "--tcp-keepalive-idle-ms",
            "1111",
            "--tcp-keepalive-interval-ms",
            "2222",
            "--tcp-keepalive-retries",
            "3",
            "--tcp-user-timeout-ms",
            "3333",
            "--tcp-send-buffer-bytes",
            "4444",
            "--tcp-recv-buffer-bytes",
            "5555",
            "--strict-socket-option-mode",
        ])
        .expect("flags parse");

        assert_eq!(
            config.connection.listen_addr,
            "0.0.0.0:6432".parse::<SocketAddr>().expect("valid socket")
        );
        assert_eq!(
            config.connection.backend_addr,
            "127.0.0.1:5433"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(config.capacity.max_clients, 500);
        assert_eq!(config.capacity.max_backends, 25);
        assert_eq!(config.capacity.max_checkout_waiters, 12);
        assert_eq!(config.pool_lifecycle.max_size, 8);
        assert_eq!(config.pool_lifecycle.min_idle, 2);
        assert_eq!(
            config.pool_lifecycle.idle_timeout,
            Duration::from_millis(1_500)
        );
        assert_eq!(
            config.pool_lifecycle.max_lifetime,
            Duration::from_millis(9_000)
        );
        assert_eq!(
            config.performance.checkout_timeout(),
            Duration::from_millis(250)
        );
        assert_eq!(config.performance.pool_mode, PoolMode::Session);
        assert_eq!(
            config.performance.recovery_mode,
            pg_kinetic_core::recovery::RecoveryMode::Drop
        );
        assert_eq!(
            config.performance.recovery_timeout(),
            Duration::from_millis(7_500)
        );
        assert_eq!(config.performance.backend_reset_query, "DISCARD TEMP");
        assert_eq!(config.qos.max_route_in_flight, 7);
        assert_eq!(config.qos.max_route_waiters, 9);
        assert_eq!(config.qos.query_timeout(), Duration::from_millis(1_234));
        assert_eq!(
            config.qos.idle_client_timeout(),
            Duration::from_millis(5_678)
        );
        assert_eq!(
            config.qos.idle_transaction_timeout(),
            Duration::from_millis(9_012)
        );
        assert_eq!(config.qos.max_client_buffer_bytes, 111);
        assert_eq!(config.qos.max_backend_buffer_bytes, 222);
        assert_eq!(config.qos.overload_error_code, "53301");
        assert_eq!(
            config.admin.admin_addr,
            Some("127.0.0.1:7000".parse().expect("valid socket"))
        );
        assert!(config.admin.admin_require_tls);
        assert_eq!(config.admin.admin_allowed_user, Some(String::from("admin")));
        assert_eq!(config.admin.admin_query_timeout_ms, 2_222);
        assert_eq!(config.admin.admin_max_clients, 16);
        assert_eq!(config.observability.debug_trace_sampling_rate, 0.25);
        assert_eq!(config.observability.phase_timing_sample_rate, 0.75);
        assert!(config.observability.otel_enabled);
        assert_eq!(
            config.observability.otel_endpoint,
            Some(String::from("http://otel.example.com:4318"))
        );
        assert_eq!(config.observability.otel_service_name, "pg-kinetic-proxy");

        assert_eq!(config.tls.client_tls_mode, ClientTlsMode::VerifyClient);
        assert_eq!(
            config.tls.client_cert_path,
            Some(PathBuf::from("client-cert.pem"))
        );
        assert_eq!(
            config.tls.client_key_path,
            Some(PathBuf::from("client-key.pem"))
        );
        assert_eq!(
            config.tls.client_ca_path,
            Some(PathBuf::from("client-ca.pem"))
        );
        assert_eq!(config.tls.backend_tls_mode, BackendTlsMode::VerifyFull);
        assert_eq!(
            config.tls.backend_ca_path,
            Some(PathBuf::from("backend-ca.pem"))
        );
        assert_eq!(
            config.tls.backend_server_name,
            Some(String::from("db.example.com"))
        );

        assert_eq!(config.auth.auth_mode, AuthMode::ScramSha256);
        assert_eq!(
            config.auth.auth_users_file,
            Some(PathBuf::from("auth-users.toml"))
        );
        assert_eq!(config.auth.backend_user, Some(String::from("proxy_user")));
        assert_eq!(
            config.auth.backend_password_env_var_name,
            Some(String::from("PG_KINETIC_BACKEND_PASSWORD"))
        );
        assert_eq!(
            config.auth.auth_failure_message_mode,
            AuthFailureMessageMode::Detailed
        );

        assert_eq!(
            config.reload.config_file,
            Some(PathBuf::from("pg-kinetic.toml"))
        );
        assert_eq!(config.reload.config_reload_interval_ms, 7_500);
        assert!(config.reload.reload_enabled);

        assert_eq!(config.drain.drain_timeout_ms, 45_000);
        assert!(config.drain.reject_new_clients_during_drain);

        assert_eq!(
            config.health.health_addr,
            Some("127.0.0.1:9091".parse().expect("valid socket"))
        );
        assert_eq!(config.health.readiness_backend_check_interval_ms, 333);
        assert_eq!(config.health.readiness_timeout_ms, 4_444);

        assert!(config.socket.tcp_nodelay);
        assert!(config.socket.tcp_keepalive);
        assert_eq!(config.socket.tcp_keepalive_idle_ms, Some(1_111));
        assert_eq!(config.socket.tcp_keepalive_interval_ms, Some(2_222));
        assert_eq!(config.socket.tcp_keepalive_retries, Some(3));
        assert_eq!(config.socket.tcp_user_timeout_ms, Some(3_333));
        assert_eq!(config.socket.tcp_send_buffer_bytes, Some(4_444));
        assert_eq!(config.socket.tcp_recv_buffer_bytes, Some(5_555));
        assert!(config.socket.strict_socket_option_mode);
    }

    #[test]
    fn route_config_parses_primary_and_replicas() {
        let config = toml::from_str::<Config>(
            r#"
            [connection]
            listen_addr = "0.0.0.0:6432"
            backend_addr = "127.0.0.1:5432"

            [[routes]]
            [routes.primary]
            address = "10.0.0.1:5432"
            connect_timeout_ms = 750
            tls_mode = "require"

            [[routes.replicas]]
            address = "10.0.0.2:5432"
            connect_timeout_ms = 250
            tls_mode = "prefer"
            weight = 2

            [[routes.replicas]]
            address = "10.0.0.3:5432"
            connect_timeout_ms = 125
            tls_mode = "verify_full"

            [routes.read_routing]
            read_routing_mode = "prefer_replica"
            fallback_policy = "wait"

            [routes.freshness]
            freshness_policy = "session_write_lsn_and_max_lag"
            max_replica_lag_ms = 2_500
            read_after_write_timeout_ms = 750

            [routes.ha]
            replica_health_interval_ms = 2_000
            replica_health_timeout_ms = 750
            "#,
        )
        .expect("route config parses");

        assert_eq!(config.routes.len(), 1);
        let route = &config.routes[0];

        assert_eq!(
            route.primary,
            BackendEndpointConfig {
                address: "10.0.0.1:5432".parse().expect("valid socket"),
                connect_timeout_ms: 750,
                tls_mode: BackendTlsMode::Require,
            }
        );
        assert_eq!(
            route.replicas,
            vec![
                ReplicaConfig {
                    address: "10.0.0.2:5432".parse().expect("valid socket"),
                    connect_timeout_ms: 250,
                    tls_mode: BackendTlsMode::Prefer,
                    weight: 2,
                },
                ReplicaConfig {
                    address: "10.0.0.3:5432".parse().expect("valid socket"),
                    connect_timeout_ms: 125,
                    tls_mode: BackendTlsMode::VerifyFull,
                    weight: 1,
                },
            ]
        );
        assert_eq!(
            route.read_routing,
            ReadRoutingConfig {
                read_routing_mode: ReadRoutingMode::PreferReplica,
                fallback_policy: FallbackPolicy::Wait,
            }
        );
        assert_eq!(
            route.freshness,
            FreshnessConfig {
                freshness_policy: FreshnessPolicy::SessionWriteLsnAndMaxLag,
                max_replica_lag_ms: 2_500,
                read_after_write_timeout_ms: 750,
            }
        );
        assert_eq!(
            route.ha,
            HaConfig {
                replica_health_interval_ms: 2_000,
                replica_health_timeout_ms: 750,
            }
        );
    }

    #[test]
    fn parses_pools_section() {
        let config: Config = toml::from_str(
            r#"
            [connection]
            listen_addr = "127.0.0.1:6543"
            backend_addr = "127.0.0.1:5432"

            [[pools]]
            database = "db_a"
            user = "user_a"
            backend_addr = "127.0.0.1:5433"
            max_backends = 7
            "#,
        )
        .expect("parse");

        assert_eq!(config.pools.len(), 1);
        assert_eq!(config.pools[0].database, "db_a");
        assert_eq!(config.pools[0].user, "user_a");
        assert_eq!(
            config.pools[0].backend_addr,
            "127.0.0.1:5433".parse().expect("valid socket")
        );
        assert_eq!(config.pools[0].max_backends, Some(7));
    }

    #[test]
    fn rejects_zero_pool_max_backends() {
        let error = toml::from_str::<Config>(
            r#"
            [connection]
            listen_addr = "127.0.0.1:6543"
            backend_addr = "127.0.0.1:5432"

            [[pools]]
            database = "db_a"
            user = "user_a"
            backend_addr = "127.0.0.1:5433"
            max_backends = 0
            "#,
        )
        .expect_err("zero pool capacity must be rejected");

        assert!(error
            .to_string()
            .contains("max_backends must be greater than zero"));
    }

    #[test]
    fn route_config_requires_primary_when_replicas_exist() {
        let _error = toml::from_str::<Config>(
            r#"
            [[routes.replicas]]
            address = "10.0.0.2:5432"
            connect_timeout_ms = 250
            tls_mode = "prefer"
            "#,
        )
        .expect_err("missing primary is rejected");
    }

    #[test]
    fn settings_snapshot_keeps_password_sources_out_of_debug_output() {
        let mut config = Config::default();
        config.auth.backend_password_env_var_name =
            Some(String::from("PG_KINETIC_BACKEND_PASSWORD"));
        config.routes = vec![RouteConfig::from_backend_addr(
            "127.0.0.1:5433".parse().expect("valid socket"),
        )];

        let snapshot = SettingsSnapshot::from_config(&config);
        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("PG_KINETIC_BACKEND_PASSWORD"));
        assert!(!debug.contains("backend_password_env_var_name"));
    }

    #[test]
    fn config_converts_named_modes() {
        let config = Config::try_parse_from([
            "pg-kinetic",
            "--client-tls-mode",
            "require",
            "--backend-tls-mode",
            "prefer",
            "--auth-mode",
            "trust",
        ])
        .expect("flags parse");

        assert_eq!(
            config.tls.client_tls_mode_core(),
            pg_kinetic_core::security::ClientTlsMode::Require
        );
        assert_eq!(
            config.tls.backend_tls_mode_core(),
            pg_kinetic_core::security::BackendTlsMode::Prefer
        );
        assert_eq!(
            config.auth.auth_mode_core(),
            pg_kinetic_core::security::AuthMode::Trust
        );
    }

    #[test]
    fn socket_config_helpers_convert_millis() {
        let socket = SocketConfig {
            tcp_nodelay: true,
            tcp_keepalive: true,
            tcp_keepalive_idle_ms: Some(1_500),
            tcp_keepalive_interval_ms: Some(2_500),
            tcp_keepalive_retries: Some(4),
            tcp_user_timeout_ms: Some(3_500),
            tcp_send_buffer_bytes: Some(4_500),
            tcp_recv_buffer_bytes: Some(5_500),
            strict_socket_option_mode: false,
        };

        assert_eq!(
            socket.tcp_keepalive_idle(),
            Some(Duration::from_millis(1_500))
        );
        assert_eq!(
            socket.tcp_keepalive_interval(),
            Some(Duration::from_millis(2_500))
        );
        assert_eq!(
            socket.tcp_user_timeout(),
            Some(Duration::from_millis(3_500))
        );
    }
}
