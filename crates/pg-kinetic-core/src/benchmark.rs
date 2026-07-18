use std::{collections::HashSet, fmt, str::FromStr, sync::Arc};

use thiserror::Error;

/// Bounded label used when a benchmark scenario name would otherwise become a
/// user-controlled Prometheus label or administrative value.
pub const CONFIGURED_BENCHMARK_SCENARIO_LABEL: &str = "configured";
pub const REDACTED_BENCHMARK_DETAIL_LABEL: &str = "redacted";

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum BenchmarkComparison {
    #[default]
    DirectPostgreSQL,
    PgBouncer,
    PgDog,
    PgKinetic,
}

impl BenchmarkComparison {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectPostgreSQL => "direct_postgresql",
            Self::PgBouncer => "pgbouncer",
            Self::PgDog => "pgdog",
            Self::PgKinetic => "pg_kinetic",
        }
    }
}

impl fmt::Display for BenchmarkComparison {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BenchmarkComparison {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "direct_postgresql" | "postgresql" => Ok(Self::DirectPostgreSQL),
            "pgbouncer" => Ok(Self::PgBouncer),
            "pgdog" => Ok(Self::PgDog),
            "pg_kinetic" | "pg-kinetic" => Ok(Self::PgKinetic),
            other => Err(format!("unknown benchmark comparison '{other}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum BenchmarkDriver {
    #[default]
    PgBench,
    Psql,
    TokioPostgres,
    PgX,
    NodePg,
    Psycopg,
}

impl BenchmarkDriver {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PgBench => "pgbench",
            Self::Psql => "psql",
            Self::TokioPostgres => "tokio_postgres",
            Self::PgX => "pgx",
            Self::NodePg => "node_pg",
            Self::Psycopg => "psycopg",
        }
    }
}

impl fmt::Display for BenchmarkDriver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BenchmarkDriver {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pgbench" => Ok(Self::PgBench),
            "psql" => Ok(Self::Psql),
            "tokio_postgres" => Ok(Self::TokioPostgres),
            "pgx" => Ok(Self::PgX),
            "node_pg" => Ok(Self::NodePg),
            "psycopg" => Ok(Self::Psycopg),
            other => Err(format!("unknown benchmark driver '{other}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum BenchmarkWorkloadKind {
    #[default]
    SimpleQuery,
    ExtendedQuery,
    PreparedStatementReuse,
    TransactionPool,
    IdleClients,
    RoutingShardingPolicy,
}

impl BenchmarkWorkloadKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SimpleQuery => "simple_query",
            Self::ExtendedQuery => "extended_query",
            Self::PreparedStatementReuse => "prepared_statement_reuse",
            Self::TransactionPool => "transaction_pool",
            Self::IdleClients => "idle_clients",
            Self::RoutingShardingPolicy => "routing_sharding_policy",
        }
    }
}

impl fmt::Display for BenchmarkWorkloadKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BenchmarkWorkloadKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "simple_query" => Ok(Self::SimpleQuery),
            "extended_query" => Ok(Self::ExtendedQuery),
            "prepared_statement_reuse" => Ok(Self::PreparedStatementReuse),
            "transaction_pool" => Ok(Self::TransactionPool),
            "idle_clients" => Ok(Self::IdleClients),
            "routing_sharding_policy" => Ok(Self::RoutingShardingPolicy),
            other => Err(format!("unknown benchmark workload '{other}'")),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct BenchmarkTarget {
    label: Arc<str>,
    comparison: BenchmarkComparison,
    dsn: Arc<str>,
}

impl BenchmarkTarget {
    pub fn new(
        label: impl Into<Arc<str>>,
        comparison: BenchmarkComparison,
        dsn: impl Into<Arc<str>>,
    ) -> Result<Self, BenchmarkValidationError> {
        let label = label.into();
        if label.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyTargetLabel);
        }

        let dsn = dsn.into();
        if dsn.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyTargetDsn);
        }

        Ok(Self {
            label,
            comparison,
            dsn,
        })
    }

    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub const fn comparison(&self) -> BenchmarkComparison {
        self.comparison
    }

    /// Returns the bounded target dimension safe for metrics and admin output.
    #[must_use]
    pub const fn metric_label(&self) -> &'static str {
        self.comparison.as_str()
    }

    #[must_use]
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    #[must_use]
    pub fn redacted_dsn(&self) -> String {
        redact_connection_string(self.dsn())
    }

    fn validate(&self) -> Result<(), BenchmarkValidationError> {
        if self.label.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyTargetLabel);
        }
        if self.dsn.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyTargetDsn);
        }
        Ok(())
    }
}

impl fmt::Debug for BenchmarkTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BenchmarkTarget")
            .field("label", &self.label)
            .field("comparison", &self.comparison)
            .field("dsn", &self.redacted_dsn())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkScenarioMatrix {
    targets: Vec<BenchmarkTarget>,
}

impl BenchmarkScenarioMatrix {
    pub fn new(targets: Vec<BenchmarkTarget>) -> Result<Self, BenchmarkValidationError> {
        let matrix = Self { targets };
        matrix.validate()?;
        Ok(matrix)
    }

    #[must_use]
    pub fn targets(&self) -> &[BenchmarkTarget] {
        &self.targets
    }

    fn validate(&self) -> Result<(), BenchmarkValidationError> {
        if self.targets.is_empty() {
            return Err(BenchmarkValidationError::EmptyTargets);
        }

        let mut labels = HashSet::new();
        for target in &self.targets {
            target.validate()?;
            if !labels.insert(target.label()) {
                return Err(BenchmarkValidationError::DuplicateTargetLabel {
                    label: Arc::from(target.label()),
                });
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkWarmupConfig {
    duration_ms: u64,
}

impl BenchmarkWarmupConfig {
    pub fn new(duration_ms: u64) -> Result<Self, BenchmarkValidationError> {
        Ok(Self { duration_ms })
    }

    #[must_use]
    pub const fn duration_ms(self) -> u64 {
        self.duration_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkConnectionProfile {
    concurrency: u32,
    connection_count: u32,
}

impl BenchmarkConnectionProfile {
    pub fn new(concurrency: u32, connection_count: u32) -> Result<Self, BenchmarkValidationError> {
        if concurrency == 0 {
            return Err(BenchmarkValidationError::InvalidConnectionProfile {
                field: "concurrency",
            });
        }
        if connection_count == 0 {
            return Err(BenchmarkValidationError::InvalidConnectionProfile {
                field: "connection_count",
            });
        }

        Ok(Self {
            concurrency,
            connection_count,
        })
    }

    #[must_use]
    pub const fn concurrency(self) -> u32 {
        self.concurrency
    }

    #[must_use]
    pub const fn connection_count(self) -> u32 {
        self.connection_count
    }
}

impl Default for BenchmarkConnectionProfile {
    fn default() -> Self {
        Self {
            concurrency: 16,
            connection_count: 16,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchmarkFeatureToggleSet {
    read_routing: bool,
    sharding: bool,
    policy_overhead: bool,
}

impl BenchmarkFeatureToggleSet {
    #[must_use]
    pub const fn new(read_routing: bool, sharding: bool, policy_overhead: bool) -> Self {
        Self {
            read_routing,
            sharding,
            policy_overhead,
        }
    }

    #[must_use]
    pub const fn read_routing(self) -> bool {
        self.read_routing
    }

    #[must_use]
    pub const fn sharding(self) -> bool {
        self.sharding
    }

    #[must_use]
    pub const fn policy_overhead(self) -> bool {
        self.policy_overhead
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkExpectedMetricSet {
    latency: bool,
    throughput: bool,
    cpu: bool,
    memory: bool,
    error_rate: bool,
}

impl BenchmarkExpectedMetricSet {
    pub fn new(
        latency: bool,
        throughput: bool,
        cpu: bool,
        memory: bool,
        error_rate: bool,
    ) -> Result<Self, BenchmarkValidationError> {
        let metrics = Self {
            latency,
            throughput,
            cpu,
            memory,
            error_rate,
        };
        if !metrics.any_enabled() {
            return Err(BenchmarkValidationError::EmptyExpectedMetrics);
        }
        Ok(metrics)
    }

    #[must_use]
    pub const fn latency(self) -> bool {
        self.latency
    }

    #[must_use]
    pub const fn throughput(self) -> bool {
        self.throughput
    }

    #[must_use]
    pub const fn cpu(self) -> bool {
        self.cpu
    }

    #[must_use]
    pub const fn memory(self) -> bool {
        self.memory
    }

    #[must_use]
    pub const fn error_rate(self) -> bool {
        self.error_rate
    }

    #[must_use]
    pub const fn any_enabled(self) -> bool {
        self.latency || self.throughput || self.cpu || self.memory || self.error_rate
    }
}

impl Default for BenchmarkExpectedMetricSet {
    fn default() -> Self {
        Self {
            latency: true,
            throughput: true,
            cpu: true,
            memory: true,
            error_rate: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkScenario {
    name: Arc<str>,
    driver: BenchmarkDriver,
    workload: BenchmarkWorkloadKind,
    duration_ms: u64,
    warmup: BenchmarkWarmupConfig,
    matrix: BenchmarkScenarioMatrix,
    connections: BenchmarkConnectionProfile,
    features: BenchmarkFeatureToggleSet,
    expected_metrics: BenchmarkExpectedMetricSet,
}

impl BenchmarkScenario {
    pub fn new(
        name: impl Into<Arc<str>>,
        driver: BenchmarkDriver,
        duration_ms: u64,
        warmup_ms: u64,
        targets: Vec<BenchmarkTarget>,
    ) -> Result<Self, BenchmarkValidationError> {
        Self::new_with_configuration(
            name,
            driver,
            BenchmarkWorkloadKind::default(),
            duration_ms,
            BenchmarkWarmupConfig::new(warmup_ms)?,
            BenchmarkScenarioMatrix::new(targets)?,
            BenchmarkConnectionProfile::default(),
            BenchmarkFeatureToggleSet::default(),
            BenchmarkExpectedMetricSet::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_configuration(
        name: impl Into<Arc<str>>,
        driver: BenchmarkDriver,
        workload: BenchmarkWorkloadKind,
        duration_ms: u64,
        warmup: BenchmarkWarmupConfig,
        matrix: BenchmarkScenarioMatrix,
        connections: BenchmarkConnectionProfile,
        features: BenchmarkFeatureToggleSet,
        expected_metrics: BenchmarkExpectedMetricSet,
    ) -> Result<Self, BenchmarkValidationError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyScenarioName);
        }
        if duration_ms == 0 {
            return Err(BenchmarkValidationError::InvalidDuration {
                field: "duration_ms",
            });
        }
        let scenario = Self {
            name,
            driver,
            workload,
            duration_ms,
            warmup,
            matrix,
            connections,
            features,
            expected_metrics,
        };
        scenario.validate()?;
        Ok(scenario)
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Scenario documents permit descriptive names, which are not safe metric labels.
    #[must_use]
    pub const fn metric_label(&self) -> &'static str {
        CONFIGURED_BENCHMARK_SCENARIO_LABEL
    }

    #[must_use]
    pub const fn driver(&self) -> BenchmarkDriver {
        self.driver
    }

    #[must_use]
    pub const fn workload(&self) -> BenchmarkWorkloadKind {
        self.workload
    }

    #[must_use]
    pub const fn duration_ms(&self) -> u64 {
        self.duration_ms
    }

    #[must_use]
    pub const fn warmup_ms(&self) -> u64 {
        self.warmup.duration_ms()
    }

    #[must_use]
    pub const fn warmup(&self) -> BenchmarkWarmupConfig {
        self.warmup
    }

    #[must_use]
    pub const fn matrix(&self) -> &BenchmarkScenarioMatrix {
        &self.matrix
    }

    #[must_use]
    pub const fn connections(&self) -> BenchmarkConnectionProfile {
        self.connections
    }

    #[must_use]
    pub const fn features(&self) -> BenchmarkFeatureToggleSet {
        self.features
    }

    #[must_use]
    pub const fn expected_metrics(&self) -> BenchmarkExpectedMetricSet {
        self.expected_metrics
    }

    #[must_use]
    pub fn targets(&self) -> &[BenchmarkTarget] {
        self.matrix.targets()
    }

    pub fn validate(&self) -> Result<(), BenchmarkValidationError> {
        if self.name.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyScenarioName);
        }
        if self.duration_ms == 0 {
            return Err(BenchmarkValidationError::InvalidDuration {
                field: "duration_ms",
            });
        }
        if self.warmup.duration_ms() > self.duration_ms {
            return Err(BenchmarkValidationError::WarmupExceedsDuration);
        }
        self.matrix.validate()?;
        if !self.expected_metrics.any_enabled() {
            return Err(BenchmarkValidationError::EmptyExpectedMetrics);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BenchmarkMetric {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    throughput_qps: f64,
    cpu_per_query: f64,
    cpu_label: Arc<str>,
    memory_per_client_bytes: f64,
    memory_label: Arc<str>,
    error_rate: f64,
}

impl BenchmarkMetric {
    pub fn new(
        p50_ms: f64,
        p95_ms: f64,
        p99_ms: f64,
        throughput_qps: f64,
        cpu_label: impl Into<Arc<str>>,
        memory_label: impl Into<Arc<str>>,
        error_rate: f64,
    ) -> Result<Self, BenchmarkValidationError> {
        let cpu_label = cpu_label.into();
        let memory_label = memory_label.into();
        let metric = Self {
            p50_ms,
            p95_ms,
            p99_ms,
            p999_ms: p99_ms,
            throughput_qps,
            cpu_per_query: 0.0,
            cpu_label,
            memory_per_client_bytes: 0.0,
            memory_label,
            error_rate,
        };
        metric.validate()?;
        Ok(metric)
    }

    pub fn with_extended_metrics(
        mut self,
        p999_ms: f64,
        cpu_per_query: f64,
        memory_per_client_bytes: f64,
    ) -> Result<Self, BenchmarkValidationError> {
        self.p999_ms = p999_ms;
        self.cpu_per_query = cpu_per_query;
        self.memory_per_client_bytes = memory_per_client_bytes;
        self.validate()?;
        Ok(self)
    }

    #[must_use]
    pub const fn p50_ms(&self) -> f64 {
        self.p50_ms
    }

    #[must_use]
    pub const fn p95_ms(&self) -> f64 {
        self.p95_ms
    }

    #[must_use]
    pub const fn p99_ms(&self) -> f64 {
        self.p99_ms
    }

    #[must_use]
    pub const fn p999_ms(&self) -> f64 {
        self.p999_ms
    }

    #[must_use]
    pub const fn throughput_qps(&self) -> f64 {
        self.throughput_qps
    }

    #[must_use]
    pub const fn cpu_per_query(&self) -> f64 {
        self.cpu_per_query
    }

    #[must_use]
    pub fn cpu_label(&self) -> &str {
        &self.cpu_label
    }

    #[must_use]
    pub const fn memory_per_client_bytes(&self) -> f64 {
        self.memory_per_client_bytes
    }

    #[must_use]
    pub fn memory_label(&self) -> &str {
        &self.memory_label
    }

    #[must_use]
    pub const fn error_rate(&self) -> f64 {
        self.error_rate
    }

    fn validate(&self) -> Result<(), BenchmarkValidationError> {
        validate_metric_value("p50_ms", self.p50_ms)?;
        validate_metric_value("p95_ms", self.p95_ms)?;
        validate_metric_value("p99_ms", self.p99_ms)?;
        validate_metric_value("p999_ms", self.p999_ms)?;
        validate_metric_value("throughput_qps", self.throughput_qps)?;
        validate_metric_value("cpu_per_query", self.cpu_per_query)?;
        validate_metric_value("memory_per_client_bytes", self.memory_per_client_bytes)?;
        if self.p95_ms < self.p50_ms || self.p99_ms < self.p95_ms || self.p999_ms < self.p99_ms {
            return Err(BenchmarkValidationError::InvalidMetric {
                field: "latency_order",
            });
        }
        if self.cpu_label.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyMetricLabel { field: "cpu_label" });
        }
        if self.memory_label.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyMetricLabel {
                field: "memory_label",
            });
        }
        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(BenchmarkValidationError::InvalidErrorRate);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BenchmarkResult {
    scenario: Arc<str>,
    target: BenchmarkTarget,
    driver: BenchmarkDriver,
    duration_ms: u64,
    metrics: BenchmarkMetric,
}

impl BenchmarkResult {
    pub fn new(
        scenario: impl Into<Arc<str>>,
        target: BenchmarkTarget,
        driver: BenchmarkDriver,
        duration_ms: u64,
        metrics: BenchmarkMetric,
    ) -> Result<Self, BenchmarkValidationError> {
        let scenario = scenario.into();
        if scenario.trim().is_empty() {
            return Err(BenchmarkValidationError::EmptyScenarioName);
        }
        if duration_ms == 0 {
            return Err(BenchmarkValidationError::InvalidDuration {
                field: "duration_ms",
            });
        }
        target.validate()?;
        metrics.validate()?;

        Ok(Self {
            scenario,
            target,
            driver,
            duration_ms,
            metrics,
        })
    }

    #[must_use]
    pub fn scenario(&self) -> &str {
        &self.scenario
    }

    #[must_use]
    pub const fn target(&self) -> &BenchmarkTarget {
        &self.target
    }

    #[must_use]
    pub const fn driver(&self) -> BenchmarkDriver {
        self.driver
    }

    #[must_use]
    pub const fn duration_ms(&self) -> u64 {
        self.duration_ms
    }

    #[must_use]
    pub const fn metrics(&self) -> &BenchmarkMetric {
        &self.metrics
    }
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum BenchmarkValidationError {
    #[error("benchmark scenario document could not be loaded: {message}")]
    InvalidScenarioDocument { message: Arc<str> },
    #[error("benchmark scenario name cannot be empty")]
    EmptyScenarioName,
    #[error("benchmark target label cannot be empty")]
    EmptyTargetLabel,
    #[error("benchmark target dsn cannot be empty")]
    EmptyTargetDsn,
    #[error("benchmark scenario must define at least one target")]
    EmptyTargets,
    #[error("benchmark scenario must define a target matrix")]
    MissingTargetMatrix,
    #[error("benchmark scenario must define required target '{comparison}'")]
    MissingRequiredTarget { comparison: Arc<str> },
    #[error("benchmark connection profile field '{field}' must be greater than zero")]
    InvalidConnectionProfile { field: &'static str },
    #[error("benchmark scenario must declare at least one expected metric")]
    EmptyExpectedMetrics,
    #[error("benchmark target label '{label}' is duplicated")]
    DuplicateTargetLabel { label: Arc<str> },
    #[error("benchmark {field} must be greater than zero")]
    InvalidDuration { field: &'static str },
    #[error("benchmark warmup cannot exceed benchmark duration")]
    WarmupExceedsDuration,
    #[error("benchmark metric '{field}' is invalid")]
    InvalidMetric { field: &'static str },
    #[error("benchmark metric '{field}' cannot be empty")]
    EmptyMetricLabel { field: &'static str },
    #[error("benchmark error rate must be finite and between 0.0 and 1.0")]
    InvalidErrorRate,
    #[error("benchmark comparison label '{label}' is unsupported")]
    UnsupportedComparisonLabel { label: Arc<str> },
    #[error("benchmark driver label '{label}' is unsupported")]
    UnsupportedDriverLabel { label: Arc<str> },
    #[error("benchmark workload label '{label}' is unsupported")]
    UnsupportedWorkloadLabel { label: Arc<str> },
}

fn validate_metric_value(field: &'static str, value: f64) -> Result<(), BenchmarkValidationError> {
    if !value.is_finite() || value < 0.0 {
        return Err(BenchmarkValidationError::InvalidMetric { field });
    }
    Ok(())
}

fn redact_connection_string(value: &str) -> String {
    let mut redacted = value.to_owned();
    if let Some(scheme_end) = value.find("://") {
        let authority_start = scheme_end + 3;
        if let Some(userinfo_end) = value[authority_start..].find('@') {
            let userinfo_end = authority_start + userinfo_end;
            redacted = format!(
                "{}<redacted>@{}",
                &value[..authority_start],
                &value[userinfo_end + 1..]
            );
        }
    }

    redact_query_credentials(&redacted)
}

fn redact_query_credentials(value: &str) -> String {
    let Some(query_start) = value.find('?') else {
        return value.to_owned();
    };
    let (query_and_fragment, fragment) = match value[query_start + 1..].find('#') {
        Some(fragment_start) => {
            let fragment_start = query_start + 1 + fragment_start;
            (
                &value[query_start + 1..fragment_start],
                &value[fragment_start..],
            )
        }
        None => (&value[query_start + 1..], ""),
    };
    let query = query_and_fragment
        .split('&')
        .map(|parameter| match parameter.split_once('=') {
            Some((key, _)) if key.eq_ignore_ascii_case("password") => {
                format!("{key}=<redacted>")
            }
            _ => parameter.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("&");

    format!("{}?{query}{fragment}", &value[..query_start])
}
