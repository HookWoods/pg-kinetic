use std::{collections::HashSet, fmt, str::FromStr, sync::Arc};

use thiserror::Error;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkTarget {
    label: Arc<str>,
    comparison: BenchmarkComparison,
    dsn: Arc<str>,
}

impl BenchmarkTarget {
    #[must_use]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkScenario {
    name: Arc<str>,
    driver: BenchmarkDriver,
    duration_ms: u64,
    warmup_ms: u64,
    targets: Vec<BenchmarkTarget>,
}

impl BenchmarkScenario {
    #[must_use]
    pub fn new(
        name: impl Into<Arc<str>>,
        driver: BenchmarkDriver,
        duration_ms: u64,
        warmup_ms: u64,
        targets: Vec<BenchmarkTarget>,
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
        if warmup_ms > duration_ms {
            return Err(BenchmarkValidationError::WarmupExceedsDuration);
        }
        if targets.is_empty() {
            return Err(BenchmarkValidationError::EmptyTargets);
        }

        let mut labels = HashSet::new();
        for target in &targets {
            target.validate()?;
            if !labels.insert(target.label()) {
                return Err(BenchmarkValidationError::DuplicateTargetLabel {
                    label: Arc::from(target.label()),
                });
            }
        }

        Ok(Self {
            name,
            driver,
            duration_ms,
            warmup_ms,
            targets,
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
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
    pub const fn warmup_ms(&self) -> u64 {
        self.warmup_ms
    }

    #[must_use]
    pub fn targets(&self) -> &[BenchmarkTarget] {
        &self.targets
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
        if self.warmup_ms > self.duration_ms {
            return Err(BenchmarkValidationError::WarmupExceedsDuration);
        }
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

#[derive(Clone, Debug, PartialEq)]
pub struct BenchmarkMetric {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    throughput_qps: f64,
    cpu_label: Arc<str>,
    memory_label: Arc<str>,
    error_rate: f64,
}

impl BenchmarkMetric {
    #[must_use]
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
            throughput_qps,
            cpu_label,
            memory_label,
            error_rate,
        };
        metric.validate()?;
        Ok(metric)
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
    pub const fn throughput_qps(&self) -> f64 {
        self.throughput_qps
    }

    #[must_use]
    pub fn cpu_label(&self) -> &str {
        &self.cpu_label
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
        validate_metric_value("throughput_qps", self.throughput_qps)?;
        if self.p95_ms < self.p50_ms || self.p99_ms < self.p95_ms {
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
    #[must_use]
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
}

fn validate_metric_value(field: &'static str, value: f64) -> Result<(), BenchmarkValidationError> {
    if !value.is_finite() || value < 0.0 {
        return Err(BenchmarkValidationError::InvalidMetric { field });
    }
    Ok(())
}

fn redact_connection_string(value: &str) -> String {
    if let Some(scheme_end) = value.find("://") {
        let authority_start = scheme_end + 3;
        if let Some(userinfo_end) = value[authority_start..].find('@') {
            let userinfo_end = authority_start + userinfo_end;
            let mut redacted = String::with_capacity(value.len());
            redacted.push_str(&value[..authority_start]);
            redacted.push_str("<redacted>@");
            redacted.push_str(&value[userinfo_end + 1..]);
            return redacted;
        }
    }

    value.to_owned()
}
