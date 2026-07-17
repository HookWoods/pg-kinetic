use std::{
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PerformanceMetric {
    LatencyP50,
    LatencyP95,
    LatencyP99,
    LatencyP999,
    Throughput,
    CpuPerQuery,
    MemoryPerClient,
    ErrorRate,
}

impl PerformanceMetric {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LatencyP50 => "latency_p50",
            Self::LatencyP95 => "latency_p95",
            Self::LatencyP99 => "latency_p99",
            Self::LatencyP999 => "latency_p999",
            Self::Throughput => "throughput",
            Self::CpuPerQuery => "cpu_per_query",
            Self::MemoryPerClient => "memory_per_client",
            Self::ErrorRate => "error_rate",
        }
    }

    #[must_use]
    pub const fn higher_is_better(self) -> bool {
        matches!(self, Self::Throughput)
    }

    #[must_use]
    pub const fn regression_delta(self, observed_value: f64, baseline_value: f64) -> f64 {
        if self.higher_is_better() {
            baseline_value - observed_value
        } else {
            observed_value - baseline_value
        }
    }
}

impl fmt::Display for PerformanceMetric {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum BenchmarkTargetLabel {
    #[default]
    DirectPostgres,
    PgBouncer,
    PgDog,
    PgKinetic,
}

impl BenchmarkTargetLabel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectPostgres => "direct_postgresql",
            Self::PgBouncer => "pgbouncer",
            Self::PgDog => "pgdog",
            Self::PgKinetic => "pg_kinetic",
        }
    }
}

impl fmt::Display for BenchmarkTargetLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub type BenchmarkTarget = BenchmarkTargetLabel;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PerformanceRegressionThreshold {
    Percentage(f64),
    Absolute(f64),
}

impl PerformanceRegressionThreshold {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Percentage(_) => "percentage",
            Self::Absolute(_) => "absolute",
        }
    }

    #[must_use]
    pub const fn value(self) -> f64 {
        match self {
            Self::Percentage(value) | Self::Absolute(value) => value,
        }
    }

    #[must_use]
    pub fn allowed_delta(self, baseline_value: f64) -> Option<f64> {
        if !baseline_value.is_finite()
            || baseline_value < 0.0
            || !self.value().is_finite()
            || self.value() < 0.0
        {
            return None;
        }

        Some(match self {
            Self::Percentage(value) => baseline_value * value / 100.0,
            Self::Absolute(value) => value,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum PerformanceBudgetOutcome {
    #[default]
    Passed,
    Warning,
    Failed,
}

impl PerformanceBudgetOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Warning => "warning",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for PerformanceBudgetOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PerformanceBudget {
    metric: PerformanceMetric,
    warning_threshold: PerformanceRegressionThreshold,
    failure_threshold: PerformanceRegressionThreshold,
}

impl PerformanceBudget {
    #[must_use]
    pub const fn new(
        metric: PerformanceMetric,
        warning_threshold: PerformanceRegressionThreshold,
        failure_threshold: PerformanceRegressionThreshold,
    ) -> Self {
        Self {
            metric,
            warning_threshold,
            failure_threshold,
        }
    }

    #[must_use]
    pub const fn metric(&self) -> PerformanceMetric {
        self.metric
    }

    #[must_use]
    pub const fn warning_threshold(&self) -> PerformanceRegressionThreshold {
        self.warning_threshold
    }

    #[must_use]
    pub const fn failure_threshold(&self) -> PerformanceRegressionThreshold {
        self.failure_threshold
    }

    #[must_use]
    pub fn evaluate(
        &self,
        scenario: impl Into<Arc<str>>,
        target: BenchmarkTargetLabel,
        observed_value: f64,
        baseline_value: Option<f64>,
    ) -> PerformanceRegressionResult {
        let outcome = baseline_value
            .filter(|value| value.is_finite() && *value >= 0.0)
            .filter(|_| observed_value.is_finite() && observed_value >= 0.0)
            .and_then(|baseline| {
                let warning_delta = self.warning_threshold.allowed_delta(baseline)?;
                let failure_delta = self.failure_threshold.allowed_delta(baseline)?;
                let regression_delta = self.metric.regression_delta(observed_value, baseline);

                Some(if regression_delta > failure_delta {
                    PerformanceBudgetOutcome::Failed
                } else if regression_delta > warning_delta {
                    PerformanceBudgetOutcome::Warning
                } else {
                    PerformanceBudgetOutcome::Passed
                })
            })
            .unwrap_or(PerformanceBudgetOutcome::Warning);

        PerformanceRegressionResult::new(
            scenario,
            target,
            self.metric,
            observed_value,
            baseline_value,
            outcome,
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PerformanceBudgetSet {
    pub budgets: Vec<PerformanceBudget>,
}

impl PerformanceBudgetSet {
    #[must_use]
    pub fn new(budgets: impl Into<Vec<PerformanceBudget>>) -> Self {
        Self {
            budgets: budgets.into(),
        }
    }

    #[must_use]
    pub fn budget_for(&self, metric: PerformanceMetric) -> Option<&PerformanceBudget> {
        self.budgets.iter().find(|budget| budget.metric() == metric)
    }

    #[must_use]
    pub fn evaluate(
        &self,
        scenario: impl Into<Arc<str>>,
        target: BenchmarkTargetLabel,
        metric: PerformanceMetric,
        observed_value: f64,
        baseline_value: Option<f64>,
    ) -> PerformanceRegressionResult {
        let scenario = scenario.into();
        if let Some(budget) = self.budget_for(metric) {
            budget.evaluate(scenario, target, observed_value, baseline_value)
        } else {
            PerformanceRegressionResult::new(
                scenario,
                target,
                metric,
                observed_value,
                baseline_value,
                PerformanceBudgetOutcome::Warning,
            )
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PerformanceRegressionResult {
    scenario: Arc<str>,
    target: BenchmarkTargetLabel,
    metric: PerformanceMetric,
    observed_value: f64,
    baseline_value: Option<f64>,
    outcome: PerformanceBudgetOutcome,
}

impl PerformanceRegressionResult {
    #[must_use]
    pub fn new(
        scenario: impl Into<Arc<str>>,
        target: BenchmarkTargetLabel,
        metric: PerformanceMetric,
        observed_value: f64,
        baseline_value: Option<f64>,
        outcome: PerformanceBudgetOutcome,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            target,
            metric,
            observed_value,
            baseline_value,
            outcome,
        }
    }

    #[must_use]
    pub fn scenario(&self) -> &str {
        &self.scenario
    }

    #[must_use]
    pub const fn target(&self) -> BenchmarkTargetLabel {
        self.target
    }

    #[must_use]
    pub const fn metric(&self) -> PerformanceMetric {
        self.metric
    }

    #[must_use]
    pub const fn observed_value(&self) -> f64 {
        self.observed_value
    }

    #[must_use]
    pub const fn baseline_value(&self) -> Option<f64> {
        self.baseline_value
    }

    #[must_use]
    pub const fn outcome(&self) -> PerformanceBudgetOutcome {
        self.outcome
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProfileKind {
    Cpu,
    Memory,
    Allocations,
    Flamegraph,
}

impl ProfileKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::Allocations => "allocations",
            Self::Flamegraph => "flamegraph",
        }
    }
}

impl fmt::Display for ProfileKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ProfileCaptureStatus {
    #[default]
    NotRequested,
    Captured,
    Unavailable,
    Failed,
}

impl ProfileCaptureStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::Captured => "captured",
            Self::Unavailable => "unavailable",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for ProfileCaptureStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProcessMetricKind {
    CpuTime,
    ResidentMemory,
    OpenFileDescriptors,
}

impl ProcessMetricKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CpuTime => "cpu_time",
            Self::ResidentMemory => "resident_memory",
            Self::OpenFileDescriptors => "open_file_descriptors",
        }
    }
}

impl fmt::Display for ProcessMetricKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ProcessMetricValue {
    Unknown,
    Integer(u64),
    Float(f64),
}

impl ProcessMetricValue {
    #[must_use]
    pub const fn unknown() -> Self {
        Self::Unknown
    }

    #[must_use]
    pub const fn as_f64(self) -> Option<f64> {
        match self {
            Self::Integer(value) => Some(value as f64),
            Self::Float(value) if value.is_finite() => Some(value),
            Self::Float(_) | Self::Unknown => None,
        }
    }

    #[must_use]
    pub const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProcessMetricSample {
    sampled_at_ms: u64,
    process_id: Option<u32>,
    command_line: Option<String>,
    metrics: Vec<(ProcessMetricKind, ProcessMetricValue)>,
}

impl ProcessMetricSample {
    #[must_use]
    pub fn new(
        sampled_at_ms: u64,
        metrics: impl Into<Vec<(ProcessMetricKind, ProcessMetricValue)>>,
    ) -> Self {
        Self {
            sampled_at_ms,
            process_id: None,
            command_line: None,
            metrics: metrics.into(),
        }
    }

    #[must_use]
    pub fn now(metrics: impl Into<Vec<(ProcessMetricKind, ProcessMetricValue)>>) -> Self {
        let sampled_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                duration.as_millis().min(u128::from(u64::MAX)) as u64
            });
        Self::new(sampled_at_ms, metrics)
    }

    #[must_use]
    pub const fn sampled_at_ms(&self) -> u64 {
        self.sampled_at_ms
    }

    #[must_use]
    pub fn process_id(&self) -> Option<u32> {
        self.process_id
    }

    #[must_use]
    pub fn command_line(&self) -> Option<&str> {
        self.command_line.as_deref()
    }

    #[must_use]
    pub fn metric(&self, kind: ProcessMetricKind) -> ProcessMetricValue {
        self.metrics
            .iter()
            .find(|(candidate, _)| *candidate == kind)
            .map_or(ProcessMetricValue::Unknown, |(_, value)| *value)
    }

    #[must_use]
    pub fn metrics(&self) -> &[(ProcessMetricKind, ProcessMetricValue)] {
        &self.metrics
    }

    #[must_use]
    pub fn redacted(mut self) -> Self {
        self.process_id = None;
        self.command_line = None;
        self
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        let metrics = self
            .metrics
            .iter()
            .map(|(kind, value)| format!("\"{}\":{}", kind, value_json(*value)))
            .collect::<Vec<_>>()
            .join(",");
        format!("{{\"sampled_at_ms\":{},\"process_id\":null,\"command_line\":null,\"metrics\":{{{metrics}}}}}", self.sampled_at_ms)
    }
}

fn value_json(value: ProcessMetricValue) -> String {
    match value {
        ProcessMetricValue::Unknown => "null".to_owned(),
        ProcessMetricValue::Integer(value) => value.to_string(),
        ProcessMetricValue::Float(value) if value.is_finite() => value.to_string(),
        ProcessMetricValue::Float(_) => "null".to_owned(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DerivedPerformanceMetric {
    metric: PerformanceMetric,
    value: Option<f64>,
}

impl DerivedPerformanceMetric {
    #[must_use]
    pub const fn new(metric: PerformanceMetric, value: Option<f64>) -> Self {
        Self { metric, value }
    }

    #[must_use]
    pub const fn metric(self) -> PerformanceMetric {
        self.metric
    }

    #[must_use]
    pub const fn value(self) -> Option<f64> {
        self.value
    }

    #[must_use]
    pub fn cpu_per_query(
        before: &ProcessMetricSample,
        after: &ProcessMetricSample,
        completed_queries: u64,
    ) -> Self {
        Self::new(
            PerformanceMetric::CpuPerQuery,
            delta(before, after, ProcessMetricKind::CpuTime)
                .and_then(|value| divide(value, completed_queries)),
        )
    }

    #[must_use]
    pub fn memory_per_client(
        before: &ProcessMetricSample,
        after: &ProcessMetricSample,
        clients: u64,
    ) -> Self {
        Self::new(
            PerformanceMetric::MemoryPerClient,
            delta(before, after, ProcessMetricKind::ResidentMemory)
                .and_then(|value| divide(value, clients)),
        )
    }
}

fn delta(
    before: &ProcessMetricSample,
    after: &ProcessMetricSample,
    kind: ProcessMetricKind,
) -> Option<f64> {
    Some(after.metric(kind).as_f64()? - before.metric(kind).as_f64()?)
        .filter(|value| value.is_finite() && *value >= 0.0)
}

fn divide(value: f64, divisor: u64) -> Option<f64> {
    (divisor > 0)
        .then_some(value / divisor as f64)
        .filter(|value| value.is_finite())
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ProcessMetricCollectionStatus {
    #[default]
    Complete,
    Partial,
    Unavailable,
    Failed,
}

impl ProcessMetricCollectionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Unavailable => "unavailable",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for ProcessMetricCollectionStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
