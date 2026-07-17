use std::{fmt, sync::Arc};

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
