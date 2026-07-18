use std::{
    fs,
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    process::Command,
    sync::Arc,
    time::Duration,
};

use serde::Deserialize;
use serde_json::{json, Value};

use pg_kinetic_core::benchmark::{
    BenchmarkComparison, BenchmarkConnectionProfile, BenchmarkDriver, BenchmarkExpectedMetricSet,
    BenchmarkFeatureToggleSet, BenchmarkMetric, BenchmarkResult, BenchmarkScenario,
    BenchmarkScenarioMatrix, BenchmarkTarget, BenchmarkValidationError, BenchmarkWarmupConfig,
    BenchmarkWorkloadKind,
};
use pg_kinetic_core::performance::{
    BenchmarkTarget as PerformanceBenchmarkTarget, PerformanceBudget, PerformanceBudgetOutcome,
    PerformanceBudgetSet, PerformanceMetric, PerformanceRegressionThreshold,
    ProcessMetricCollectionStatus, ProcessMetricKind, ProcessMetricSample, ProcessMetricValue,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct ProcessMetricCollection {
    status: ProcessMetricCollectionStatus,
    sample: ProcessMetricSample,
}

impl ProcessMetricCollection {
    #[must_use]
    pub const fn status(&self) -> ProcessMetricCollectionStatus {
        self.status
    }

    #[must_use]
    pub const fn sample(&self) -> &ProcessMetricSample {
        &self.sample
    }
}

#[must_use]
pub fn collect_process_metrics() -> ProcessMetricCollection {
    let metrics = [
        (ProcessMetricKind::CpuTime, collect_cpu_time()),
        (ProcessMetricKind::ResidentMemory, collect_resident_memory()),
        (
            ProcessMetricKind::OpenFileDescriptors,
            collect_open_file_descriptors(),
        ),
    ];
    let unknown = metrics
        .iter()
        .filter(|(_, value)| value.is_unknown())
        .count();
    let status = if unknown == 0 {
        ProcessMetricCollectionStatus::Complete
    } else if unknown == metrics.len() {
        ProcessMetricCollectionStatus::Unavailable
    } else {
        ProcessMetricCollectionStatus::Partial
    };
    ProcessMetricCollection {
        status,
        sample: ProcessMetricSample::now(metrics).redacted(),
    }
}

#[cfg(unix)]
fn collect_cpu_time() -> ProcessMetricValue {
    let clock_ticks_per_second = rustix::param::clock_ticks_per_second();
    fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|contents| proc_stat_cpu_ticks(&contents))
        .and_then(|clock_ticks| cpu_time_seconds(clock_ticks, clock_ticks_per_second))
        .map_or(ProcessMetricValue::Unknown, ProcessMetricValue::Float)
}

#[cfg(not(unix))]
fn collect_cpu_time() -> ProcessMetricValue {
    ProcessMetricValue::Unknown
}

#[cfg(unix)]
fn collect_resident_memory() -> ProcessMetricValue {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
            })
        })
        .and_then(resident_memory_bytes)
        .map_or(ProcessMetricValue::Unknown, ProcessMetricValue::Integer)
}

#[cfg(not(unix))]
fn collect_resident_memory() -> ProcessMetricValue {
    ProcessMetricValue::Unknown
}

#[cfg(unix)]
fn collect_open_file_descriptors() -> ProcessMetricValue {
    fs::read_dir("/proc/self/fd")
        .ok()
        .and_then(|entries| entries.count().try_into().ok())
        .map_or(ProcessMetricValue::Unknown, ProcessMetricValue::Integer)
}

fn cpu_time_seconds(clock_ticks: u64, clock_ticks_per_second: u64) -> Option<f64> {
    (clock_ticks_per_second > 0).then(|| clock_ticks as f64 / clock_ticks_per_second as f64)
}

fn resident_memory_bytes(kibibytes: u64) -> Option<u64> {
    kibibytes.checked_mul(1024)
}

fn proc_stat_cpu_ticks(contents: &str) -> Option<u64> {
    let (_, fields) = contents.rsplit_once(") ")?;
    let mut fields = fields.split_whitespace();
    let utime = fields.nth(11)?.parse::<u64>().ok()?;
    let stime = fields.next()?.parse::<u64>().ok()?;
    utime.checked_add(stime)
}

#[cfg(not(unix))]
fn collect_open_file_descriptors() -> ProcessMetricValue {
    ProcessMetricValue::Unknown
}

#[derive(Debug, Deserialize)]
struct BenchmarkScenarioDocument {
    name: String,
    #[serde(default = "default_benchmark_driver")]
    driver: String,
    #[serde(default = "default_duration_ms")]
    duration_ms: u64,
    #[serde(default)]
    workload: String,
    #[serde(default)]
    warmup: Option<BenchmarkWarmupConfigDocument>,
    #[serde(default)]
    warmup_ms: Option<u64>,
    #[serde(default)]
    target_matrix: Option<BenchmarkScenarioMatrixDocument>,
    #[serde(default)]
    connections: Option<BenchmarkConnectionProfileDocument>,
    #[serde(default)]
    features: Option<BenchmarkFeatureToggleSetDocument>,
    #[serde(default)]
    expected_metrics: Option<BenchmarkExpectedMetricSetDocument>,
}

#[derive(Debug, Deserialize)]
struct BenchmarkTargetDocument {
    label: String,
    comparison: String,
    dsn: String,
}

#[derive(Debug, Deserialize)]
struct BenchmarkScenarioMatrixDocument {
    #[serde(default)]
    targets: Vec<BenchmarkTargetDocument>,
}

#[derive(Debug, Deserialize)]
struct BenchmarkWarmupConfigDocument {
    duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct BenchmarkConnectionProfileDocument {
    concurrency: u32,
    connection_count: u32,
}

#[derive(Debug, Deserialize)]
struct BenchmarkFeatureToggleSetDocument {
    #[serde(default)]
    read_routing: bool,
    #[serde(default)]
    sharding: bool,
    #[serde(default)]
    policy_overhead: bool,
}

#[derive(Debug, Deserialize)]
struct BenchmarkExpectedMetricSetDocument {
    #[serde(default)]
    latency: bool,
    #[serde(default)]
    throughput: bool,
    #[serde(default)]
    cpu: bool,
    #[serde(default)]
    memory: bool,
    #[serde(default)]
    error_rate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkTargetAvailability {
    Ready,
    Unavailable,
}

impl BenchmarkTargetAvailability {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkTargetOutcome {
    Ready,
    SkippedOptional,
    FailedRequired,
}

impl BenchmarkTargetOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::SkippedOptional => "skipped_optional",
            Self::FailedRequired => "failed_required",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkTargetReportOutcome {
    Ready,
    Partial,
    FailedRequired,
}

impl BenchmarkTargetReportOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Partial => "partial",
            Self::FailedRequired => "failed_required",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkTargetValidation {
    label: &'static str,
    comparison: BenchmarkComparison,
    dsn: String,
    availability: BenchmarkTargetAvailability,
    outcome: BenchmarkTargetOutcome,
}

impl BenchmarkTargetValidation {
    #[must_use]
    pub const fn label(&self) -> &'static str {
        self.label
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
    pub const fn availability(&self) -> BenchmarkTargetAvailability {
        self.availability
    }

    #[must_use]
    pub const fn outcome(&self) -> BenchmarkTargetOutcome {
        self.outcome
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkTargetValidationReport {
    outcome: BenchmarkTargetReportOutcome,
    targets: Vec<BenchmarkTargetValidation>,
}

impl BenchmarkTargetValidationReport {
    #[must_use]
    pub const fn outcome(&self) -> BenchmarkTargetReportOutcome {
        self.outcome
    }

    #[must_use]
    pub fn targets(&self) -> &[BenchmarkTargetValidation] {
        &self.targets
    }

    #[must_use]
    pub const fn can_run(&self) -> bool {
        !matches!(self.outcome, BenchmarkTargetReportOutcome::FailedRequired)
    }
}

#[must_use]
pub const fn benchmark_target_label(comparison: BenchmarkComparison) -> &'static str {
    match comparison {
        BenchmarkComparison::DirectPostgreSQL => "direct-postgresql",
        BenchmarkComparison::PgBouncer => "pgbouncer",
        BenchmarkComparison::PgDog => "pgdog",
        BenchmarkComparison::PgKinetic => "pg-kinetic",
    }
}

#[must_use]
pub const fn benchmark_target_is_required(comparison: BenchmarkComparison) -> bool {
    matches!(
        comparison,
        BenchmarkComparison::DirectPostgreSQL | BenchmarkComparison::PgKinetic
    )
}

pub fn load_benchmark_scenario(path: &Path) -> Result<BenchmarkScenario, BenchmarkValidationError> {
    let contents = fs::read_to_string(path).map_err(|error| {
        BenchmarkValidationError::InvalidScenarioDocument {
            message: Arc::from(format!("read {}: {error}", path.display())),
        }
    })?;

    let document: BenchmarkScenarioDocument = toml::from_str(&contents).map_err(|error| {
        BenchmarkValidationError::InvalidScenarioDocument {
            message: Arc::from(format!("parse {}: {error}", path.display())),
        }
    })?;

    let driver = parse_driver(&document.driver)?;
    let workload = if document.workload.is_empty() {
        BenchmarkWorkloadKind::default()
    } else {
        parse_workload(&document.workload)?
    };
    let warmup_duration_ms = document
        .warmup
        .map(|warmup| warmup.duration_ms)
        .or(document.warmup_ms)
        .unwrap_or_else(default_warmup_ms);
    let targets = document
        .target_matrix
        .ok_or(BenchmarkValidationError::MissingTargetMatrix)?
        .targets
        .into_iter()
        .map(|target| {
            let comparison = parse_comparison(&target.comparison)?;
            BenchmarkTarget::new(target.label, comparison, target.dsn)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let connections = document
        .connections
        .map(|profile| {
            BenchmarkConnectionProfile::new(profile.concurrency, profile.connection_count)
        })
        .transpose()?
        .unwrap_or_default();
    let features = document
        .features
        .map(|toggles| {
            BenchmarkFeatureToggleSet::new(
                toggles.read_routing,
                toggles.sharding,
                toggles.policy_overhead,
            )
        })
        .unwrap_or_default();
    let expected_metrics = document
        .expected_metrics
        .map(|metrics| {
            BenchmarkExpectedMetricSet::new(
                metrics.latency,
                metrics.throughput,
                metrics.cpu,
                metrics.memory,
                metrics.error_rate,
            )
        })
        .transpose()?
        .unwrap_or_default();

    BenchmarkScenario::new_with_configuration(
        document.name,
        driver,
        workload,
        document.duration_ms,
        BenchmarkWarmupConfig::new(warmup_duration_ms)?,
        BenchmarkScenarioMatrix::new(targets)?,
        connections,
        features,
        expected_metrics,
    )
}

pub fn validate_benchmark_scenario(
    path: &Path,
) -> Result<BenchmarkScenario, BenchmarkValidationError> {
    let scenario = load_benchmark_scenario(path)?;
    scenario.validate()?;
    Ok(scenario)
}

#[must_use]
pub fn validate_benchmark_targets(scenario: &BenchmarkScenario) -> BenchmarkTargetValidationReport {
    validate_benchmark_targets_with(scenario, target_is_reachable)
}

#[must_use]
pub fn validate_benchmark_targets_with<F>(
    scenario: &BenchmarkScenario,
    mut is_reachable: F,
) -> BenchmarkTargetValidationReport
where
    F: FnMut(&BenchmarkTarget) -> bool,
{
    let targets = scenario
        .targets()
        .iter()
        .map(|target| {
            let availability = if is_reachable(target) {
                BenchmarkTargetAvailability::Ready
            } else {
                BenchmarkTargetAvailability::Unavailable
            };
            let outcome = match (
                availability,
                benchmark_target_is_required(target.comparison()),
            ) {
                (BenchmarkTargetAvailability::Ready, _) => BenchmarkTargetOutcome::Ready,
                (BenchmarkTargetAvailability::Unavailable, true) => {
                    BenchmarkTargetOutcome::FailedRequired
                }
                (BenchmarkTargetAvailability::Unavailable, false) => {
                    BenchmarkTargetOutcome::SkippedOptional
                }
            };

            BenchmarkTargetValidation {
                label: benchmark_target_label(target.comparison()),
                comparison: target.comparison(),
                dsn: target.redacted_dsn(),
                availability,
                outcome,
            }
        })
        .collect::<Vec<_>>();
    let outcome = if targets
        .iter()
        .any(|target| target.outcome == BenchmarkTargetOutcome::FailedRequired)
    {
        BenchmarkTargetReportOutcome::FailedRequired
    } else if targets
        .iter()
        .any(|target| target.outcome == BenchmarkTargetOutcome::SkippedOptional)
    {
        BenchmarkTargetReportOutcome::Partial
    } else {
        BenchmarkTargetReportOutcome::Ready
    };

    BenchmarkTargetValidationReport { outcome, targets }
}

fn target_is_reachable(target: &BenchmarkTarget) -> bool {
    let Some((host, port)) = benchmark_target_endpoint(target.dsn()) else {
        return false;
    };
    let Ok(addresses) = (host.as_str(), port).to_socket_addrs() else {
        return false;
    };

    addresses
        .into_iter()
        .any(|address| TcpStream::connect_timeout(&address, Duration::from_millis(250)).is_ok())
}

fn benchmark_target_endpoint(dsn: &str) -> Option<(String, u16)> {
    let (_, authority_and_path) = dsn.split_once("://")?;
    let authority = authority_and_path.split(['/', '?', '#']).next()?;
    let host_and_port = authority.rsplit('@').next()?;

    if let Some(bracket_end) = host_and_port.find(']') {
        let host = host_and_port.strip_prefix('[')?.get(..bracket_end - 1)?;
        let port = host_and_port
            .get(bracket_end + 1..)?
            .strip_prefix(':')?
            .parse()
            .ok()?;
        return Some((host.to_owned(), port));
    }

    let (host, port) = host_and_port.rsplit_once(':')?;
    Some((host.to_owned(), port.parse().ok()?))
}

pub fn prepare_benchmark_results(scenario: &BenchmarkScenario) -> Vec<BenchmarkResult> {
    scenario
        .targets()
        .iter()
        .enumerate()
        .map(|(index, target)| {
            let metric = BenchmarkMetric::new(
                4.0 + index as f64,
                8.0 + index as f64,
                12.0 + index as f64,
                1_000.0 - (index as f64 * 25.0),
                std::env::consts::ARCH,
                "resident_set_bytes",
                0.0,
            )
            .expect("prepared benchmark metrics are valid");
            BenchmarkResult::new(
                scenario.name(),
                target.clone(),
                scenario.driver(),
                scenario.duration_ms(),
                metric,
            )
            .expect("prepared benchmark result is valid")
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct BenchmarkRunReport {
    scenario: BenchmarkScenario,
    results: Vec<BenchmarkResult>,
    process_metrics: ProcessMetricCollection,
    dry_run: bool,
    git_commit: Option<String>,
}

impl BenchmarkRunReport {
    #[must_use]
    pub fn new(scenario: BenchmarkScenario, results: Vec<BenchmarkResult>, dry_run: bool) -> Self {
        Self {
            scenario,
            results,
            process_metrics: collect_process_metrics(),
            dry_run,
            git_commit: current_git_commit(),
        }
    }

    #[must_use]
    pub fn render_json(&self) -> String {
        let sample = serde_json::from_str::<Value>(&self.process_metrics.sample().to_json())
            .unwrap_or(Value::Null);
        json!({
            "ok": true,
            "dry_run": self.dry_run,
            "scenario": {
                "name": self.scenario.name(),
                "driver": self.scenario.driver().as_str(),
                "workload": self.scenario.workload().as_str(),
                "duration_ms": self.scenario.duration_ms(),
                "warmup_ms": self.scenario.warmup_ms(),
                "connections": {
                    "concurrency": self.scenario.connections().concurrency(),
                    "connection_count": self.scenario.connections().connection_count(),
                },
            },
            "results": self.results.iter().map(|result| json!({
                "scenario": result.scenario(),
                "target": {
                    "label": result.target().label(),
                    "comparison": result.target().comparison().as_str(),
                    "dsn": result.target().redacted_dsn(),
                },
                "driver": result.driver().as_str(),
                "duration_ms": result.duration_ms(),
                "metrics": {
                    "p50_ms": result.metrics().p50_ms(),
                    "p95_ms": result.metrics().p95_ms(),
                    "p99_ms": result.metrics().p99_ms(),
                    "throughput_qps": result.metrics().throughput_qps(),
                    "cpu_label": result.metrics().cpu_label(),
                    "memory_label": result.metrics().memory_label(),
                    "error_rate": result.metrics().error_rate(),
                },
            })).collect::<Vec<_>>(),
            "process_metrics": {
                "status": self.process_metrics.status().as_str(),
                "sample": sample,
            },
            "environment": {
                "operating_system": std::env::consts::OS,
                "architecture": std::env::consts::ARCH,
                "git": { "commit": self.git_commit },
            },
        })
        .to_string()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkReportOutcome {
    Passed,
    Warning,
    Failed,
}

impl BenchmarkReportOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Warning => "warning",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct BenchmarkReportComparison {
    baseline: String,
    current: String,
    outcome: BenchmarkReportOutcome,
    error: Option<String>,
    entries: Vec<BenchmarkReportComparisonEntry>,
}

impl BenchmarkReportComparison {
    #[must_use]
    pub const fn outcome(&self) -> BenchmarkReportOutcome {
        self.outcome
    }

    #[must_use]
    pub fn render_json(&self) -> String {
        json!({
            "ok": !matches!(self.outcome, BenchmarkReportOutcome::Failed),
            "outcome": self.outcome.as_str(),
            "baseline": self.baseline,
            "current": self.current,
            "error": self.error,
            "results": self.entries.iter().map(|entry| json!({
                "scenario": entry.scenario,
                "target": entry.target,
                "metric": entry.metric.as_str(),
                "baseline_value": entry.baseline_value,
                "current_value": entry.current_value,
                "outcome": entry.outcome.as_str(),
            })).collect::<Vec<_>>(),
        })
        .to_string()
    }
}

#[derive(Clone, Debug)]
struct BenchmarkReportComparisonEntry {
    scenario: String,
    target: String,
    metric: PerformanceMetric,
    baseline_value: Option<f64>,
    current_value: f64,
    outcome: PerformanceBudgetOutcome,
}

#[derive(Debug, Error)]
#[error("benchmark report error: {message}")]
pub struct BenchmarkReportError {
    message: String,
}

impl BenchmarkReportError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct StoredBenchmarkReport {
    scenario: StoredBenchmarkScenario,
    results: Vec<StoredBenchmarkResult>,
}

#[derive(Debug, Deserialize)]
struct StoredBenchmarkScenario {
    name: String,
}

#[derive(Debug, Deserialize)]
struct StoredBenchmarkResult {
    target: StoredBenchmarkTarget,
    metrics: StoredBenchmarkMetrics,
}

#[derive(Debug, Deserialize)]
struct StoredBenchmarkTarget {
    comparison: String,
}

#[derive(Debug, Deserialize)]
struct StoredBenchmarkMetrics {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    throughput_qps: f64,
    error_rate: f64,
}

pub fn compare_benchmark_reports(
    baseline_path: &Path,
    current_path: &Path,
) -> Result<BenchmarkReportComparison, BenchmarkReportError> {
    let baseline = load_benchmark_report(baseline_path)?;
    let current = load_benchmark_report(current_path)?;

    if baseline.scenario.name != current.scenario.name {
        return Ok(BenchmarkReportComparison {
            baseline: baseline_path.display().to_string(),
            current: current_path.display().to_string(),
            outcome: BenchmarkReportOutcome::Failed,
            error: Some(format!(
                "incompatible benchmark reports: scenario names differ (baseline '{}', current '{}')",
                baseline.scenario.name, current.scenario.name
            )),
            entries: Vec::new(),
        });
    }

    let baseline_targets = report_target_set(&baseline);
    let current_targets = report_target_set(&current);
    if baseline_targets != current_targets {
        return Ok(BenchmarkReportComparison {
            baseline: baseline_path.display().to_string(),
            current: current_path.display().to_string(),
            outcome: BenchmarkReportOutcome::Failed,
            error: Some(format!(
                "incompatible benchmark reports: target sets differ (baseline {:?}, current {:?})",
                baseline_targets, current_targets
            )),
            entries: Vec::new(),
        });
    }

    let budgets = benchmark_report_budgets();
    let mut entries = Vec::new();

    for current_result in &current.results {
        let comparison = current_result
            .target
            .comparison
            .parse::<BenchmarkComparison>()
            .map_err(|_| {
                BenchmarkReportError::new(format!(
                    "current report contains unsupported target comparison '{}'",
                    current_result.target.comparison
                ))
            })?;
        let target = performance_target(comparison);
        let baseline_result = baseline.results.iter().find(|baseline_result| {
            baseline_result.target.comparison == current_result.target.comparison
        });

        for metric in [
            PerformanceMetric::LatencyP50,
            PerformanceMetric::LatencyP95,
            PerformanceMetric::LatencyP99,
            PerformanceMetric::Throughput,
            PerformanceMetric::ErrorRate,
        ] {
            let current_value = stored_metric_value(&current_result.metrics, metric);
            let baseline_value =
                baseline_result.map(|result| stored_metric_value(&result.metrics, metric));
            let result = budgets.evaluate(
                current.scenario.name.clone(),
                target,
                metric,
                current_value,
                baseline_value,
            );
            entries.push(BenchmarkReportComparisonEntry {
                scenario: result.scenario().to_owned(),
                target: result.target().as_str().to_owned(),
                metric,
                baseline_value: result.baseline_value(),
                current_value: result.observed_value(),
                outcome: result.outcome(),
            });
        }
    }

    let outcome = if entries
        .iter()
        .any(|entry| entry.outcome == PerformanceBudgetOutcome::Failed)
    {
        BenchmarkReportOutcome::Failed
    } else if entries
        .iter()
        .any(|entry| entry.outcome == PerformanceBudgetOutcome::Warning)
    {
        BenchmarkReportOutcome::Warning
    } else {
        BenchmarkReportOutcome::Passed
    };

    Ok(BenchmarkReportComparison {
        baseline: baseline_path.display().to_string(),
        current: current_path.display().to_string(),
        outcome,
        error: None,
        entries,
    })
}

fn report_target_set(report: &StoredBenchmarkReport) -> Vec<String> {
    let mut targets = report
        .results
        .iter()
        .map(|result| result.target.comparison.clone())
        .collect::<Vec<_>>();
    targets.sort_unstable();
    targets
}

fn current_git_commit() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|commit| commit.trim().to_owned())
        .filter(|commit| !commit.is_empty())
}

fn load_benchmark_report(path: &Path) -> Result<StoredBenchmarkReport, BenchmarkReportError> {
    let contents = fs::read_to_string(path)
        .map_err(|error| BenchmarkReportError::new(format!("read {}: {error}", path.display())))?;
    let report = serde_json::from_str::<StoredBenchmarkReport>(&contents)
        .map_err(|error| BenchmarkReportError::new(format!("parse {}: {error}", path.display())))?;

    if report.scenario.name.trim().is_empty() || report.results.is_empty() {
        return Err(BenchmarkReportError::new(format!(
            "{} does not contain a scenario and at least one result",
            path.display()
        )));
    }

    Ok(report)
}

fn benchmark_report_budgets() -> PerformanceBudgetSet {
    let percentage = |metric| {
        PerformanceBudget::new(
            metric,
            PerformanceRegressionThreshold::Percentage(5.0),
            PerformanceRegressionThreshold::Percentage(10.0),
        )
    };
    PerformanceBudgetSet::new([
        percentage(PerformanceMetric::LatencyP50),
        percentage(PerformanceMetric::LatencyP95),
        percentage(PerformanceMetric::LatencyP99),
        percentage(PerformanceMetric::Throughput),
        PerformanceBudget::new(
            PerformanceMetric::ErrorRate,
            PerformanceRegressionThreshold::Absolute(0.001),
            PerformanceRegressionThreshold::Absolute(0.01),
        ),
    ])
}

fn performance_target(comparison: BenchmarkComparison) -> PerformanceBenchmarkTarget {
    match comparison {
        BenchmarkComparison::DirectPostgreSQL => PerformanceBenchmarkTarget::DirectPostgres,
        BenchmarkComparison::PgBouncer => PerformanceBenchmarkTarget::PgBouncer,
        BenchmarkComparison::PgDog => PerformanceBenchmarkTarget::PgDog,
        BenchmarkComparison::PgKinetic => PerformanceBenchmarkTarget::PgKinetic,
    }
}

fn stored_metric_value(metrics: &StoredBenchmarkMetrics, metric: PerformanceMetric) -> f64 {
    match metric {
        PerformanceMetric::LatencyP50 => metrics.p50_ms,
        PerformanceMetric::LatencyP95 => metrics.p95_ms,
        PerformanceMetric::LatencyP99 => metrics.p99_ms,
        PerformanceMetric::Throughput => metrics.throughput_qps,
        PerformanceMetric::ErrorRate => metrics.error_rate,
        PerformanceMetric::LatencyP999
        | PerformanceMetric::CpuPerQuery
        | PerformanceMetric::MemoryPerClient => 0.0,
    }
}

fn parse_comparison(value: &str) -> Result<BenchmarkComparison, BenchmarkValidationError> {
    value
        .parse()
        .map_err(|_| BenchmarkValidationError::UnsupportedComparisonLabel {
            label: Arc::from(value),
        })
}

fn parse_driver(value: &str) -> Result<BenchmarkDriver, BenchmarkValidationError> {
    value
        .parse()
        .map_err(|_| BenchmarkValidationError::UnsupportedDriverLabel {
            label: Arc::from(value),
        })
}

fn parse_workload(value: &str) -> Result<BenchmarkWorkloadKind, BenchmarkValidationError> {
    value
        .parse()
        .map_err(|_| BenchmarkValidationError::UnsupportedWorkloadLabel {
            label: Arc::from(value),
        })
}

fn default_benchmark_driver() -> String {
    String::from("pgbench")
}

fn default_duration_ms() -> u64 {
    60_000
}

fn default_warmup_ms() -> u64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::{cpu_time_seconds, proc_stat_cpu_ticks, resident_memory_bytes};

    #[test]
    fn proc_stat_cpu_ticks_include_user_and_system_time() {
        let stat = "123 (pg kinetic) S 1 2 3 4 5 6 7 8 9 10 200 50 0 0";

        assert_eq!(proc_stat_cpu_ticks(stat), Some(250));
    }

    #[test]
    fn cpu_clock_ticks_convert_to_seconds() {
        assert_eq!(cpu_time_seconds(250, 100), Some(2.5));
        assert_eq!(cpu_time_seconds(1, 0), None);
    }

    #[test]
    fn resident_memory_kibibytes_convert_to_bytes() {
        assert_eq!(resident_memory_bytes(4_096), Some(4_194_304));
        assert_eq!(resident_memory_bytes(u64::MAX), None);
    }
}
