use std::{
    fs,
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    sync::Arc,
    time::Duration,
};

use serde::Deserialize;

use pg_kinetic_core::benchmark::{
    BenchmarkComparison, BenchmarkConnectionProfile, BenchmarkDriver, BenchmarkExpectedMetricSet,
    BenchmarkFeatureToggleSet, BenchmarkMetric, BenchmarkResult, BenchmarkScenario,
    BenchmarkScenarioMatrix, BenchmarkTarget, BenchmarkValidationError, BenchmarkWarmupConfig,
    BenchmarkWorkloadKind,
};
use pg_kinetic_core::performance::{
    ProcessMetricCollectionStatus, ProcessMetricKind, ProcessMetricSample, ProcessMetricValue,
};

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
    fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|contents| {
            contents.rsplit_once(") ").map(|(_, rest)| {
                rest.split_whitespace()
                    .nth(11)
                    .and_then(|value| value.parse().ok())
            })
        })
        .flatten()
        .map_or(ProcessMetricValue::Unknown, ProcessMetricValue::Integer)
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
