use std::{fs, path::Path, sync::Arc};

use serde::Deserialize;

use pg_kinetic_core::benchmark::{
    BenchmarkComparison, BenchmarkConnectionProfile, BenchmarkDriver, BenchmarkExpectedMetricSet,
    BenchmarkFeatureToggleSet, BenchmarkMetric, BenchmarkResult, BenchmarkScenario,
    BenchmarkScenarioMatrix, BenchmarkTarget, BenchmarkValidationError, BenchmarkWarmupConfig,
    BenchmarkWorkloadKind,
};

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
    targets: Vec<BenchmarkTargetDocument>,
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
        .map(|matrix| matrix.targets)
        .unwrap_or(document.targets)
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
