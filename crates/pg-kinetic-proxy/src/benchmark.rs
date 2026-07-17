use std::{fs, path::Path, sync::Arc};

use serde::Deserialize;

use pg_kinetic_core::benchmark::{
    BenchmarkComparison, BenchmarkDriver, BenchmarkMetric, BenchmarkResult, BenchmarkScenario,
    BenchmarkTarget, BenchmarkValidationError,
};

#[derive(Debug, Deserialize)]
struct BenchmarkScenarioDocument {
    name: String,
    #[serde(default = "default_benchmark_driver")]
    driver: String,
    #[serde(default = "default_duration_ms")]
    duration_ms: u64,
    #[serde(default = "default_warmup_ms")]
    warmup_ms: u64,
    #[serde(default)]
    targets: Vec<BenchmarkTargetDocument>,
}

#[derive(Debug, Deserialize)]
struct BenchmarkTargetDocument {
    label: String,
    comparison: String,
    dsn: String,
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
    let targets = document
        .targets
        .into_iter()
        .map(|target| {
            let comparison = parse_comparison(&target.comparison)?;
            BenchmarkTarget::new(target.label, comparison, target.dsn)
        })
        .collect::<Result<Vec<_>, _>>()?;

    BenchmarkScenario::new(
        document.name,
        driver,
        document.duration_ms,
        document.warmup_ms,
        targets,
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

fn default_benchmark_driver() -> String {
    String::from("pgbench")
}

fn default_duration_ms() -> u64 {
    60_000
}

fn default_warmup_ms() -> u64 {
    5_000
}
