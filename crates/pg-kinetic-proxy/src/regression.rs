use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use pg_kinetic_core::{
    benchmark::BenchmarkComparison,
    performance::{
        BenchmarkTarget as PerformanceBenchmarkTarget, PerformanceBudget, PerformanceBudgetOutcome,
        PerformanceBudgetSet, PerformanceMetric, PerformanceRegressionThreshold,
        PerformanceScoreOutcome,
    },
    regression::{
        RegressionArtifactPolicy, RegressionCase, RegressionCaseSpec, RegressionCategory,
        RegressionManifest, RegressionOutcome, RegressionPlatform,
    },
};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegressionError {
    #[error("regression manifest error: {0}")]
    Manifest(String),
    #[error("regression runner error: {0}")]
    Runner(String),
    #[error("performance score error: {0}")]
    Score(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegressionManifestDocument {
    version: u32,
    #[serde(rename = "case")]
    cases: Vec<RegressionCaseDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegressionCaseDocument {
    id: String,
    category: String,
    platform: String,
    timeout_seconds: u64,
    #[serde(default)]
    services: Vec<String>,
    command: String,
    success_marker: Option<String>,
    #[serde(default = "default_artifact_policy")]
    artifact_policy: String,
    artifact_path: Option<String>,
    #[allow(dead_code)]
    compatibility: Option<toml::Value>,
}

fn default_artifact_policy() -> String {
    String::from("summary")
}

pub fn load_regression_manifest(path: &Path) -> Result<RegressionManifest, RegressionError> {
    let contents = fs::read_to_string(path).map_err(|error| {
        RegressionError::Manifest(format!(
            "read {}: {error}",
            redact_sensitive_text(&path.display().to_string())
        ))
    })?;
    let document = toml::from_str::<RegressionManifestDocument>(&contents).map_err(|error| {
        RegressionError::Manifest(format!(
            "parse {}: {error}",
            redact_sensitive_text(&path.display().to_string())
        ))
    })?;
    if document.version != 1 {
        return Err(RegressionError::Manifest(format!(
            "{} has unsupported manifest version {}",
            path.display(),
            document.version
        )));
    }

    let cases = document
        .cases
        .into_iter()
        .map(|case| {
            let category = case
                .category
                .parse::<RegressionCategory>()
                .map_err(RegressionError::Manifest)?;
            let platform = case
                .platform
                .parse::<RegressionPlatform>()
                .map_err(RegressionError::Manifest)?;
            let artifact_policy = case
                .artifact_policy
                .parse::<RegressionArtifactPolicy>()
                .map_err(RegressionError::Manifest)?;
            RegressionCase::new(RegressionCaseSpec {
                id: Arc::from(case.id),
                category,
                platform,
                timeout: Duration::from_secs(case.timeout_seconds),
                services: case.services.into_iter().map(Arc::from).collect(),
                command: Arc::from(case.command),
                success_marker: case.success_marker.map(Arc::from),
                artifact_policy,
                artifact_path: case.artifact_path.map(Arc::from),
            })
            .map_err(RegressionError::Manifest)
        })
        .collect::<Result<Vec<_>, _>>()?;

    RegressionManifest::new(cases).map_err(RegressionError::Manifest)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RegressionSelection {
    pub category: Option<RegressionCategory>,
    pub platform: Option<RegressionPlatform>,
}

impl RegressionSelection {
    #[must_use]
    pub fn matches(self, case: &RegressionCase) -> bool {
        self.category
            .is_none_or(|category| case.category() == category)
            && self
                .platform
                .is_none_or(|platform| case.platform().matches_filter(platform))
    }
}

#[derive(Clone, Debug)]
pub struct RegressionCaseReport {
    pub id: String,
    pub category: RegressionCategory,
    pub platform: RegressionPlatform,
    pub outcome: RegressionOutcome,
    pub duration_ms: u128,
    pub message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RegressionRunReport {
    cases: Vec<RegressionCaseReport>,
}

impl RegressionRunReport {
    #[must_use]
    pub fn cases(&self) -> &[RegressionCaseReport] {
        &self.cases
    }

    #[must_use]
    pub fn has_failures(&self) -> bool {
        self.cases.iter().any(|case| {
            matches!(
                case.outcome,
                RegressionOutcome::Failed
                    | RegressionOutcome::TimedOut
                    | RegressionOutcome::Blocked
            )
        })
    }

    #[must_use]
    pub fn render_json(&self) -> String {
        json!({
            "ok": !self.has_failures(),
            "results": self.cases.iter().map(|case| json!({
                "id": case.id,
                "category": case.category.as_str(),
                "platform": case.platform.as_str(),
                "outcome": case.outcome.as_str(),
                "duration_ms": case.duration_ms,
                "message": case.message,
            })).collect::<Vec<_>>(),
        })
        .to_string()
    }
}

#[derive(Debug, Default)]
pub struct RegressionRunner;

impl RegressionRunner {
    #[must_use]
    pub fn list(
        &self,
        manifest: &RegressionManifest,
        selection: RegressionSelection,
    ) -> Vec<RegressionCaseReport> {
        manifest
            .cases()
            .iter()
            .filter(|case| selection.matches(case))
            .map(|case| RegressionCaseReport {
                id: case.id().to_owned(),
                category: case.category(),
                platform: case.platform(),
                outcome: if case.platform().supports_current_platform() {
                    RegressionOutcome::Skipped
                } else {
                    RegressionOutcome::Blocked
                },
                duration_ms: 0,
                message: None,
            })
            .collect()
    }

    pub fn run(
        &self,
        manifest: &RegressionManifest,
        selection: RegressionSelection,
    ) -> Result<RegressionRunReport, RegressionError> {
        let available_services = available_services();
        let cases = manifest
            .cases()
            .iter()
            .filter(|case| selection.matches(case))
            .map(|case| self.run_case(case, &available_services))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RegressionRunReport { cases })
    }

    fn run_case(
        &self,
        case: &RegressionCase,
        available_services: &BTreeSet<String>,
    ) -> Result<RegressionCaseReport, RegressionError> {
        let report = |outcome, duration_ms, message| RegressionCaseReport {
            id: case.id().to_owned(),
            category: case.category(),
            platform: case.platform(),
            outcome,
            duration_ms,
            message,
        };

        if !case.platform().supports_current_platform() {
            return Ok(report(
                RegressionOutcome::Skipped,
                0,
                Some(format!("not supported on {}", current_platform())),
            ));
        }

        let missing_services = case
            .services()
            .iter()
            .filter(|service| !available_services.contains(service.as_ref()))
            .map(|service| service.to_string())
            .collect::<Vec<_>>();
        if !missing_services.is_empty() {
            return Ok(report(
                RegressionOutcome::Blocked,
                0,
                Some(format!(
                    "required services unavailable: {}",
                    missing_services.join(", ")
                )),
            ));
        }

        let artifact_path = case
            .artifact_path()
            .map(PathBuf::from)
            .unwrap_or_else(|| temporary_artifact_path(case.id()));
        ensure_ignored_output_path(&artifact_path)?;
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RegressionError::Runner(format!("create {}: {error}", parent.display()))
            })?;
        }
        let artifact = fs::File::create(&artifact_path).map_err(|error| {
            RegressionError::Runner(format!("create {}: {error}", artifact_path.display()))
        })?;
        let stderr = artifact.try_clone().map_err(|error| {
            RegressionError::Runner(format!("clone {}: {error}", artifact_path.display()))
        })?;

        let started = Instant::now();
        let mut command = shell_command(case.command());
        let mut child = command
            .stdout(Stdio::from(artifact))
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| {
                RegressionError::Runner(format!(
                    "start regression case '{}': {}",
                    case.id(),
                    redact_sensitive_text(&error.to_string())
                ))
            })?;

        let status = loop {
            if let Some(status) = child.try_wait().map_err(|error| {
                RegressionError::Runner(format!(
                    "wait for regression case '{}': {error}",
                    case.id()
                ))
            })? {
                break Some(status);
            }
            if started.elapsed() >= case.timeout() {
                terminate_process_tree(&mut child)?;
                child.wait().map_err(|error| {
                    RegressionError::Runner(format!(
                        "wait for timed out regression case '{}': {error}",
                        case.id()
                    ))
                })?;
                break None;
            }
            thread::sleep(Duration::from_millis(25));
        };

        let duration_ms = started.elapsed().as_millis();
        let output = fs::read_to_string(&artifact_path).unwrap_or_default();
        if !matches!(case.artifact_policy(), RegressionArtifactPolicy::Large) {
            let _ = fs::remove_file(&artifact_path);
        }

        if status.is_none() {
            return Ok(report(
                RegressionOutcome::TimedOut,
                duration_ms,
                Some(format!("exceeded {} seconds", case.timeout().as_secs())),
            ));
        }
        let status = status.expect("status is present after timeout handling");
        if !status.success() {
            return Ok(report(
                RegressionOutcome::Failed,
                duration_ms,
                Some(format!("command exited with {status}")),
            ));
        }
        if let Some(marker) = case.success_marker() {
            if !output.contains(marker) {
                return Ok(report(
                    RegressionOutcome::Failed,
                    duration_ms,
                    Some(String::from("success marker was not observed")),
                ));
            }
        }

        Ok(report(RegressionOutcome::Passed, duration_ms, None))
    }
}

fn terminate_process_tree(child: &mut Child) -> Result<(), RegressionError> {
    terminate_descendants(child.id());
    child.kill().or_else(|error| {
        if error.kind() == std::io::ErrorKind::InvalidInput {
            Ok(())
        } else {
            Err(RegressionError::Runner(format!(
                "stop timed out regression process {}: {error}",
                child.id()
            )))
        }
    })
}

#[cfg(windows)]
fn terminate_descendants(pid: u32) {
    let pid = pid.to_string();
    let _ = Command::new("taskkill")
        .args(["/PID", pid.as_str(), "/T", "/F"])
        .status();
}

#[cfg(not(windows))]
fn terminate_descendants(pid: u32) {
    let pid = pid.to_string();
    let _ = Command::new("pkill")
        .args(["-TERM", "-P", pid.as_str()])
        .status();
}

pub fn write_ignored_output(path: &Path, contents: &str) -> Result<(), RegressionError> {
    ensure_ignored_output_path(path)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            RegressionError::Runner(format!("create {}: {error}", parent.display()))
        })?;
    }
    fs::write(path, contents)
        .map_err(|error| RegressionError::Runner(format!("write {}: {error}", path.display())))
}

fn available_services() -> BTreeSet<String> {
    env::var("PG_KINETIC_REGRESSION_SERVICES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|service| !service.is_empty())
        .map(str::to_owned)
        .collect()
}

fn temporary_artifact_path(case_id: &str) -> PathBuf {
    PathBuf::from("target")
        .join("regression")
        .join("runtime")
        .join(format!("{}.log", safe_file_name(case_id)))
}

fn ensure_ignored_output_path(path: &Path) -> Result<(), RegressionError> {
    if path.is_absolute() {
        return Err(RegressionError::Runner(format!(
            "output path {} must be relative and ignored",
            path.display()
        )));
    }
    let status = Command::new("git")
        .args([
            "check-ignore",
            "--quiet",
            "--",
            path.to_string_lossy().as_ref(),
        ])
        .status()
        .map_err(|error| {
            RegressionError::Runner(format!("verify ignored output {}: {error}", path.display()))
        })?;
    if !status.success() {
        return Err(RegressionError::Runner(format!(
            "output path {} is not ignored",
            path.display()
        )));
    }
    Ok(())
}

fn shell_command(command: &str) -> Command {
    #[cfg(windows)]
    {
        let mut process = Command::new("cmd");
        process.args(["/C", command]);
        process
    }
    #[cfg(not(windows))]
    {
        let mut process = Command::new("sh");
        process.args(["-c", command]);
        process
    }
}

fn current_platform() -> &'static str {
    if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        "unsupported"
    }
}

fn safe_file_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct PerformanceScoreEntry {
    pub scenario: String,
    pub target: String,
    pub metric: PerformanceMetric,
    pub baseline_value: Option<f64>,
    pub current_value: Option<f64>,
    pub outcome: PerformanceScoreOutcome,
}

#[derive(Clone, Debug)]
pub struct PerformanceScoreReport {
    baseline: String,
    current: String,
    outcome: PerformanceScoreOutcome,
    error: Option<String>,
    entries: Vec<PerformanceScoreEntry>,
}

impl PerformanceScoreReport {
    #[must_use]
    pub const fn outcome(&self) -> PerformanceScoreOutcome {
        self.outcome
    }

    #[must_use]
    pub fn entries(&self) -> &[PerformanceScoreEntry] {
        &self.entries
    }

    #[must_use]
    pub fn release_failed(&self) -> bool {
        matches!(
            self.outcome,
            PerformanceScoreOutcome::Failed | PerformanceScoreOutcome::MissingBaseline
        )
    }

    #[must_use]
    pub fn render_json(&self) -> String {
        json!({
            "ok": matches!(
                self.outcome,
                PerformanceScoreOutcome::Passed | PerformanceScoreOutcome::Warning
            ),
            "outcome": self.outcome.as_str(),
            "baseline": redact_sensitive_text(&self.baseline),
            "current": redact_sensitive_text(&self.current),
            "error": self.error.as_ref().map(|error| redact_sensitive_text(error)),
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

#[derive(Debug, Deserialize)]
struct StoredScoreReport {
    scenario: StoredScoreScenario,
    results: Vec<StoredScoreResult>,
}

#[derive(Debug, Deserialize)]
struct StoredScoreScenario {
    name: String,
}

#[derive(Debug, Deserialize)]
struct StoredScoreResult {
    target: StoredScoreTarget,
    metrics: StoredScoreMetrics,
}

#[derive(Debug, Deserialize)]
struct StoredScoreTarget {
    comparison: String,
}

#[derive(Debug, Deserialize)]
struct StoredScoreMetrics {
    p50_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
    p999_ms: Option<f64>,
    throughput_qps: Option<f64>,
    cpu_per_query: Option<f64>,
    memory_per_client_bytes: Option<f64>,
    error_rate: Option<f64>,
    checkout_latency_ms: Option<f64>,
    prepared_cache_hit_rate: Option<f64>,
}

pub fn score_benchmark_reports(
    baseline_path: &Path,
    current_path: &Path,
) -> Result<PerformanceScoreReport, RegressionError> {
    let baseline = load_score_report(baseline_path)?;
    let current = load_score_report(current_path)?;
    if baseline.scenario.name != current.scenario.name {
        return Err(RegressionError::Score(format!(
            "incompatible benchmark reports: scenario names differ (baseline '{}', current '{}')",
            baseline.scenario.name, current.scenario.name
        )));
    }
    let baseline_targets = score_target_set(&baseline)?;
    let current_targets = score_target_set(&current)?;
    if baseline_targets != current_targets {
        let error = format!(
            "incompatible benchmark reports: target sets differ (baseline {:?}, current {:?})",
            baseline_targets, current_targets
        );
        return Ok(PerformanceScoreReport {
            baseline: baseline_path.display().to_string(),
            current: current_path.display().to_string(),
            outcome: PerformanceScoreOutcome::Failed,
            error: Some(error),
            entries: Vec::new(),
        });
    }

    let budgets = score_budgets();
    let mut entries = Vec::new();
    for current_result in &current.results {
        let comparison = current_result
            .target
            .comparison
            .parse::<BenchmarkComparison>()
            .map_err(|_| {
                RegressionError::Score(format!(
                    "current report contains unsupported target comparison '{}'",
                    current_result.target.comparison
                ))
            })?;
        let target = performance_target(comparison);
        let baseline_result = baseline.results.iter().find(|baseline_result| {
            baseline_result.target.comparison == current_result.target.comparison
        });
        for metric in score_metrics() {
            let current_value = score_metric_value(&current_result.metrics, metric);
            let baseline_value =
                baseline_result.and_then(|result| score_metric_value(&result.metrics, metric));
            let outcome = score_entry_outcome(
                &budgets,
                &current.scenario.name,
                target,
                metric,
                baseline_value,
                current_value,
            );
            entries.push(PerformanceScoreEntry {
                scenario: current.scenario.name.clone(),
                target: target.as_str().to_owned(),
                metric,
                baseline_value,
                current_value,
                outcome,
            });
        }
    }
    let outcome = aggregate_score_outcome(&entries);
    Ok(PerformanceScoreReport {
        baseline: baseline_path.display().to_string(),
        current: current_path.display().to_string(),
        outcome,
        error: None,
        entries,
    })
}

fn score_target_set(report: &StoredScoreReport) -> Result<BTreeSet<String>, RegressionError> {
    report
        .results
        .iter()
        .map(|result| {
            result
                .target
                .comparison
                .parse::<BenchmarkComparison>()
                .map(|comparison| comparison.as_str().to_owned())
                .map_err(|_| {
                    RegressionError::Score(format!(
                        "report contains unsupported target comparison '{}'",
                        result.target.comparison
                    ))
                })
        })
        .collect()
}

fn load_score_report(path: &Path) -> Result<StoredScoreReport, RegressionError> {
    let contents = fs::read_to_string(path).map_err(|error| {
        RegressionError::Score(format!(
            "read {}: {error}",
            redact_sensitive_text(&path.display().to_string())
        ))
    })?;
    let report = serde_json::from_str::<StoredScoreReport>(&contents).map_err(|error| {
        RegressionError::Score(format!(
            "parse {}: {error}",
            redact_sensitive_text(&path.display().to_string())
        ))
    })?;
    if report.scenario.name.trim().is_empty() || report.results.is_empty() {
        return Err(RegressionError::Score(format!(
            "{} does not contain a scenario and at least one result",
            path.display()
        )));
    }
    Ok(report)
}

fn score_metrics() -> [PerformanceMetric; 10] {
    [
        PerformanceMetric::LatencyP50,
        PerformanceMetric::LatencyP95,
        PerformanceMetric::LatencyP99,
        PerformanceMetric::LatencyP999,
        PerformanceMetric::Throughput,
        PerformanceMetric::CpuPerQuery,
        PerformanceMetric::MemoryPerClient,
        PerformanceMetric::ErrorRate,
        PerformanceMetric::CheckoutLatency,
        PerformanceMetric::PreparedCacheHitRate,
    ]
}

fn score_budgets() -> PerformanceBudgetSet {
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
        percentage(PerformanceMetric::LatencyP999),
        percentage(PerformanceMetric::Throughput),
        percentage(PerformanceMetric::CpuPerQuery),
        percentage(PerformanceMetric::MemoryPerClient),
        PerformanceBudget::new(
            PerformanceMetric::ErrorRate,
            PerformanceRegressionThreshold::Absolute(0.001),
            PerformanceRegressionThreshold::Absolute(0.01),
        ),
        percentage(PerformanceMetric::CheckoutLatency),
        percentage(PerformanceMetric::PreparedCacheHitRate),
    ])
}

fn score_entry_outcome(
    budgets: &PerformanceBudgetSet,
    scenario: &str,
    target: PerformanceBenchmarkTarget,
    metric: PerformanceMetric,
    baseline_value: Option<f64>,
    current_value: Option<f64>,
) -> PerformanceScoreOutcome {
    let (Some(baseline_value), Some(current_value)) = (baseline_value, current_value) else {
        return PerformanceScoreOutcome::MissingBaseline;
    };
    if !baseline_value.is_finite()
        || baseline_value < 0.0
        || !current_value.is_finite()
        || current_value < 0.0
    {
        return PerformanceScoreOutcome::MissingBaseline;
    }
    match budgets
        .evaluate(
            scenario,
            target,
            metric,
            current_value,
            Some(baseline_value),
        )
        .outcome()
    {
        PerformanceBudgetOutcome::Passed => PerformanceScoreOutcome::Passed,
        PerformanceBudgetOutcome::Warning => PerformanceScoreOutcome::Warning,
        PerformanceBudgetOutcome::Failed => PerformanceScoreOutcome::Failed,
    }
}

fn aggregate_score_outcome(entries: &[PerformanceScoreEntry]) -> PerformanceScoreOutcome {
    if entries
        .iter()
        .any(|entry| entry.outcome == PerformanceScoreOutcome::Failed)
    {
        PerformanceScoreOutcome::Failed
    } else if entries
        .iter()
        .any(|entry| entry.outcome == PerformanceScoreOutcome::MissingBaseline)
    {
        PerformanceScoreOutcome::MissingBaseline
    } else if entries
        .iter()
        .any(|entry| entry.outcome == PerformanceScoreOutcome::Warning)
    {
        PerformanceScoreOutcome::Warning
    } else {
        PerformanceScoreOutcome::Passed
    }
}

fn score_metric_value(metrics: &StoredScoreMetrics, metric: PerformanceMetric) -> Option<f64> {
    match metric {
        PerformanceMetric::LatencyP50 => metrics.p50_ms,
        PerformanceMetric::LatencyP95 => metrics.p95_ms,
        PerformanceMetric::LatencyP99 => metrics.p99_ms,
        PerformanceMetric::LatencyP999 => metrics.p999_ms,
        PerformanceMetric::Throughput => metrics.throughput_qps,
        PerformanceMetric::CpuPerQuery => metrics.cpu_per_query,
        PerformanceMetric::MemoryPerClient => metrics.memory_per_client_bytes,
        PerformanceMetric::ErrorRate => metrics.error_rate,
        PerformanceMetric::CheckoutLatency => metrics.checkout_latency_ms,
        PerformanceMetric::PreparedCacheHitRate => metrics.prepared_cache_hit_rate,
    }
}

fn performance_target(comparison: BenchmarkComparison) -> PerformanceBenchmarkTarget {
    match comparison {
        BenchmarkComparison::DirectPostgreSQL => PerformanceBenchmarkTarget::DirectPostgres,
        BenchmarkComparison::PgBouncer => PerformanceBenchmarkTarget::PgBouncer,
        BenchmarkComparison::PgDog => PerformanceBenchmarkTarget::PgDog,
        BenchmarkComparison::PgKinetic => PerformanceBenchmarkTarget::PgKinetic,
    }
}

pub fn redact_sensitive_text(value: &str) -> String {
    let mut redacted = value.to_owned();
    for marker in ["password=", "token=", "secret=", "api_key=", "apikey="] {
        redact_value_after_marker(&mut redacted, marker);
    }
    redact_url_credentials(&mut redacted);
    redacted
}

fn redact_value_after_marker(value: &mut String, marker: &str) {
    let mut start = 0;
    loop {
        let lower = value.to_ascii_lowercase();
        let Some(offset) = lower[start..].find(marker) else {
            break;
        };
        let value_start = start + offset + marker.len();
        let value_end = value[value_start..]
            .find(|character: char| {
                character.is_whitespace() || matches!(character, '&' | ';' | ',' | '\'' | '"')
            })
            .map_or(value.len(), |length| value_start + length);
        value.replace_range(value_start..value_end, "[REDACTED]");
        start = value_start + "[REDACTED]".len();
    }
}

fn redact_url_credentials(value: &mut String) {
    let mut cursor = 0;
    while let Some(scheme_offset) = value[cursor..].find("://") {
        let authority_start = cursor + scheme_offset + 3;
        let authority_end = value[authority_start..]
            .find(['/', '?', '#', ' ', '\n', '\r'])
            .map_or(value.len(), |length| authority_start + length);
        if let Some(at_offset) = value[authority_start..authority_end].rfind('@') {
            let credentials_end = authority_start + at_offset;
            value.replace_range(authority_start..credentials_end, "[REDACTED]");
            cursor = authority_start + "[REDACTED]".len();
        } else {
            cursor = authority_end;
        }
    }
}
