use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use crate::regression::write_ignored_output;

use pg_kinetic_core::compatibility::{
    validate_public_path, CompatibilityArtifactPolicy, CompatibilityLanguage, CompatibilityLibrary,
    CompatibilityOutcome, CompatibilitySuite, CompatibilitySuiteSpec, CompatibilityTarget,
};
use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompatibilityError {
    #[error("compatibility manifest error: {0}")]
    Manifest(String),
    #[error("compatibility runner error: {0}")]
    Runner(String),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CompatibilitySuiteSelector {
    pub language: Option<CompatibilityLanguage>,
    pub target: Option<CompatibilityTarget>,
    pub smoke: bool,
}

#[derive(Clone, Debug)]
pub struct CompatibilityRunConfig {
    pub manifest_path: PathBuf,
    pub selector: CompatibilitySuiteSelector,
    pub library: Option<String>,
    pub category: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CompatibilityCaseObservation {
    pub case_id: String,
    pub outcome: String,
}

#[derive(Clone, Debug)]
pub struct CompatibilityCaseReport {
    pub suite_id: String,
    pub language: CompatibilityLanguage,
    pub library: String,
    pub version: Option<String>,
    pub target: CompatibilityTarget,
    pub outcome: CompatibilityOutcome,
    pub duration_ms: u128,
    pub skip_reason: Option<String>,
    pub error_summary: Option<String>,
    pub observations: Vec<CompatibilityCaseObservation>,
}

#[derive(Clone, Debug)]
pub struct CompatibilityRunReport {
    cases: Vec<CompatibilityCaseReport>,
}

impl CompatibilityRunReport {
    #[must_use]
    pub fn cases(&self) -> &[CompatibilityCaseReport] {
        &self.cases
    }

    #[must_use]
    pub fn has_failures(&self) -> bool {
        self.cases.iter().any(|case| {
            matches!(
                case.outcome,
                CompatibilityOutcome::Failed | CompatibilityOutcome::Blocked
            )
        })
    }

    #[must_use]
    pub fn render_json(&self) -> String {
        let mut by_category = BTreeMap::<String, usize>::from([
            ("blocked".to_owned(), 0),
            ("fail".to_owned(), 0),
            ("pass".to_owned(), 0),
            ("skip".to_owned(), 0),
        ]);
        for case in &self.cases {
            *by_category
                .entry(case.outcome.as_str().to_owned())
                .or_default() += 1;
        }

        json!({
            "ok": !self.has_failures(),
            "success_marker": "compatibility report complete",
            "summary": by_category,
            "results": self.cases.iter().map(|case| json!({
                "suite_id": case.suite_id,
                "language": case.language.as_str(),
                "library": case.library,
                "version": case.version,
                "target": case.target.as_str(),
                "outcome": case.outcome.as_str(),
                "duration_ms": case.duration_ms,
                "skip_reason": case.skip_reason,
                "error_summary": case.error_summary,
                "cases": case.observations.iter().map(|observation| json!({
                    "case_id": observation.case_id,
                    "outcome": observation.outcome,
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
        .to_string()
    }
}

#[derive(Debug, Default)]
pub struct CompatibilityRunner;

#[derive(Debug)]
struct LoadedCompatibilitySuite {
    suite: CompatibilitySuite,
    category: String,
}

impl CompatibilityRunner {
    pub fn list(
        &self,
        manifest_path: &Path,
        selector: CompatibilitySuiteSelector,
        library: Option<&str>,
        category: Option<&str>,
    ) -> Result<Vec<CompatibilitySuite>, CompatibilityError> {
        let manifest = load_compatibility_manifest(manifest_path)?;
        Ok(manifest
            .into_iter()
            .filter(|loaded| {
                selector_matches(&loaded.suite, &loaded.category, selector, library, category)
            })
            .map(|loaded| loaded.suite)
            .collect())
    }

    pub fn run(
        &self,
        config: &CompatibilityRunConfig,
    ) -> Result<CompatibilityRunReport, CompatibilityError> {
        let suites = self.list(
            &config.manifest_path,
            config.selector,
            config.library.as_deref(),
            config.category.as_deref(),
        )?;
        if suites.is_empty() {
            return Err(CompatibilityError::Runner(
                "no compatibility suites matched the selected filters".to_owned(),
            ));
        }
        let live_enabled = env::var("PG_KINETIC_COMPAT_LIVE").is_ok_and(|value| value == "1");
        let services = available_services();
        let mut cases = Vec::with_capacity(suites.len());

        for suite in suites {
            let case = if !live_enabled {
                skipped_case(
                    &suite,
                    "live-stack-unavailable",
                    "set PG_KINETIC_COMPAT_LIVE=1 after starting direct PostgreSQL and pg-kinetic",
                )
            } else {
                let missing_services = suite
                    .required_services()
                    .iter()
                    .filter(|service| !services.contains(service.as_ref()))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                if !missing_services.is_empty() {
                    skipped_case(
                        &suite,
                        "live-stack-unavailable",
                        &format!(
                            "required services unavailable: {}",
                            missing_services.join(", ")
                        ),
                    )
                } else {
                    run_suite(&suite)?
                }
            };
            record_suite_artifact(&suite, &case)?;
            cases.push(case);
        }
        record_comparison_failures(&mut cases);

        Ok(CompatibilityRunReport { cases })
    }
}

#[derive(Debug)]
pub struct CompatibilityCommandBuilder;

impl CompatibilityCommandBuilder {
    #[must_use]
    pub fn build(suite: &CompatibilitySuite) -> String {
        format!(
            "{} PG_KINETIC_COMPAT_TARGET={}",
            suite.command(),
            suite.target().as_str()
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestDocument {
    version: u32,
    #[serde(rename = "case")]
    cases: Vec<ManifestCaseDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestCaseDocument {
    id: String,
    category: String,
    platform: String,
    timeout_seconds: u64,
    services: Vec<String>,
    command: String,
    success_marker: Option<String>,
    artifact_policy: Option<String>,
    artifact_path: Option<String>,
    #[serde(default)]
    compatibility: Option<CompatibilityCaseDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompatibilityCaseDocument {
    suite_id: String,
    language: String,
    library: String,
    library_version: Option<String>,
    target: String,
    command: String,
    timeout_seconds: u64,
    #[serde(default)]
    required_services: Vec<String>,
    #[serde(default = "default_artifact_policy")]
    artifact_policy: String,
    artifact_path: Option<String>,
    #[serde(default)]
    smoke: bool,
    category: Option<String>,
    #[serde(default)]
    required: bool,
    skip_reason: Option<String>,
}

fn default_artifact_policy() -> String {
    String::from("summary")
}

fn load_compatibility_manifest(
    manifest_path: &Path,
) -> Result<Vec<LoadedCompatibilitySuite>, CompatibilityError> {
    let contents = fs::read_to_string(manifest_path)
        .map_err(|error| CompatibilityError::Manifest(format!("read manifest: {error}")))?;
    let document = toml::from_str::<ManifestDocument>(&contents)
        .map_err(|error| CompatibilityError::Manifest(format!("parse manifest: {error}")))?;
    if document.version != 1 {
        return Err(CompatibilityError::Manifest(format!(
            "unsupported manifest version {}",
            document.version
        )));
    }

    let mut suites = Vec::new();
    for case in document.cases {
        let case_category = case.category.clone();
        let _ = (
            case.id,
            case.category,
            case.platform,
            case.timeout_seconds,
            case.services,
            case.command,
            case.success_marker,
            case.artifact_policy,
            case.artifact_path,
        );
        let Some(compatibility) = case.compatibility else {
            continue;
        };
        validate_public_path(&compatibility.command).map_err(CompatibilityError::Manifest)?;
        let category = compatibility.category.unwrap_or(case_category);
        validate_public_path(&category).map_err(CompatibilityError::Manifest)?;
        let smoke = compatibility.smoke;
        let language = compatibility
            .language
            .parse::<CompatibilityLanguage>()
            .map_err(CompatibilityError::Manifest)?;
        let target = compatibility
            .target
            .parse::<CompatibilityTarget>()
            .map_err(CompatibilityError::Manifest)?;
        let library = CompatibilityLibrary::new(
            compatibility.library,
            compatibility.library_version,
            compatibility.required,
            compatibility.skip_reason,
        )
        .map_err(CompatibilityError::Manifest)?;
        let artifact_policy = compatibility
            .artifact_policy
            .parse::<CompatibilityArtifactPolicy>()
            .map_err(CompatibilityError::Manifest)?;
        let suite = CompatibilitySuite::new(CompatibilitySuiteSpec {
            id: Arc::from(compatibility.suite_id),
            language,
            library,
            target,
            command: Arc::from(compatibility.command),
            timeout: Duration::from_secs(compatibility.timeout_seconds),
            required_services: compatibility
                .required_services
                .into_iter()
                .map(Arc::from)
                .collect(),
            artifact_policy,
            artifact_path: compatibility.artifact_path.map(Arc::from),
            smoke,
        })
        .map_err(CompatibilityError::Manifest)?;
        suites.push(LoadedCompatibilitySuite { suite, category });
    }
    Ok(suites)
}

fn selector_matches(
    suite: &CompatibilitySuite,
    suite_category: &str,
    selector: CompatibilitySuiteSelector,
    library: Option<&str>,
    category: Option<&str>,
) -> bool {
    let language_matches = selector
        .language
        .is_none_or(|language| suite.language() == language);
    let target_matches = selector
        .target
        .is_none_or(|target| suite.target() == target);
    let smoke_matches = !selector.smoke || suite.smoke();
    let library_matches = library.is_none_or(|library| suite.library().name() == library);
    let category_matches = category.is_none_or(|category| suite_category == category);
    language_matches && target_matches && smoke_matches && library_matches && category_matches
}

fn skipped_case(
    suite: &CompatibilitySuite,
    skip_reason: &str,
    error_summary: &str,
) -> CompatibilityCaseReport {
    let outcome = if suite.library().required() {
        CompatibilityOutcome::Failed
    } else {
        CompatibilityOutcome::Skipped
    };
    CompatibilityCaseReport {
        suite_id: suite.id().to_owned(),
        language: suite.language(),
        library: suite.library().name().to_owned(),
        version: suite.library().version().map(str::to_owned),
        target: suite.target(),
        outcome,
        duration_ms: 0,
        skip_reason: Some(skip_reason.to_owned()),
        error_summary: Some(error_summary.to_owned()),
        observations: Vec::new(),
    }
}

fn run_suite(suite: &CompatibilitySuite) -> Result<CompatibilityCaseReport, CompatibilityError> {
    let started = Instant::now();
    let contract_path = workspace_file("compat/common/contract.toml");
    let expected_results_path = workspace_file("compat/common/expected-results.json");
    let mut child = shell_command(suite.command())
        .env("PG_KINETIC_COMPAT_TARGET", suite.target().as_str())
        .env("PG_KINETIC_COMPAT_LIBRARY", suite.library().name())
        .env("PG_KINETIC_COMPAT_CONTRACT", contract_path)
        .env("PG_KINETIC_COMPAT_EXPECTED_RESULTS", expected_results_path)
        .env(
            "PG_KINETIC_COMPAT_TIMEOUT_SECONDS",
            suite.timeout().as_secs().to_string(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            CompatibilityError::Runner(format!("start suite '{}': {error}", suite.id()))
        })?;
    let deadline = started + suite.timeout();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let output = child.wait_with_output().map_err(|error| {
                    CompatibilityError::Runner(format!(
                        "read suite '{}' output: {error}",
                        suite.id()
                    ))
                })?;
                return Ok(completed_case_from_output(
                    suite,
                    started,
                    status.success(),
                    &output,
                ));
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Ok(CompatibilityCaseReport {
                    suite_id: suite.id().to_owned(),
                    language: suite.language(),
                    library: suite.library().name().to_owned(),
                    version: suite.library().version().map(str::to_owned),
                    target: suite.target(),
                    outcome: CompatibilityOutcome::Failed,
                    duration_ms: started.elapsed().as_millis(),
                    skip_reason: None,
                    error_summary: Some(format!(
                        "command timed out after {} seconds",
                        suite.timeout().as_secs()
                    )),
                    observations: Vec::new(),
                });
            }
            Err(error) => {
                return Err(CompatibilityError::Runner(format!(
                    "wait for suite '{}': {error}",
                    suite.id()
                )));
            }
        }
    }
}

fn completed_case_from_output(
    suite: &CompatibilitySuite,
    started: Instant,
    status_success: bool,
    output: &std::process::Output,
) -> CompatibilityCaseReport {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let parsed = parse_suite_output(suite, &stdout);
    let observations = parsed
        .as_ref()
        .map(|summary| summary.observations.clone())
        .unwrap_or_default();
    let expected_cases = expected_case_outcomes();
    let expectation_error = match &expected_cases {
        Ok(expected_cases) => validate_observations(&observations, expected_cases),
        Err(error) => Some(error.to_string()),
    };
    let parsed_outcome = parsed
        .as_ref()
        .map(|summary| summary.outcome)
        .unwrap_or(CompatibilityOutcome::Failed);
    let mut outcome = if status_success || !matches!(parsed_outcome, CompatibilityOutcome::Passed) {
        parsed_outcome
    } else {
        CompatibilityOutcome::Failed
    };
    let skip_reason = parsed
        .as_ref()
        .and_then(|summary| summary.skip_reason.clone());
    if matches!(outcome, CompatibilityOutcome::Skipped) && suite.library().required() {
        outcome = CompatibilityOutcome::Failed;
    }
    if expectation_error.is_some() {
        outcome = CompatibilityOutcome::Failed;
    }
    let error_summary = expectation_error
        .or_else(|| parsed.and_then(|summary| summary.error_summary))
        .or_else(|| (!status_success).then(|| first_non_empty_line(&stderr)))
        .or_else(|| {
            (!status_success || parsed_outcome == CompatibilityOutcome::Failed)
                .then(|| "suite did not emit a recognized compatibility verdict".to_owned())
        })
        .filter(|value| !value.trim().is_empty());

    CompatibilityCaseReport {
        suite_id: suite.id().to_owned(),
        language: suite.language(),
        library: suite.library().name().to_owned(),
        version: suite.library().version().map(str::to_owned),
        target: suite.target(),
        outcome,
        duration_ms: started.elapsed().as_millis(),
        skip_reason,
        error_summary,
        observations,
    }
}

#[derive(Debug)]
struct ParsedSuiteOutput {
    outcome: CompatibilityOutcome,
    skip_reason: Option<String>,
    error_summary: Option<String>,
    observations: Vec<CompatibilityCaseObservation>,
}

fn parse_suite_output(suite: &CompatibilitySuite, stdout: &str) -> Option<ParsedSuiteOutput> {
    let value = stdout
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())?;
    let observations = collect_observations(suite, &value);

    if let Some(results) = value.get("results").and_then(Value::as_array) {
        if let Some(mut parsed) = summarize_results(suite, results) {
            parsed.observations = observations;
            return Some(parsed);
        }
    }

    let outcome = value.get("outcome").and_then(Value::as_str)?;
    Some(ParsedSuiteOutput {
        outcome: parse_outcome(outcome)?,
        skip_reason: value
            .get("skip_reason")
            .and_then(Value::as_str)
            .map(str::to_owned),
        error_summary: value
            .get("error_summary")
            .and_then(Value::as_str)
            .map(str::to_owned),
        observations,
    })
}

fn summarize_results(suite: &CompatibilitySuite, results: &[Value]) -> Option<ParsedSuiteOutput> {
    let mut saw_skip = false;
    let mut saw_pass = false;
    let mut first_skip_reason = None;
    let mut first_error_summary = None;
    for result in results {
        if result
            .get("library")
            .and_then(Value::as_str)
            .is_some_and(|library| library != suite.library().name())
        {
            continue;
        }
        match result.get("outcome").and_then(Value::as_str) {
            Some("blocked") => {
                return Some(ParsedSuiteOutput {
                    outcome: CompatibilityOutcome::Blocked,
                    skip_reason: None,
                    error_summary: result
                        .get("error_summary")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    observations: Vec::new(),
                });
            }
            Some("fail") | Some("failed") => {
                return Some(ParsedSuiteOutput {
                    outcome: CompatibilityOutcome::Failed,
                    skip_reason: None,
                    error_summary: result
                        .get("error_summary")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    observations: Vec::new(),
                });
            }
            Some("skip") | Some("skipped") => {
                saw_skip = true;
                first_skip_reason.get_or_insert_with(|| {
                    result
                        .get("skip_reason")
                        .and_then(Value::as_str)
                        .unwrap_or("feature-unsupported")
                        .to_owned()
                });
                first_error_summary.get_or_insert_with(|| {
                    result
                        .get("error_summary")
                        .and_then(Value::as_str)
                        .unwrap_or("suite reported a skipped case")
                        .to_owned()
                });
            }
            Some("pass") | Some("passed") | Some("connected") | Some("one-row") => {
                saw_pass = true;
            }
            _ => {}
        }
    }
    if saw_skip {
        return Some(ParsedSuiteOutput {
            outcome: CompatibilityOutcome::Skipped,
            skip_reason: first_skip_reason,
            error_summary: first_error_summary,
            observations: Vec::new(),
        });
    }
    if saw_pass {
        return Some(ParsedSuiteOutput {
            outcome: CompatibilityOutcome::Passed,
            skip_reason: None,
            error_summary: None,
            observations: Vec::new(),
        });
    }
    None
}

fn collect_observations(
    suite: &CompatibilitySuite,
    value: &Value,
) -> Vec<CompatibilityCaseObservation> {
    let mut observations = Vec::new();
    if let Some(items) = value.get("cases").and_then(Value::as_array) {
        collect_case_items(suite, items, &mut observations);
    }
    if let Some(results) = value.get("results").and_then(Value::as_array) {
        for result in results {
            if result
                .get("library")
                .and_then(Value::as_str)
                .is_some_and(|library| library != suite.library().name())
            {
                continue;
            }
            collect_case_item(result, &mut observations);
            if let Some(items) = result.get("cases").and_then(Value::as_array) {
                collect_case_items(suite, items, &mut observations);
            }
        }
    }
    observations
}

fn collect_case_items(
    suite: &CompatibilitySuite,
    items: &[Value],
    observations: &mut Vec<CompatibilityCaseObservation>,
) {
    for item in items {
        if item
            .get("library")
            .and_then(Value::as_str)
            .is_some_and(|library| library != suite.library().name())
        {
            continue;
        }
        collect_case_item(item, observations);
    }
}

fn collect_case_item(item: &Value, observations: &mut Vec<CompatibilityCaseObservation>) {
    let Some(case_id) = item.get("case_id").and_then(Value::as_str) else {
        return;
    };
    let Some(outcome) = item.get("outcome").and_then(Value::as_str) else {
        return;
    };
    observations.push(CompatibilityCaseObservation {
        case_id: case_id.to_owned(),
        outcome: outcome.to_owned(),
    });
}

fn record_comparison_failures(cases: &mut Vec<CompatibilityCaseReport>) {
    let mut by_key = BTreeMap::<
        (CompatibilityLanguage, String, Option<String>, String),
        BTreeMap<CompatibilityTarget, String>,
    >::new();
    let mut available_targets = BTreeMap::<
        (CompatibilityLanguage, String, Option<String>),
        BTreeSet<CompatibilityTarget>,
    >::new();
    for case in cases
        .iter()
        .filter(|case| matches!(case.outcome, CompatibilityOutcome::Passed))
    {
        available_targets
            .entry((case.language, case.library.clone(), case.version.clone()))
            .or_default()
            .insert(case.target);
        for observation in &case.observations {
            by_key
                .entry((
                    case.language,
                    case.library.clone(),
                    case.version.clone(),
                    observation.case_id.clone(),
                ))
                .or_default()
                .insert(case.target, observation.outcome.clone());
        }
    }
    let mut comparison_failures = Vec::new();
    for ((language, library, version, case_id), targets) in by_key {
        let has_pair = available_targets
            .get(&(language, library.clone(), version.clone()))
            .is_some_and(|targets| {
                targets.contains(&CompatibilityTarget::DirectPostgres)
                    && targets.contains(&CompatibilityTarget::PgKinetic)
            });
        if has_pair && !targets.contains_key(&CompatibilityTarget::DirectPostgres) {
            comparison_failures.push(comparison_failure(
                language,
                library,
                version,
                case_id,
                "missing direct-postgres observation for paired compatibility case".to_owned(),
            ));
            continue;
        }
        if has_pair && !targets.contains_key(&CompatibilityTarget::PgKinetic) {
            comparison_failures.push(comparison_failure(
                language,
                library,
                version,
                case_id,
                "missing pg-kinetic observation for paired compatibility case".to_owned(),
            ));
            continue;
        }
        let Some(direct) = targets.get(&CompatibilityTarget::DirectPostgres) else {
            continue;
        };
        let Some(proxy) = targets.get(&CompatibilityTarget::PgKinetic) else {
            continue;
        };
        if direct == proxy {
            continue;
        }
        let error_summary = format!(
            "direct-postgres outcome '{direct}' differs from pg-kinetic outcome '{proxy}' for case '{case_id}'"
        );
        comparison_failures.push(comparison_failure(
            language,
            library,
            version,
            case_id,
            error_summary,
        ));
    }
    cases.extend(comparison_failures);
}

fn comparison_failure(
    language: CompatibilityLanguage,
    library: String,
    version: Option<String>,
    case_id: String,
    error_summary: String,
) -> CompatibilityCaseReport {
    CompatibilityCaseReport {
        suite_id: format!("compatibility-comparison-{language}-{library}-{case_id}"),
        language,
        library,
        version,
        target: CompatibilityTarget::PgKinetic,
        outcome: CompatibilityOutcome::Failed,
        duration_ms: 0,
        skip_reason: None,
        error_summary: Some(error_summary),
        observations: vec![CompatibilityCaseObservation {
            case_id,
            outcome: "comparison-failed".to_owned(),
        }],
    }
}

fn parse_outcome(value: &str) -> Option<CompatibilityOutcome> {
    match value {
        "pass" | "passed" => Some(CompatibilityOutcome::Passed),
        "fail" | "failed" => Some(CompatibilityOutcome::Failed),
        "skip" | "skipped" => Some(CompatibilityOutcome::Skipped),
        "blocked" => Some(CompatibilityOutcome::Blocked),
        _ => None,
    }
}

fn record_suite_artifact(
    suite: &CompatibilitySuite,
    case: &CompatibilityCaseReport,
) -> Result<(), CompatibilityError> {
    if !matches!(suite.artifact_policy(), CompatibilityArtifactPolicy::Large) {
        return Ok(());
    }
    let Some(path) = suite.artifact_path().map(PathBuf::from) else {
        return Ok(());
    };
    write_ignored_output(&path, &case_json(case).to_string())
        .map_err(|error| CompatibilityError::Runner(error.to_string()))
}

fn case_json(case: &CompatibilityCaseReport) -> Value {
    json!({
        "suite_id": case.suite_id,
        "language": case.language.as_str(),
        "library": case.library,
        "version": case.version,
        "target": case.target.as_str(),
        "outcome": case.outcome.as_str(),
        "duration_ms": case.duration_ms,
        "skip_reason": case.skip_reason,
        "error_summary": case.error_summary,
        "cases": case.observations.iter().map(|observation| json!({
            "case_id": observation.case_id,
            "outcome": observation.outcome,
        })).collect::<Vec<_>>(),
    })
}

fn expected_case_outcomes() -> Result<BTreeMap<String, String>, CompatibilityError> {
    let path = workspace_file("compat/common/expected-results.json");
    let contents = fs::read_to_string(&path).map_err(|error| {
        CompatibilityError::Runner(format!(
            "read compatibility expected results '{}': {error}",
            path.display()
        ))
    })?;
    let value = serde_json::from_str::<Value>(&contents).map_err(|error| {
        CompatibilityError::Runner(format!(
            "parse compatibility expected results '{}': {error}",
            path.display()
        ))
    })?;
    let cases = value
        .get("cases")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            CompatibilityError::Runner(format!(
                "compatibility expected results '{}' must contain a cases object",
                path.display()
            ))
        })?;
    let mut outcomes = BTreeMap::new();
    for (case_id, case) in cases {
        let outcome = case
            .get("outcome")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| case.get("sqlstate").is_some().then(|| "sqlstate".to_owned()))
            .or_else(|| {
                case.get("rows")
                    .and_then(Value::as_array)
                    .is_some_and(|rows| !rows.is_empty())
                    .then(|| "one-row".to_owned())
            })
            .ok_or_else(|| {
                CompatibilityError::Runner(format!(
                    "compatibility expected result '{case_id}' must declare an outcome, rows, or sqlstate"
                ))
            })?;
        outcomes.insert(case_id.clone(), outcome);
    }
    Ok(outcomes)
}

fn validate_observations(
    observations: &[CompatibilityCaseObservation],
    expected_cases: &BTreeMap<String, String>,
) -> Option<String> {
    for observation in observations {
        if matches!(observation.outcome.as_str(), "skip" | "skipped") {
            continue;
        }
        let Some(expected_outcome) = expected_cases.get(&observation.case_id) else {
            return Some(format!(
                "suite reported unknown compatibility case '{}'",
                observation.case_id
            ));
        };
        if observation.outcome != *expected_outcome {
            return Some(format!(
                "suite reported outcome '{}' for compatibility case '{}', expected '{}'",
                observation.outcome, observation.case_id, expected_outcome
            ));
        }
    }
    None
}

fn workspace_file(relative_path: &str) -> PathBuf {
    let Ok(mut current) = env::current_dir() else {
        return PathBuf::from(relative_path);
    };
    loop {
        let candidate = current.join(relative_path);
        if candidate.exists() {
            return candidate.canonicalize().unwrap_or(candidate);
        }
        if !current.pop() {
            return PathBuf::from(relative_path);
        }
    }
}

fn first_non_empty_line(value: &str) -> String {
    value
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("command exited without a JSON report")
        .to_owned()
}

fn available_services() -> BTreeSet<String> {
    env::var("PG_KINETIC_COMPAT_SERVICES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
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
