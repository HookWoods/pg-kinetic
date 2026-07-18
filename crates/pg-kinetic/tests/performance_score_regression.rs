use std::{fs, path::PathBuf};

use pg_kinetic::core::performance::PerformanceScoreOutcome;
use pg_kinetic_proxy::regression::{redact_sensitive_text, score_benchmark_reports};
use serde_json::Value;

fn workspace_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

#[test]
fn sample_baseline_scores_against_itself() {
    let sample = workspace_path("regression/baselines/performance-score.sample.json");
    let report = score_benchmark_reports(&sample, &sample).expect("score sample baseline");

    assert_eq!(report.outcome(), PerformanceScoreOutcome::Passed);
    assert_eq!(report.entries().len(), 10);
    assert!(report
        .entries()
        .iter()
        .all(|entry| entry.outcome == PerformanceScoreOutcome::Passed));
}

#[test]
fn score_fails_release_gate_for_a_latency_breach() {
    let baseline = workspace_path("regression/baselines/performance-score.sample.json");
    let current = workspace_path("target/performance-score-breach.json");
    let contents = fs::read_to_string(&baseline).expect("read sample baseline");
    fs::write(
        &current,
        contents.replace("\"p95_ms\": 2.0", "\"p95_ms\": 3.0"),
    )
    .expect("write breached score input");

    let report = score_benchmark_reports(&baseline, &current).expect("score breached report");
    assert_eq!(report.outcome(), PerformanceScoreOutcome::Failed);
    assert!(report.release_failed());
    let _ = fs::remove_file(current);
}

#[test]
fn score_fails_when_target_sets_differ() {
    let baseline = workspace_path("regression/baselines/performance-score.sample.json");
    let current = workspace_path("target/performance-score-missing-target.json");
    let contents = fs::read_to_string(&baseline).expect("read sample baseline");
    let mut report = serde_json::from_str::<Value>(&contents).expect("parse sample report");
    let extra_target = report["results"]
        .as_array_mut()
        .expect("results are an array")
        .first()
        .expect("sample has a result")
        .clone();
    let extra_target = {
        let mut value = extra_target;
        value["target"]["comparison"] = Value::String(String::from("direct_postgresql"));
        value
    };
    report["results"]
        .as_array_mut()
        .expect("results are an array")
        .push(extra_target);
    fs::write(
        &current,
        serde_json::to_string_pretty(&report).expect("serialize missing target report"),
    )
    .expect("write missing target score input");

    let report = score_benchmark_reports(&baseline, &current).expect("score missing target report");
    assert_eq!(report.outcome(), PerformanceScoreOutcome::Failed);
    assert!(report.release_failed());
    let rendered = serde_json::from_str::<Value>(&report.render_json()).expect("parse score json");
    assert_eq!(rendered["ok"], false);
    assert!(rendered["error"]
        .as_str()
        .expect("error is present")
        .contains("target sets differ"));
    let _ = fs::remove_file(current);
}

#[test]
fn score_json_marks_missing_baseline_as_not_ok() {
    let baseline = workspace_path("regression/baselines/performance-score.sample.json");
    let current = workspace_path("target/performance-score-missing-metric.json");
    let contents = fs::read_to_string(&baseline).expect("read sample baseline");
    let current_contents = contents.replace("\"p95_ms\": 2.0", "\"p95_ms\": null");
    fs::write(&current, current_contents).expect("write missing metric score input");

    let report = score_benchmark_reports(&baseline, &current).expect("score missing metric report");
    assert_eq!(report.outcome(), PerformanceScoreOutcome::MissingBaseline);
    assert!(report.release_failed());
    let rendered = serde_json::from_str::<Value>(&report.render_json()).expect("parse score json");
    assert_eq!(rendered["ok"], false);
    let _ = fs::remove_file(current);
}

#[test]
fn score_output_redacts_connection_credentials() {
    let redacted = redact_sensitive_text("postgres://user:secret@db.example/test?password=hidden");

    assert!(!redacted.contains("secret"));
    assert!(!redacted.contains("hidden"));
    assert!(redacted.contains("[REDACTED]"));
}
