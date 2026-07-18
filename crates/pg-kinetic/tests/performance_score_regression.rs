use std::{fs, path::PathBuf};

use pg_kinetic::core::performance::PerformanceScoreOutcome;
use pg_kinetic_proxy::regression::{redact_sensitive_text, score_benchmark_reports};

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
fn score_output_redacts_connection_credentials() {
    let redacted = redact_sensitive_text("postgres://user:secret@db.example/test?password=hidden");

    assert!(!redacted.contains("secret"));
    assert!(!redacted.contains("hidden"));
    assert!(redacted.contains("[REDACTED]"));
}
