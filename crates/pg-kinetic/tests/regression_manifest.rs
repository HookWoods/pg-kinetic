use std::{fs, path::PathBuf, time::Duration};

use pg_kinetic::core::regression::{
    RegressionArtifactPolicy, RegressionCase, RegressionCategory, RegressionManifest,
    RegressionOutcome, RegressionPlatform,
};
use pg_kinetic_proxy::regression::{
    load_regression_manifest, RegressionRunner, RegressionSelection,
};

fn workspace_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

#[test]
fn checked_in_manifest_covers_the_regression_categories() {
    let manifest = load_regression_manifest(&workspace_path("regression/manifest.toml"))
        .expect("checked-in manifest should load");
    let categories = manifest
        .cases()
        .iter()
        .map(|case| case.category())
        .collect::<Vec<_>>();

    for category in [
        RegressionCategory::Smoke,
        RegressionCategory::Protocol,
        RegressionCategory::Docs,
        RegressionCategory::Benchmark,
        RegressionCategory::Compatibility,
    ] {
        assert!(categories.contains(&category));
    }
    assert!(manifest
        .cases()
        .iter()
        .all(|case| case.timeout() >= Duration::from_secs(1)));
}

#[test]
fn private_artifact_paths_are_rejected() {
    let path = workspace_path("target/regression-invalid-manifest.toml");
    fs::write(
        &path,
        r#"
version = 1
[[case]]
id = "invalid-output"
category = "smoke"
platform = "all"
timeout_seconds = 1
services = []
command = "echo ok"
artifact_policy = "large"
artifact_path = "target/private/output.log"
"#,
    )
    .expect("write invalid manifest");

    let error = load_regression_manifest(&path).expect_err("private artifact path must fail");
    assert!(error.to_string().contains("private artifact location"));
    let _ = fs::remove_file(path);
}

#[test]
fn runner_records_pass_fail_skip_timeout_and_blocked_outcomes() {
    let platform_mismatch = if cfg!(target_os = "windows") {
        RegressionPlatform::Linux
    } else {
        RegressionPlatform::Windows
    };
    let manifest = RegressionManifest::new(vec![
        regression_case(
            "pass",
            RegressionPlatform::All,
            vec![],
            "echo marker",
            Some("marker"),
            2,
        ),
        regression_case(
            "fail",
            RegressionPlatform::All,
            vec![],
            "echo marker",
            Some("missing"),
            2,
        ),
        regression_case(
            "skip",
            platform_mismatch,
            vec![],
            "echo marker",
            Some("marker"),
            2,
        ),
        regression_case(
            "timeout",
            RegressionPlatform::All,
            vec![],
            timeout_command(),
            None,
            1,
        ),
        regression_case(
            "blocked",
            RegressionPlatform::All,
            vec!["missing-service-for-test"],
            "echo marker",
            None,
            2,
        ),
    ])
    .expect("build regression manifest");

    let report = RegressionRunner
        .run(&manifest, RegressionSelection::default())
        .expect("run regression cases");
    let outcomes = report
        .cases()
        .iter()
        .map(|case| case.outcome)
        .collect::<Vec<_>>();

    assert!(outcomes.contains(&RegressionOutcome::Passed));
    assert!(outcomes.contains(&RegressionOutcome::Failed));
    assert!(outcomes.contains(&RegressionOutcome::Skipped));
    assert!(outcomes.contains(&RegressionOutcome::TimedOut));
    assert!(outcomes.contains(&RegressionOutcome::Blocked));
}

fn regression_case(
    id: &str,
    platform: RegressionPlatform,
    services: Vec<&str>,
    command: impl Into<String>,
    success_marker: Option<&str>,
    timeout_seconds: u64,
) -> RegressionCase {
    RegressionCase::new(
        id,
        RegressionCategory::Smoke,
        platform,
        Duration::from_secs(timeout_seconds),
        services,
        command.into(),
        success_marker.map(String::from),
        RegressionArtifactPolicy::None,
        None::<String>,
    )
    .expect("build regression case")
}

fn timeout_command() -> String {
    if cfg!(target_os = "windows") {
        String::from("ping 127.0.0.1 -n 4 > NUL")
    } else {
        String::from("sleep 4")
    }
}
