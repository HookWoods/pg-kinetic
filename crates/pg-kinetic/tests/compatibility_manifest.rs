use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use pg_kinetic::core::compatibility::{
    CompatibilityArtifactPolicy, CompatibilityLanguage, CompatibilityLibrary, CompatibilitySuite,
    CompatibilitySuiteSpec, CompatibilityTarget,
};
use serde_json::Value;

fn workspace_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

#[test]
fn compatibility_suites_have_stable_manifest_fields() {
    let manifest = manifest_cases();
    let suites = manifest
        .iter()
        .filter_map(|case| case.get("compatibility"))
        .collect::<Vec<_>>();
    assert!(!suites.is_empty());

    for suite in &suites {
        for field in [
            "suite_id",
            "language",
            "library",
            "library_version",
            "target",
            "command",
            "timeout_seconds",
            "required_services",
            "artifact_policy",
            "artifact_path",
            "smoke",
            "category",
            "required",
        ] {
            assert!(suite.get(field).is_some(), "missing {field}: {suite:?}");
        }
        let id = suite["suite_id"].as_str().expect("suite id is string");
        assert!(id.len() <= 96);
        assert!(id.chars().all(|character| character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || character == '-'));
    }
}

#[test]
fn manifest_targets_direct_postgres_and_pg_kinetic() {
    let manifest = manifest_cases();
    let targets = manifest
        .iter()
        .filter_map(|case| case.get("compatibility"))
        .map(|suite| {
            suite["target"]
                .as_str()
                .expect("target is string")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    assert!(targets.contains("direct-postgres"));
    assert!(targets.contains("pg-kinetic"));
}

#[test]
fn optional_libraries_record_stable_skip_reasons() {
    let suites = manifest_cases();
    let optional = suites
        .iter()
        .filter_map(|case| case.get("compatibility"))
        .filter(|suite| suite.get("required").and_then(Value::as_bool) == Some(false))
        .collect::<Vec<_>>();
    assert!(!optional.is_empty());
    assert!(optional.iter().all(|suite| suite
        .get("skip_reason")
        .and_then(Value::as_str)
        .is_some_and(|reason| !reason.trim().is_empty())));
}

#[test]
fn library_versions_are_recorded() {
    let suites = manifest_cases()
        .into_iter()
        .filter(|case| case.get("compatibility").is_some())
        .collect::<Vec<_>>();
    let versioned = suites
        .iter()
        .filter(|suite| {
            suite
                .get("compatibility")
                .and_then(|compatibility| compatibility.get("library_version"))
                .and_then(Value::as_str)
                .is_some()
        })
        .count();
    assert_eq!(versioned, suites.len());
}

#[test]
fn compatibility_schema_requires_filter_and_report_metadata() {
    let schema = std::fs::read_to_string(workspace_path("regression/manifest.schema.json"))
        .expect("read schema");
    let value = serde_json::from_str::<Value>(&schema).expect("schema parses as JSON");
    let required = value["properties"]["case"]["items"]["properties"]["compatibility"]["required"]
        .as_array()
        .expect("compatibility required fields");
    for field in [
        "library_version",
        "required_services",
        "artifact_policy",
        "artifact_path",
        "smoke",
        "category",
        "required",
    ] {
        assert!(
            required.iter().any(|entry| entry == field),
            "schema must require {field}"
        );
    }
    let properties = value["properties"]["case"]["items"]["properties"]["compatibility"]
        ["properties"]
        .as_object()
        .expect("compatibility properties");
    assert!(properties.contains_key("skip_reason"));
    assert!(
        !required.iter().any(|entry| entry == "skip_reason"),
        "skip reason must remain optional"
    );
}

#[test]
fn manifest_rejects_parent_directory_commands() {
    let library = CompatibilityLibrary::new("tokio-postgres", Some("0.7"), true, None::<&str>)
        .expect("library is valid");
    let error = CompatibilitySuite::new(CompatibilitySuiteSpec {
        id: Arc::from("rust-tokio-postgres-pg-kinetic"),
        language: CompatibilityLanguage::Rust,
        library,
        target: CompatibilityTarget::PgKinetic,
        command: Arc::from("../run.sh"),
        timeout: Duration::from_secs(1),
        required_services: Vec::new(),
        artifact_policy: CompatibilityArtifactPolicy::Summary,
        artifact_path: None,
        smoke: true,
    })
    .expect_err("parent directory path should fail");
    assert!(error.contains("within the project"));
}

#[test]
fn linux_first_and_windows_parity_commands_exist() {
    assert!(workspace_path("scripts/compat/run.sh").is_file());
    assert!(workspace_path("scripts/compat/run.ps1").is_file());
    let linux_script =
        std::fs::read_to_string(workspace_path("scripts/compat/run.sh")).expect("read script");
    assert!(linux_script.contains("set -euo pipefail"));
}

#[test]
fn public_integration_surfaces_exclude_roadmap_labels() {
    let paths = [
        ".github/workflows/compatibility.yml",
        "regression/manifest.toml",
        "regression/manifest.schema.json",
        "scripts/compat/run.sh",
        "scripts/compat/run.ps1",
        "docs/compatibility.md",
        "docs/testing.md",
        "docs/regression.md",
        "docs-site/sidebars.js",
        "README.md",
    ];
    for path in paths {
        let contents = std::fs::read_to_string(workspace_path(path)).expect("read public file");
        let lower = contents.to_ascii_lowercase();
        assert!(
            !contains_roadmap_label(&lower),
            "{path} contains a roadmap label"
        );
    }
}

fn manifest_cases() -> Vec<Value> {
    let contents =
        std::fs::read_to_string(workspace_path("regression/manifest.toml")).expect("read manifest");
    let value = contents
        .parse::<toml::Value>()
        .expect("manifest parses as TOML");
    serde_json::to_value(value["case"].clone())
        .expect("manifest cases convert to JSON")
        .as_array()
        .expect("case array")
        .clone()
}

fn contains_roadmap_label(contents: &str) -> bool {
    contents.match_indices("phase").any(|(index, _)| {
        contents[index + "phase".len()..]
            .trim_start()
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_digit())
    })
}
