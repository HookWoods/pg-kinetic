use std::{path::PathBuf, sync::Arc};

use pg_kinetic::core::compatibility::CompatibilityCase;
use serde_json::Value;

fn workspace_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

#[test]
fn behavior_contract_parses_and_references_fixtures() {
    let contract = contract();
    assert_eq!(contract["version"].as_integer(), Some(1));
    assert_eq!(
        contract["fixtures"]["schema"].as_str(),
        Some("compat/common/schema.sql")
    );
    assert_eq!(
        contract["fixtures"]["seed"].as_str(),
        Some("compat/common/seed.sql")
    );
    assert!(workspace_path("compat/common/schema.sql").is_file());
    assert!(workspace_path("compat/common/seed.sql").is_file());
}

#[test]
fn every_contract_case_has_required_metadata() {
    let contract = contract();
    let cases = contract["case"].as_array().expect("case array");
    assert!(cases.len() >= 13);
    for case in cases {
        for field in [
            "id",
            "description",
            "category",
            "sql_mode",
            "expected_outcome",
            "required_capability",
        ] {
            assert!(case.get(field).and_then(toml::Value::as_str).is_some());
        }
        let built = CompatibilityCase::new(
            Arc::from(case["id"].as_str().expect("id")),
            Arc::from(case["category"].as_str().expect("category")),
            Arc::from(case["description"].as_str().expect("description")),
            Arc::from(case["sql_mode"].as_str().expect("sql mode")),
            Arc::from(case["expected_outcome"].as_str().expect("expected outcome")),
            Arc::from(case["required_capability"].as_str().expect("capability")),
        )
        .expect("case metadata is valid");
        assert_eq!(built.id(), case["id"].as_str().expect("id"));
    }
}

#[test]
fn expected_results_are_json_compatible() {
    let contents = std::fs::read_to_string(workspace_path("compat/common/expected-results.json"))
        .expect("read expected results");
    let value = serde_json::from_str::<Value>(&contents).expect("expected results parse");
    assert!(value["cases"].as_object().expect("cases object").len() >= 13);
}

#[test]
fn contract_contains_no_credentials_or_local_paths() {
    for path in [
        "compat/README.md",
        "compat/common/contract.toml",
        "compat/common/schema.sql",
        "compat/common/seed.sql",
        "compat/common/expected-results.json",
    ] {
        let contents = std::fs::read_to_string(workspace_path(path)).expect("read compat file");
        let lower = contents.to_ascii_lowercase();
        assert!(!lower.contains("password="), "{path} contains credentials");
        assert!(!lower.contains("c:/"), "{path} contains a Windows path");
        assert!(!lower.contains("/users/"), "{path} contains a user path");
        assert!(!lower.contains("/home/"), "{path} contains a home path");
    }
}

fn contract() -> toml::Value {
    let contents = std::fs::read_to_string(workspace_path("compat/common/contract.toml"))
        .expect("read contract");
    contents.parse::<toml::Value>().expect("contract parses")
}
