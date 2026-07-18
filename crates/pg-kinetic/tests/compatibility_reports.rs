use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use pg_kinetic::core::compatibility::{CompatibilityLanguage, CompatibilityTarget};
use pg_kinetic_proxy::compatibility::{
    CompatibilityRunConfig, CompatibilityRunner, CompatibilitySuiteSelector,
};

fn workspace_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

#[test]
fn every_required_language_maps_core_contract_cases() {
    let manifest = workspace_path("regression/manifest.toml");
    let suites = CompatibilityRunner
        .list(&manifest, CompatibilitySuiteSelector::default(), None, None)
        .expect("list suites");
    let languages = suites
        .iter()
        .map(|suite| suite.language())
        .collect::<BTreeSet<_>>();
    for language in CompatibilityLanguage::ALL {
        assert!(languages.contains(&language), "{language} missing");
    }

    let required = [
        "tokio-postgres",
        "sqlx",
        "pgx",
        "database-sql",
        "jdbc",
        "datasource",
        "hikari",
        "spring-jdbc",
        "pg",
        "kysely",
        "psycopg",
        "asyncpg",
        "sqlalchemy",
        "npgsql",
        "libpq",
    ];
    let libraries = suites
        .iter()
        .map(|suite| suite.library().name())
        .collect::<BTreeSet<_>>();
    for library in required {
        assert!(libraries.contains(library), "{library} missing");
    }
}

#[test]
fn direct_and_proxy_targets_are_comparable_for_each_required_library() {
    let manifest = workspace_path("regression/manifest.toml");
    let suites = CompatibilityRunner
        .list(&manifest, CompatibilitySuiteSelector::default(), None, None)
        .expect("list suites");
    let mut targets_by_language =
        BTreeMap::<CompatibilityLanguage, BTreeSet<CompatibilityTarget>>::new();
    let mut targets_by_required_library =
        BTreeMap::<(CompatibilityLanguage, String), BTreeSet<CompatibilityTarget>>::new();
    for suite in suites {
        targets_by_language
            .entry(suite.language())
            .or_default()
            .insert(suite.target());
        if suite.library().required() {
            targets_by_required_library
                .entry((suite.language(), suite.library().name().to_owned()))
                .or_default()
                .insert(suite.target());
        }
    }
    for language in CompatibilityLanguage::ALL {
        let targets = targets_by_language
            .get(&language)
            .expect("language targets");
        assert!(targets.contains(&CompatibilityTarget::DirectPostgres));
        assert!(targets.contains(&CompatibilityTarget::PgKinetic));
    }
    for ((language, library), targets) in targets_by_required_library {
        assert!(
            targets.contains(&CompatibilityTarget::DirectPostgres),
            "{language}:{library} missing direct-postgres comparison"
        );
        assert!(
            targets.contains(&CompatibilityTarget::PgKinetic),
            "{language}:{library} missing pg-kinetic comparison"
        );
    }
}

#[test]
fn normalized_reports_include_skip_reasons_and_redacted_errors() {
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path: workspace_path("regression/manifest.toml"),
            selector: CompatibilitySuiteSelector::default(),
            library: Some("hikari".to_owned()),
            category: None,
        })
        .expect("run report");
    let value =
        serde_json::from_str::<serde_json::Value>(&report.render_json()).expect("json report");
    let results = value["results"].as_array().expect("results");
    assert!(!results.is_empty());
    assert!(results
        .iter()
        .all(|result| result["skip_reason"] == "live-stack-unavailable"));
    assert!(!report.render_json().contains("password="));
}

#[test]
fn advanced_contract_cases_are_opt_in_and_documented() {
    let contract = std::fs::read_to_string(workspace_path("compat/common/contract.toml"))
        .expect("read contract");
    for case_id in [
        "read-routing-safe-read",
        "primary-write-route",
        "shard-key-lookup",
        "policy-deny",
        "tls-connect",
        "auth-failure",
        "prepared-invalidation",
    ] {
        assert!(contract.contains(case_id), "{case_id} missing");
    }
    assert!(contract.contains("advanced = true"));
}
