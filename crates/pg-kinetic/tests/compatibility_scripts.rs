use std::{
    path::PathBuf,
    sync::{Mutex, OnceLock},
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

fn compat_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn compatibility_runner_lists_and_filters_suites() {
    let manifest = workspace_path("regression/manifest.toml");
    let runner = CompatibilityRunner;
    let all = runner
        .list(&manifest, CompatibilitySuiteSelector::default(), None, None)
        .expect("list all suites");
    assert!(all.len() >= 16);

    let rust = runner
        .list(
            &manifest,
            CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Rust),
                target: None,
                smoke: false,
            },
            None,
            None,
        )
        .expect("list rust suites");
    assert!(rust
        .iter()
        .all(|suite| suite.language() == CompatibilityLanguage::Rust));

    let pg = runner
        .list(
            &manifest,
            CompatibilitySuiteSelector {
                language: None,
                target: Some(CompatibilityTarget::PgKinetic),
                smoke: false,
            },
            Some("pgx"),
            None,
        )
        .expect("list pgx suites");
    assert!(pg.iter().all(|suite| suite.library().name() == "pgx"));
    assert!(pg
        .iter()
        .all(|suite| suite.target() == CompatibilityTarget::PgKinetic));
}

#[test]
fn compatibility_runner_emits_normalized_json_reports() {
    let _guard = compat_env_lock();
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path: workspace_path("regression/manifest.toml"),
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::PgKinetic),
                smoke: true,
            },
            library: None,
            category: None,
        })
        .expect("run structural report");
    let rendered = report.render_json();
    let value = serde_json::from_str::<serde_json::Value>(&rendered).expect("report parses");
    assert_eq!(value["ok"], false);
    assert_eq!(value["success_marker"], "compatibility report complete");
    let results = value["results"].as_array().expect("results array");
    assert!(!results.is_empty());
    assert!(results
        .iter()
        .all(|result| result.get("language").is_some()));
    assert!(results.iter().all(|result| result.get("library").is_some()));
    assert!(results.iter().all(|result| result.get("version").is_some()));
    assert!(results.iter().all(|result| result.get("target").is_some()));
    assert!(results
        .iter()
        .all(|result| result.get("duration_ms").is_some()));
    assert_eq!(value["summary"]["pass"], 0);
    assert_eq!(value["summary"]["fail"], results.len());
    assert_eq!(value["summary"]["skip"], 0);
    assert_eq!(value["summary"]["blocked"], 0);
}

#[test]
fn required_live_preflight_skips_are_failures() {
    let _guard = compat_env_lock();
    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    std::env::remove_var("PG_KINETIC_COMPAT_SERVICES");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path: workspace_path("regression/manifest.toml"),
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Rust),
                target: Some(CompatibilityTarget::PgKinetic),
                smoke: true,
            },
            library: Some("tokio-postgres".to_owned()),
            category: None,
        })
        .expect("run missing-service report");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "fail");
    assert_eq!(case.skip_reason.as_deref(), Some("live-stack-unavailable"));
    assert!(case.error_summary.is_some());
}

#[test]
fn script_wrappers_exist_and_keep_large_outputs_ignored() {
    let linux = std::fs::read_to_string(workspace_path("scripts/compat/run.sh"))
        .expect("read linux wrapper");
    assert!(linux.contains("set -euo pipefail"));
    assert!(linux.contains("cargo run -p pg-kinetic -- compat"));
    assert!(workspace_path("scripts/compat/run.ps1").is_file());

    let manifest =
        std::fs::read_to_string(workspace_path("regression/manifest.toml")).expect("read manifest");
    assert!(manifest.contains("artifact_path = \"target/compat/"));
}

#[test]
fn compatibility_runner_rejects_empty_filtered_selection() {
    let manifest = workspace_path("regression/manifest.toml");
    let error = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path: manifest,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Rust),
                target: Some(CompatibilityTarget::PgKinetic),
                smoke: true,
            },
            library: Some("missing-library".to_owned()),
            category: None,
        })
        .expect_err("empty selection should fail");
    assert!(error
        .to_string()
        .contains("no compatibility suites matched"));
}

#[test]
fn runner_scopes_child_results_and_writes_suite_artifacts() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("emit-skip.ps1")
    } else {
        target_dir.join("emit-skip.sh")
    };
    if cfg!(windows) {
        std::fs::write(
            &command_path,
            "Write-Output '{\"outcome\":\"pass\",\"results\":[{\"library\":\"psycopg\",\"outcome\":\"pass\"},{\"library\":\"asyncpg\",\"outcome\":\"skip\",\"skip_reason\":\"toolchain-unavailable\",\"error_summary\":\"missing tool\"}]}'\r\n",
        )
        .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            "#!/usr/bin/env sh\nprintf '%s\\n' '{\"outcome\":\"pass\",\"results\":[{\"library\":\"psycopg\",\"outcome\":\"pass\"},{\"library\":\"asyncpg\",\"outcome\":\"skip\",\"skip_reason\":\"toolchain-unavailable\",\"error_summary\":\"missing tool\"}]}'\n",
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let artifact_path = "target/compat/test-runner/report.json";
    let manifest_path = target_dir.join("manifest.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-skip"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "toolchain-unavailable"
artifact_policy = "large"
artifact_path = "{artifact_path}"

[case.compatibility]
suite_id = "test-skip-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "large"
artifact_path = "{artifact_path}"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::DirectPostgres),
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "pass");
    assert_eq!(case.skip_reason.as_deref(), None);

    let artifact = std::fs::read_to_string(artifact_path).expect("read artifact");
    let artifact = serde_json::from_str::<serde_json::Value>(&artifact).expect("artifact json");
    assert_eq!(artifact["outcome"], "pass");
}

#[test]
fn runner_treats_nonzero_child_with_pass_json_as_failure() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("emit-pass-fail.ps1")
    } else {
        target_dir.join("emit-pass-fail.sh")
    };
    if cfg!(windows) {
        std::fs::write(
            &command_path,
            "Write-Output '{\"outcome\":\"pass\"}'\r\nexit 7\r\n",
        )
        .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            "#!/usr/bin/env sh\nprintf '%s\\n' '{\"outcome\":\"pass\"}'\nexit 7\n",
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let artifact_path = "target/compat/test-runner/report-nonzero.json";
    let manifest_path = target_dir.join("manifest-nonzero.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-nonzero"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "large"
artifact_path = "{artifact_path}"

[case.compatibility]
suite_id = "test-nonzero-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "large"
artifact_path = "{artifact_path}"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::DirectPostgres),
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "fail");
}

#[test]
fn runner_treats_successful_child_without_json_verdict_as_failure() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("emit-invalid-success.ps1")
    } else {
        target_dir.join("emit-invalid-success.sh")
    };
    if cfg!(windows) {
        std::fs::write(&command_path, "Write-Output 'not-json'\r\nexit 0\r\n")
            .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            "#!/usr/bin/env sh\nprintf '%s\\n' 'not-json'\n",
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let manifest_path = target_dir.join("manifest-invalid-success.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-invalid-success"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "summary"

[case.compatibility]
suite_id = "test-invalid-success-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "summary"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::DirectPostgres),
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "fail");
    assert!(report.has_failures());
    assert!(case
        .error_summary
        .as_deref()
        .is_some_and(|summary| summary.contains("recognized compatibility verdict")));
}

#[test]
fn runner_treats_required_suite_skip_as_failure() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("emit-required-skip.ps1")
    } else {
        target_dir.join("emit-required-skip.sh")
    };
    let json = r#"{"results":[{"library":"psycopg","outcome":"skip","skip_reason":"toolchain-unavailable","error_summary":"missing required toolchain"}]}"#;
    if cfg!(windows) {
        std::fs::write(
            &command_path,
            format!("Write-Output '{json}'\r\nexit 0\r\n"),
        )
        .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            format!("#!/usr/bin/env sh\nprintf '%s\\n' '{json}'\n"),
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let manifest_path = target_dir.join("manifest-required-skip.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-required-skip"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "summary"

[case.compatibility]
suite_id = "test-required-skip-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "summary"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::DirectPostgres),
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "fail");
    assert_eq!(case.skip_reason.as_deref(), Some("toolchain-unavailable"));
    assert!(report.has_failures());
}

#[test]
fn runner_passes_shared_contract_paths_to_child_suites() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("check-contract-env.ps1")
    } else {
        target_dir.join("check-contract-env.sh")
    };
    if cfg!(windows) {
        std::fs::write(
            &command_path,
            r#"if (!(Test-Path $env:PG_KINETIC_COMPAT_CONTRACT) -or !(Test-Path $env:PG_KINETIC_COMPAT_EXPECTED_RESULTS)) { exit 9 }
Write-Output '{"results":[{"library":"psycopg","outcome":"pass"}]}'
exit 0
"#,
        )
        .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            r#"#!/usr/bin/env sh
test -f "$PG_KINETIC_COMPAT_CONTRACT" || exit 9
test -f "$PG_KINETIC_COMPAT_EXPECTED_RESULTS" || exit 9
printf '%s\n' '{"results":[{"library":"psycopg","outcome":"pass"}]}'
"#,
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let manifest_path = target_dir.join("manifest-contract-env.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-contract-env"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "summary"

[case.compatibility]
suite_id = "test-contract-env-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "summary"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::DirectPostgres),
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "pass");
}

#[test]
fn runner_fails_unknown_reported_contract_case() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("emit-unknown-case.ps1")
    } else {
        target_dir.join("emit-unknown-case.sh")
    };
    if cfg!(windows) {
        std::fs::write(
            &command_path,
            "Write-Output '{\"outcome\":\"pass\",\"cases\":[{\"case_id\":\"unknown-case\",\"outcome\":\"connected\"}]}'\r\nexit 0\r\n",
        )
        .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            "#!/usr/bin/env sh\nprintf '%s\\n' '{\"outcome\":\"pass\",\"cases\":[{\"case_id\":\"unknown-case\",\"outcome\":\"connected\"}]}'\n",
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let manifest_path = target_dir.join("manifest-unknown-case.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-unknown-case"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "summary"

[case.compatibility]
suite_id = "test-unknown-case-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "summary"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::DirectPostgres),
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "fail");
    assert!(case
        .error_summary
        .as_deref()
        .is_some_and(|summary| summary.contains("unknown compatibility case")));
}

#[test]
fn runner_fails_reported_contract_outcome_mismatches() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("emit-outcome-mismatch.ps1")
    } else {
        target_dir.join("emit-outcome-mismatch.sh")
    };
    if cfg!(windows) {
        std::fs::write(
            &command_path,
            "Write-Output '{\"outcome\":\"pass\",\"cases\":[{\"case_id\":\"startup-connect\",\"outcome\":\"one-row\"}]}'\r\nexit 0\r\n",
        )
        .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            "#!/usr/bin/env sh\nprintf '%s\\n' '{\"outcome\":\"pass\",\"cases\":[{\"case_id\":\"startup-connect\",\"outcome\":\"one-row\"}]}'\n",
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let manifest_path = target_dir.join("manifest-outcome-mismatch.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-outcome-mismatch"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "summary"

[case.compatibility]
suite_id = "test-outcome-mismatch-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "summary"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: Some(CompatibilityTarget::DirectPostgres),
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    let case = report.cases().first().expect("one case");
    assert_eq!(case.outcome.as_str(), "fail");
    assert!(case
        .error_summary
        .as_deref()
        .is_some_and(|summary| summary.contains("expected 'connected'")));
}

#[test]
fn runner_records_missing_paired_case_observations() {
    let _guard = compat_env_lock();
    let target_dir = workspace_path("target/compat/test-runner");
    std::fs::create_dir_all(&target_dir).expect("create target dir");
    let command_path = if cfg!(windows) {
        target_dir.join("emit-paired-missing-case.ps1")
    } else {
        target_dir.join("emit-paired-missing-case.sh")
    };
    if cfg!(windows) {
        std::fs::write(
            &command_path,
            r#"if ($env:PG_KINETIC_COMPAT_TARGET -eq 'pg-kinetic') {
  Write-Output '{"results":[{"library":"psycopg","outcome":"pass","cases":[]}]}'
} else {
  Write-Output '{"results":[{"library":"psycopg","outcome":"pass","cases":[{"case_id":"startup-connect","outcome":"connected"}]}]}'
}
exit 0
"#,
        )
        .expect("write command");
    } else {
        std::fs::write(
            &command_path,
            r#"#!/usr/bin/env sh
if [ "$PG_KINETIC_COMPAT_TARGET" = "pg-kinetic" ]; then
  printf '%s\n' '{"results":[{"library":"psycopg","outcome":"pass","cases":[]}]}'
else
  printf '%s\n' '{"results":[{"library":"psycopg","outcome":"pass","cases":[{"case_id":"startup-connect","outcome":"connected"}]}]}'
fi
"#,
        )
        .expect("write command");
    }
    let script_path = command_path
        .canonicalize()
        .expect("canonical command")
        .to_string_lossy()
        .replace('\\', "/");
    let command = if cfg!(windows) {
        format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script_path}")
    } else {
        format!("sh {script_path}")
    };
    let manifest_path = target_dir.join("manifest-paired-missing-case.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
version = 1

[[case]]
id = "compat-test-paired-missing-direct"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "summary"

[case.compatibility]
suite_id = "test-paired-missing-direct"
language = "python"
library = "psycopg"
library_version = "3"
target = "direct-postgres"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "summary"
smoke = true
category = "core"
required = true

[[case]]
id = "compat-test-paired-missing-proxy"
category = "compatibility"
platform = "all"
timeout_seconds = 10
services = []
command = "{command}"
success_marker = "compatibility report complete"
artifact_policy = "summary"

[case.compatibility]
suite_id = "test-paired-missing-proxy"
language = "python"
library = "psycopg"
library_version = "3"
target = "pg-kinetic"
command = "{command}"
timeout_seconds = 10
required_services = []
artifact_policy = "summary"
smoke = true
category = "core"
required = true
"#
        ),
    )
    .expect("write manifest");

    std::env::set_var("PG_KINETIC_COMPAT_LIVE", "1");
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path,
            selector: CompatibilitySuiteSelector {
                language: Some(CompatibilityLanguage::Python),
                target: None,
                smoke: true,
            },
            library: Some("psycopg".to_owned()),
            category: Some("core".to_owned()),
        })
        .expect("run temp manifest");
    std::env::remove_var("PG_KINETIC_COMPAT_LIVE");

    assert!(report.has_failures());
    assert!(report.cases().iter().any(|case| {
        case.suite_id.starts_with("compatibility-comparison-")
            && case
                .error_summary
                .as_deref()
                .is_some_and(|summary| summary.contains("missing pg-kinetic observation"))
    }));
}

#[test]
fn public_docs_and_ci_link_the_compatibility_workflow() {
    let compatibility = std::fs::read_to_string(workspace_path("docs/compatibility.md"))
        .expect("read compatibility docs");
    let regression = std::fs::read_to_string(workspace_path("docs/regression.md"))
        .expect("read regression docs");
    let sidebar =
        std::fs::read_to_string(workspace_path("docs-site/sidebars.js")).expect("read sidebar");
    let readme = std::fs::read_to_string(workspace_path("README.md")).expect("read README");
    let workflow = std::fs::read_to_string(workspace_path(".github/workflows/compatibility.yml"))
        .expect("read workflow");

    assert!(sidebar.contains("'compatibility'"));
    assert!(readme.contains("docs/compatibility.md"));
    assert!(compatibility.contains("cargo run -p xtask -- compat --target pg-kinetic --smoke"));
    assert!(regression.contains("case.compatibility"));
    assert!(workflow.contains("runs-on: ubuntu-latest"));
    assert!(workflow.contains("pull_request:"));
    assert!(workflow.contains("workflow_dispatch:"));
    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("PG_KINETIC_COMPAT_LIVE"));
    assert!(workflow.contains("--target direct-postgres --smoke"));
    assert!(workflow.contains("--target pg-kinetic --smoke"));
    assert!(workflow.contains(
        "docker compose -f bench/compose.yml up --detach --wait --build postgres pg-kinetic"
    ));
    assert!(workflow.contains("--category framework"));
    assert!(workflow.contains("npm --prefix docs-site run build"));
}

#[test]
fn production_library_suite_files_are_present() {
    for path in [
        "compat/rust/Cargo.toml",
        "compat/go/go.mod",
        "compat/java/build.gradle.kts",
        "compat/javascript/package.json",
        "compat/python/requirements.txt",
        "compat/dotnet/PgKinetic.Compatibility.csproj",
        "compat/c/Makefile",
        "compat/cpp/CMakeLists.txt",
    ] {
        assert!(workspace_path(path).is_file(), "{path} missing");
    }
}
