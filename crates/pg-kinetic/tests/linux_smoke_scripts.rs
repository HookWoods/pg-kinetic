use std::{
    fs,
    path::{Path, PathBuf},
};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate path has workspace root")
        .to_path_buf()
}

fn read_repository_file(path: &str) -> String {
    fs::read_to_string(repository_root().join(path)).unwrap_or_else(|error| {
        panic!("read {path}: {error}");
    })
}

#[test]
fn bash_scripts_use_the_shared_contract() {
    let common = read_repository_file("scripts/lib/common.sh");
    assert!(common.starts_with("#!/usr/bin/env bash\nset -euo pipefail"));
    assert!(common.contains("readonly REPO_ROOT="));
    assert!(common.contains("printf 'PASS: %s"));
    assert!(common.contains("printf 'SKIP: %s"));
    assert!(common.contains("resolve_command"));

    for path in [
        "scripts/smoke/psql.sh",
        "scripts/smoke/compat.sh",
        "scripts/smoke/read-routing.sh",
        "scripts/smoke/sharding.sh",
        "scripts/smoke/runtime.sh",
        "scripts/smoke/mirroring.sh",
        "scripts/smoke/performance.sh",
        "scripts/bench/run-performance.sh",
        "scripts/bench/compare-performance.sh",
        "scripts/bench/profile-performance.sh",
    ] {
        let script = read_repository_file(path);
        assert!(
            script.starts_with("#!/usr/bin/env bash\nset -euo pipefail"),
            "{path} must enable strict mode"
        );
        assert!(
            script.contains("source \"$SCRIPT_DIR/../lib/common.sh\""),
            "{path} must use the shared Bash helpers"
        );
    }

    let admin_wire = read_repository_file("scripts/lib/admin_wire.py");
    assert!(admin_wire.contains("startup_packet"));
    assert!(admin_wire.contains("query_packet"));
}

#[test]
fn every_powershell_smoke_has_a_bash_entrypoint() {
    for script in fs::read_dir(repository_root().join("scripts/smoke"))
        .expect("read smoke scripts")
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.ends_with(".ps1"))
    {
        let bash_script = script.trim_end_matches(".ps1").to_owned() + ".sh";
        assert!(
            repository_root()
                .join("scripts/smoke")
                .join(&bash_script)
                .is_file(),
            "{script} must have Bash parity at {bash_script}"
        );
    }
}

#[test]
fn linux_smoke_scripts_cover_the_powershell_smoke_contract() {
    let psql = read_repository_file("scripts/smoke/psql.sh");
    for setting in [
        "HOST_NAME",
        "PORT",
        "USER_NAME",
        "DATABASE",
        "PASSWORD",
        "PGSSLMODE",
        "PGGSSENCMODE",
        "PGCONNECT_TIMEOUT",
    ] {
        assert!(psql.contains(setting), "psql smoke is missing {setting}");
    }
    assert!(!psql.contains("powershell.exe"));

    let compatibility = read_repository_file("scripts/smoke/compat.sh");
    for command in ["CARGO=", "GO=", "NPM=", "PYTHON="] {
        assert!(
            compatibility.contains(command),
            "compat smoke is missing {command}"
        );
    }
    assert!(compatibility.contains("--package-lock=false"));
}

#[test]
fn benchmark_scripts_preserve_the_product_command_contract() {
    let run = read_repository_file("scripts/bench/run-performance.sh");
    assert!(run.contains("benchmark run"));
    assert!(run.contains("--format json"));
    assert!(run.contains("--dry-run"));

    let compare = read_repository_file("scripts/bench/compare-performance.sh");
    assert!(compare.contains("benchmark compare"));
    assert!(compare.contains("--baseline and --current are required"));

    let profile = read_repository_file("scripts/bench/profile-performance.sh");
    assert!(profile.contains("profile validate"));
    assert!(profile.contains("profile run"));
    assert!(profile.contains("flamegraph"));
    assert!(profile.contains("perf"));
}

#[test]
fn testing_guide_documents_linux_and_windows_workflows() {
    let guide = read_repository_file("docs/testing.md");
    assert!(guide.contains("## Linux"));
    assert!(guide.contains("## Windows"));
    assert!(guide.contains("cargo run -p xtask -- ci-linux"));
    assert!(guide.contains("scripts/smoke/performance.sh"));
}

#[test]
fn xtask_linux_smoke_runs_the_full_smoke_set() {
    let xtask = read_repository_file("crates/xtask/src/main.rs");
    for script in [
        "scripts/smoke/psql.sh",
        "scripts/smoke/read-routing.sh",
        "scripts/smoke/compat.sh",
        "scripts/smoke/runtime.sh",
        "scripts/smoke/mirroring.sh",
        "scripts/smoke/sharding.sh",
        "scripts/smoke/performance.sh",
    ] {
        assert!(xtask.contains(script), "smoke-linux must run {script}");
    }
}
