use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use anyhow::{bail, Context};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileTool {
    Flamegraph,
    Perf,
    Ebpf,
}

impl ProfileTool {
    pub const ALL: [Self; 3] = [Self::Flamegraph, Self::Perf, Self::Ebpf];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Flamegraph => "flamegraph",
            Self::Perf => "perf",
            Self::Ebpf => "ebpf",
        }
    }

    const fn command(self) -> &'static str {
        match self {
            Self::Flamegraph => "cargo-flamegraph",
            Self::Perf => "perf",
            Self::Ebpf => "bpftrace",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProfileRunConfig {
    pub kind: ProfileTool,
    pub scenario: PathBuf,
    pub target: String,
    pub duration_ms: u64,
    pub output_path: PathBuf,
}

impl ProfileRunConfig {
    pub fn new(
        kind: ProfileTool,
        scenario: impl Into<PathBuf>,
        target: impl Into<String>,
        duration_ms: u64,
        output_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            kind,
            scenario: scenario.into(),
            target: target.into(),
            duration_ms,
            output_path: output_path.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProfileToolStatus {
    Ready { command: String },
    Skipped { reason: String },
}

impl ProfileToolStatus {
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileRunOutcome {
    Completed,
    Skipped,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileRunResult {
    pub ok: bool,
    pub outcome: ProfileRunOutcome,
    pub profile_kind: ProfileTool,
    pub scenario: String,
    pub target: String,
    pub duration_ms: u64,
    pub output_path: String,
    pub platform: String,
    pub tool_status: ProfileToolStatus,
}

impl ProfileRunResult {
    pub fn new(config: &ProfileRunConfig, tool_status: ProfileToolStatus) -> Self {
        let outcome = if tool_status.is_ready() {
            ProfileRunOutcome::Completed
        } else {
            ProfileRunOutcome::Skipped
        };

        Self {
            ok: true,
            outcome,
            profile_kind: config.kind,
            scenario: redact_path(&config.scenario),
            target: redact_value(&config.target),
            duration_ms: config.duration_ms,
            output_path: redact_path(&config.output_path),
            platform: std::env::consts::OS.to_owned(),
            tool_status,
        }
    }

    pub fn render_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).context("serialize profile run metadata")
    }
}

type ToolLookup = dyn Fn(&str) -> bool + Send + Sync;

pub struct ProfileRunner {
    tool_lookup: Arc<ToolLookup>,
}

impl std::fmt::Debug for ProfileRunner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProfileRunner")
            .finish_non_exhaustive()
    }
}

impl Default for ProfileRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfileRunner {
    pub fn new() -> Self {
        Self::with_tool_lookup(command_exists)
    }

    pub fn with_tool_lookup(lookup: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self {
            tool_lookup: Arc::new(lookup),
        }
    }

    pub fn validate(&self, tool: ProfileTool) -> ProfileToolStatus {
        if matches!(tool, ProfileTool::Perf) && !cfg!(target_os = "linux") {
            return ProfileToolStatus::Skipped {
                reason: format!(
                    "perf profiling requires Linux; current platform is {}",
                    std::env::consts::OS
                ),
            };
        }
        if matches!(tool, ProfileTool::Ebpf) && !cfg!(target_os = "linux") {
            return ProfileToolStatus::Skipped {
                reason: format!(
                    "eBPF profiling requires Linux; current platform is {}",
                    std::env::consts::OS
                ),
            };
        }

        let command = tool.command();
        if (self.tool_lookup)(command) {
            ProfileToolStatus::Ready {
                command: command.to_owned(),
            }
        } else {
            ProfileToolStatus::Skipped {
                reason: format!("optional profiling tool '{command}' is not available on PATH"),
            }
        }
    }

    pub fn run(&self, config: &ProfileRunConfig) -> anyhow::Result<ProfileRunResult> {
        let tool_status = self.validate(config.kind);
        let result = ProfileRunResult::new(config, tool_status.clone());
        if !tool_status.is_ready() {
            return Ok(result);
        }

        if let Some(parent) = config.output_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create profile output directory {}", parent.display()))?;
        }

        let status = match config.kind {
            ProfileTool::Flamegraph => Command::new("cargo")
                .args([
                    "flamegraph",
                    "--bin",
                    &config.target,
                    "--output",
                    path_arg(&config.output_path),
                    "--",
                    "benchmark",
                    "run",
                    "--scenario",
                    path_arg(&config.scenario),
                    "--dry-run",
                ])
                .status()
                .context("run cargo flamegraph")?,
            ProfileTool::Perf => Command::new("perf")
                .args([
                    "record",
                    "--output",
                    path_arg(&config.output_path),
                    "cargo",
                    "run",
                    "-p",
                    &config.target,
                    "--",
                    "benchmark",
                    "run",
                    "--scenario",
                    path_arg(&config.scenario),
                    "--dry-run",
                ])
                .status()
                .context("run perf record")?,
            ProfileTool::Ebpf => Command::new("bpftrace")
                .args([
                    "-q",
                    "-o",
                    path_arg(&config.output_path),
                    "-e",
                    EBPF_PROFILE_SCRIPT,
                    "-c",
                    &cargo_benchmark_command(config),
                ])
                .status()
                .context("run bpftrace profile")?,
        };

        if !status.success() {
            bail!(
                "{} profiling command exited with {status}",
                config.kind.as_str()
            );
        }

        Ok(result)
    }
}

const EBPF_PROFILE_SCRIPT: &str = r#"
BEGIN
{
  printf("pg_kinetic_ebpf_profile_start\n");
}

tracepoint:raw_syscalls:sys_enter /comm == "pg-kinetic"/
{
  @syscalls = count();
}

tracepoint:sched:sched_switch /args->prev_comm == "pg-kinetic" || args->next_comm == "pg-kinetic"/
{
  @sched_switches = count();
}

END
{
  printf("pg_kinetic_ebpf_profile_end\n");
}
"#;

fn command_exists(command: &str) -> bool {
    Command::new(command).arg("--version").output().is_ok()
}

fn cargo_benchmark_command(config: &ProfileRunConfig) -> String {
    format!(
        "cargo run -p {} -- benchmark run --scenario {} --dry-run",
        shell_quote(&config.target),
        shell_quote(path_arg(&config.scenario))
    )
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/'))
    {
        return value.to_owned();
    }

    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn path_arg(path: &Path) -> &str {
    path.to_str().unwrap_or("<invalid-path>")
}

fn redact_path(path: &Path) -> String {
    let rendered = path.to_string_lossy();
    if path.is_absolute() {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("<invalid-path>");
        return format!("<absolute>/{file_name}");
    }

    redact_value(&rendered)
}

fn redact_value(value: &str) -> String {
    if let Some((prefix, suffix)) = value.split_once("://") {
        let redacted_suffix = suffix
            .split_once('@')
            .map(|(_, host)| format!("<redacted>@{host}"))
            .unwrap_or_else(|| suffix.to_owned());
        return format!("{prefix}://{redacted_suffix}");
    }

    value.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_optional_tool_returns_a_skipped_result() {
        let runner = ProfileRunner::with_tool_lookup(|_| false);
        let config = ProfileRunConfig::new(
            ProfileTool::Flamegraph,
            "bench/scenarios/benchmark-simple-query.toml",
            "pg-kinetic",
            30_000,
            "bench/profiles/simple-query.svg",
        );

        let result = runner.run(&config).expect("missing tool is optional");

        assert_eq!(result.outcome, ProfileRunOutcome::Skipped);
        assert!(matches!(
            result.tool_status,
            ProfileToolStatus::Skipped { ref reason } if reason.contains("cargo-flamegraph")
        ));
    }

    #[test]
    fn profile_metadata_is_json_and_redacts_absolute_paths() {
        let runner = ProfileRunner::with_tool_lookup(|_| true);
        let config = ProfileRunConfig::new(
            ProfileTool::Flamegraph,
            std::env::temp_dir().join("profile-secret-scenario.toml"),
            "pg-kinetic",
            30_000,
            std::env::temp_dir().join("profile-output.svg"),
        );

        let result = ProfileRunResult::new(&config, runner.validate(ProfileTool::Flamegraph));
        let metadata = result.render_json().expect("serialize metadata");

        assert!(metadata.contains("\"profile_kind\":\"flamegraph\""));
        assert!(metadata.contains("\"duration_ms\":30000"));
        assert!(metadata.contains("<absolute>/profile-secret-scenario.toml"));
        assert!(metadata.contains("<absolute>/profile-output.svg"));
        assert!(!metadata.contains(std::env::temp_dir().to_string_lossy().as_ref()));
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn ebpf_profile_is_skipped_on_non_linux_before_lookup() {
        let runner = ProfileRunner::with_tool_lookup(|_| true);
        let status = runner.validate(ProfileTool::Ebpf);

        assert!(matches!(
            status,
            ProfileToolStatus::Skipped { ref reason }
                if reason.contains("eBPF profiling requires Linux")
        ));
    }

    #[test]
    fn ebpf_child_command_shell_quotes_user_paths() {
        let config = ProfileRunConfig::new(
            ProfileTool::Ebpf,
            "bench/scenarios/simple query's.toml",
            "pg-kinetic",
            30_000,
            "bench/profiles/simple.txt",
        );

        assert_eq!(
            cargo_benchmark_command(&config),
            "cargo run -p pg-kinetic -- benchmark run --scenario 'bench/scenarios/simple query'\"'\"'s.toml' --dry-run"
        );
    }
}
