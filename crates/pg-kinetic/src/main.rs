use std::{
    fs,
    path::{Path, PathBuf},
    process,
    sync::Arc,
};

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use pg_kinetic::config::Config;
use pg_kinetic::core::benchmark::{BenchmarkScenario, BenchmarkTarget, BenchmarkValidationError};
use pg_kinetic::core::{
    compatibility::{CompatibilityLanguage, CompatibilityTarget},
    lsn::FreshnessStatus,
    policy::PolicyAction,
    regression::{RegressionCategory, RegressionPlatform},
    routing::QueryClass as RoutingQueryClass,
    session::TransactionAccessMode,
};
use pg_kinetic::route::{QueryClass, RouteKey};
use pg_kinetic_proxy::benchmark::{
    compare_benchmark_reports, prepare_benchmark_results, validate_benchmark_scenario,
    BenchmarkReportOutcome, BenchmarkRunReport,
};
use pg_kinetic_proxy::compatibility::{
    CompatibilityRunConfig, CompatibilityRunner, CompatibilitySuiteSelector,
};
use pg_kinetic_proxy::policy::{preview_policy, PolicyPreviewError, PolicyPreviewEvaluation};
use pg_kinetic_proxy::preflight::PreflightRunner;
use pg_kinetic_proxy::profile::{ProfileRunConfig, ProfileRunner, ProfileTool};
use pg_kinetic_proxy::regression::{
    load_regression_manifest, redact_sensitive_text, score_benchmark_reports, write_ignored_output,
    RegressionRunner, RegressionSelection,
};
use pg_kinetic_proxy::sharding::{preview_route, RoutePreviewError, RoutePreviewRequest};
use serde::Deserialize;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Parser)]
#[command(name = "pg-kinetic", about = "Low-overhead PostgreSQL wire proxy")]
struct Cli {
    #[command(flatten)]
    config: Config,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    RoutePreview(RoutePreviewArgs),
    PolicyPreview(PolicyPreviewArgs),
    Benchmark(BenchmarkArgs),
    Compat(CompatArgs),
    Regression(RegressionArgs),
    Profile(ProfileArgs),
    Preflight(PreflightArgs),
}

#[derive(Debug, Args)]
struct RoutePreviewArgs {
    #[arg(long)]
    config: PathBuf,

    #[arg(long)]
    database: String,

    #[arg(long)]
    user: String,

    #[arg(long)]
    sql: String,

    #[arg(long)]
    application_name: Option<String>,
}

#[derive(Debug, Args)]
struct PolicyPreviewArgs {
    #[arg(long)]
    config: PathBuf,

    #[arg(long)]
    database: String,

    #[arg(long)]
    user: String,

    #[arg(long)]
    route: String,

    #[arg(long)]
    shard: String,

    #[arg(long, value_parser = parse_routing_query_class)]
    query_class: RoutingQueryClass,

    #[arg(long)]
    application_name: Option<String>,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct BenchmarkArgs {
    #[command(subcommand)]
    command: BenchmarkCommand,
}

#[derive(Debug, Args)]
struct RegressionArgs {
    #[command(subcommand)]
    command: RegressionCommand,
}

#[derive(Debug, Args)]
struct CompatArgs {
    #[command(subcommand)]
    command: CompatCommand,
}

#[derive(Debug, Args)]
struct ProfileArgs {
    #[command(subcommand)]
    command: ProfileCommand,
}

#[derive(Debug, Args)]
struct PreflightArgs {
    #[arg(long)]
    config: PathBuf,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,
}

#[derive(Debug, Subcommand)]
enum BenchmarkCommand {
    Validate(BenchmarkValidateArgs),
    Run(BenchmarkRunArgs),
    Compare(BenchmarkCompareArgs),
    Score(BenchmarkScoreArgs),
}

#[derive(Debug, Subcommand)]
enum RegressionCommand {
    List(RegressionListArgs),
    Run(RegressionRunArgs),
}

#[derive(Debug, Subcommand)]
enum CompatCommand {
    List(CompatListArgs),
    Run(CompatRunArgs),
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    Validate,
    Run(ProfileRunArgs),
}

#[derive(Debug, Args)]
struct ProfileRunArgs {
    #[arg(long)]
    scenario: PathBuf,

    #[arg(long, value_parser = parse_profile_tool)]
    kind: ProfileTool,

    #[arg(long, default_value = "pg-kinetic")]
    target: String,

    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct BenchmarkValidateArgs {
    #[arg(long)]
    scenario: PathBuf,
}

#[derive(Debug, Args)]
struct BenchmarkRunArgs {
    #[arg(long)]
    scenario: PathBuf,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct BenchmarkCompareArgs {
    #[arg(long)]
    baseline: PathBuf,

    #[arg(long)]
    current: PathBuf,
}

#[derive(Debug, Args)]
struct BenchmarkScoreArgs {
    #[arg(long)]
    baseline: PathBuf,

    #[arg(long)]
    current: PathBuf,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,

    #[arg(long)]
    release: bool,
}

#[derive(Debug, Args)]
struct RegressionListArgs {
    #[arg(long)]
    manifest: PathBuf,

    #[arg(long, value_enum)]
    category: Option<RegressionCategory>,

    #[arg(long, value_enum)]
    platform: Option<RegressionPlatform>,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct RegressionRunArgs {
    #[arg(long)]
    manifest: PathBuf,

    #[arg(long, value_enum)]
    category: Option<RegressionCategory>,

    #[arg(long, value_enum)]
    platform: Option<RegressionPlatform>,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct CompatListArgs {
    #[arg(long, default_value = "regression/manifest.toml")]
    manifest: PathBuf,

    #[arg(long, value_parser = parse_compatibility_language)]
    language: Option<CompatibilityLanguage>,

    #[arg(long)]
    library: Option<String>,

    #[arg(long, value_parser = parse_compatibility_target)]
    target: Option<CompatibilityTarget>,

    #[arg(long)]
    category: Option<String>,

    #[arg(long)]
    smoke: bool,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,
}

#[derive(Debug, Args)]
struct CompatRunArgs {
    #[arg(long, default_value = "regression/manifest.toml")]
    manifest: PathBuf,

    #[arg(long, value_parser = parse_compatibility_language)]
    language: Option<CompatibilityLanguage>,

    #[arg(long)]
    library: Option<String>,

    #[arg(long, value_parser = parse_compatibility_target)]
    target: Option<CompatibilityTarget>,

    #[arg(long)]
    category: Option<String>,

    #[arg(long)]
    smoke: bool,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    format: OutputFormat,

    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Json,
}

#[derive(Debug, Deserialize)]
struct RoutePreviewFileConfig {
    sharding: pg_kinetic::config::ShardingConfig,
}

#[derive(Debug, Deserialize)]
struct PolicyPreviewFileConfig {
    policy: pg_kinetic::config::PolicyConfig,
    #[serde(default)]
    sharding: pg_kinetic::config::ShardingConfig,
}

fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let Cli { config, command } = cli;

    match command {
        Some(Command::RoutePreview(args)) => return run_route_preview(config, args),
        Some(Command::PolicyPreview(args)) => return run_policy_preview(config, args),
        Some(Command::Benchmark(args)) => return run_benchmark(config, args),
        Some(Command::Compat(args)) => return run_compat(args),
        Some(Command::Regression(args)) => return run_regression(args),
        Some(Command::Profile(args)) => return run_profile(config, args),
        Some(Command::Preflight(args)) => return run_preflight(config, args),
        None => {}
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?
        .block_on(pg_kinetic::run(config))
        .context("pg-kinetic runtime failed")
}

fn run_policy_preview(_config: Config, args: PolicyPreviewArgs) -> anyhow::Result<()> {
    let PolicyPreviewArgs {
        config: preview_config_path,
        database,
        user,
        route,
        shard,
        query_class,
        application_name,
        format,
    } = args;

    let preview_file = match load_policy_preview_config(&preview_config_path) {
        Ok(file) => file,
        Err(error) => {
            println!(
                "{}",
                render_policy_preview_error(
                    &route,
                    Some(&shard),
                    &PolicyPreviewError::new("config_load_failed", error.to_string())
                )
            );
            process::exit(1);
        }
    };

    let input = build_policy_preview_input(
        &database,
        &user,
        application_name.as_deref(),
        &route,
        &shard,
        query_class,
    );
    let preview = match preview_policy(
        &preview_file.policy,
        preview_file.sharding.sharding_enabled,
        &input,
    ) {
        Ok(preview) => preview,
        Err(error) => {
            println!(
                "{}",
                render_policy_preview_error(&route, Some(&shard), &error)
            );
            process::exit(1);
        }
    };

    match format {
        OutputFormat::Json => {
            println!(
                "{}",
                render_policy_preview_success(&route, Some(&shard), &preview)
            );
        }
    }

    Ok(())
}

fn run_route_preview(_config: Config, args: RoutePreviewArgs) -> anyhow::Result<()> {
    let RoutePreviewArgs {
        config: preview_config_path,
        database,
        user,
        sql,
        application_name,
    } = args;

    let request = RoutePreviewRequest::new(&database, &user, application_name.as_deref(), &sql);
    let route_label = preview_route_label(&database, &user, application_name.as_deref());
    let preview_file = match load_route_preview_config(&preview_config_path) {
        Ok(file) => file,
        Err(error) => {
            println!("{}", render_preview_error(&route_label, &error));
            process::exit(1);
        }
    };

    match preview_route(&preview_file.sharding, request) {
        Ok(summary) => {
            println!("{}", render_preview_success(&summary));
            Ok(())
        }
        Err(error) => {
            println!("{}", render_preview_error(&route_label, &error));
            process::exit(1);
        }
    }
}

fn run_benchmark(_config: Config, args: BenchmarkArgs) -> anyhow::Result<()> {
    match args.command {
        BenchmarkCommand::Validate(args) => run_benchmark_validate(args),
        BenchmarkCommand::Run(args) => run_benchmark_run(args),
        BenchmarkCommand::Compare(args) => run_benchmark_compare(args),
        BenchmarkCommand::Score(args) => run_benchmark_score(args),
    }
}

fn run_regression(args: RegressionArgs) -> anyhow::Result<()> {
    match args.command {
        RegressionCommand::List(args) => run_regression_list(args),
        RegressionCommand::Run(args) => run_regression_run(args),
    }
}

fn run_compat(args: CompatArgs) -> anyhow::Result<()> {
    match args.command {
        CompatCommand::List(args) => run_compat_list(args),
        CompatCommand::Run(args) => run_compat_run(args),
    }
}

fn run_compat_list(args: CompatListArgs) -> anyhow::Result<()> {
    let CompatListArgs {
        manifest,
        language,
        library,
        target,
        category,
        smoke,
        format,
    } = args;
    let selector = CompatibilitySuiteSelector {
        language,
        target,
        smoke,
    };
    let suites = CompatibilityRunner
        .list(&manifest, selector, library.as_deref(), category.as_deref())
        .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    match format {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "ok": true,
                "suites": suites.iter().map(|suite| serde_json::json!({
                    "id": suite.id(),
                    "language": suite.language().as_str(),
                    "library": suite.library().name(),
                    "version": suite.library().version(),
                    "target": suite.target().as_str(),
                    "timeout_seconds": suite.timeout().as_secs(),
                    "required_services": suite
                        .required_services()
                        .iter()
                        .map(|service| service.as_ref())
                        .collect::<Vec<_>>(),
                    "artifact_policy": suite.artifact_policy().as_str(),
                    "artifact_path": suite.artifact_path(),
                    "smoke": suite.smoke(),
                })).collect::<Vec<_>>(),
            }))
            .context("serialize compatibility suite list")?
        ),
    }
    Ok(())
}

fn run_compat_run(args: CompatRunArgs) -> anyhow::Result<()> {
    let CompatRunArgs {
        manifest,
        language,
        library,
        target,
        category,
        smoke,
        format,
        output,
    } = args;
    let report = CompatibilityRunner
        .run(&CompatibilityRunConfig {
            manifest_path: manifest,
            selector: CompatibilitySuiteSelector {
                language,
                target,
                smoke,
            },
            library,
            category,
        })
        .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    let rendered = report.render_json();
    if let Some(output) = output {
        write_ignored_output(&output, &rendered)
            .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    }
    match format {
        OutputFormat::Json => println!("{rendered}"),
    }
    if report.has_failures() {
        process::exit(1);
    }
    Ok(())
}

fn run_regression_list(args: RegressionListArgs) -> anyhow::Result<()> {
    let RegressionListArgs {
        manifest: manifest_path,
        category,
        platform,
        format,
    } = args;
    let manifest = load_regression_manifest(&manifest_path)
        .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    let selection = RegressionSelection { category, platform };
    let cases = manifest
        .cases()
        .iter()
        .filter(|case| selection.matches(case))
        .map(|case| {
            serde_json::json!({
                "id": case.id(),
                "category": case.category().as_str(),
                "platform": case.platform().as_str(),
                "timeout_seconds": case.timeout().as_secs(),
                "services": case
                    .services()
                    .iter()
                    .map(|service| service.as_ref())
                    .collect::<Vec<_>>(),
                "success_marker": case.success_marker(),
                "artifact_policy": case.artifact_policy().as_str(),
                "artifact_path": case.artifact_path(),
            })
        })
        .collect::<Vec<_>>();
    match format {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({ "ok": true, "cases": cases }))
                .context("serialize regression case list")?
        ),
    }
    Ok(())
}

fn run_regression_run(args: RegressionRunArgs) -> anyhow::Result<()> {
    let RegressionRunArgs {
        manifest: manifest_path,
        category,
        platform,
        output,
        format,
    } = args;
    let manifest = load_regression_manifest(&manifest_path)
        .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    let report = RegressionRunner
        .run(&manifest, RegressionSelection { category, platform })
        .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    let rendered = report.render_json();
    if let Some(output) = output {
        write_ignored_output(&output, &rendered)
            .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    }
    match format {
        OutputFormat::Json => println!("{rendered}"),
    }
    if report.has_failures() {
        process::exit(1);
    }
    Ok(())
}

fn run_profile(_config: Config, args: ProfileArgs) -> anyhow::Result<()> {
    let runner = ProfileRunner::new();
    match args.command {
        ProfileCommand::Validate => {
            let statuses = ProfileTool::ALL
                .into_iter()
                .map(|tool| (tool.as_str(), runner.validate(tool)))
                .collect::<std::collections::BTreeMap<_, _>>();
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({ "ok": true, "tools": statuses }))
                    .context("serialize profile validation")?
            );
            Ok(())
        }
        ProfileCommand::Run(args) => run_profile_run(&runner, args),
    }
}

fn run_profile_run(runner: &ProfileRunner, args: ProfileRunArgs) -> anyhow::Result<()> {
    let ProfileRunArgs {
        scenario,
        kind,
        target,
        output,
    } = args;
    let scenario_config = validate_benchmark_scenario(&scenario)
        .map_err(|error| anyhow::anyhow!("profile scenario validation failed: {error}"))?;
    let output = output.unwrap_or_else(|| default_profile_output(&scenario, kind));
    let config = ProfileRunConfig::new(
        kind,
        scenario,
        target,
        scenario_config.duration_ms(),
        output,
    );
    let result = runner.run(&config)?;
    println!("{}", result.render_json()?);
    Ok(())
}

fn default_profile_output(scenario: &Path, kind: ProfileTool) -> PathBuf {
    let scenario_name = scenario
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("profile");
    let extension = match kind {
        ProfileTool::Flamegraph => "svg",
        ProfileTool::Perf => "data",
    };
    PathBuf::from("bench").join("profiles").join(format!(
        "{scenario_name}-{}.{}",
        kind.as_str(),
        extension
    ))
}

fn run_preflight(_config: Config, args: PreflightArgs) -> anyhow::Result<()> {
    let PreflightArgs { config, format } = args;
    let report = PreflightRunner::new(config).run();

    match format {
        OutputFormat::Json => {
            println!("{}", report.render_json());
        }
    }

    if report.has_errors() {
        process::exit(1);
    }

    Ok(())
}

fn run_benchmark_validate(args: BenchmarkValidateArgs) -> anyhow::Result<()> {
    let BenchmarkValidateArgs { scenario } = args;

    match validate_benchmark_scenario(&scenario) {
        Ok(scenario) => {
            println!("{}", render_benchmark_validation_success(&scenario));
            Ok(())
        }
        Err(error) => {
            println!("{}", render_benchmark_error(&scenario, &error));
            process::exit(1);
        }
    }
}

fn run_benchmark_run(args: BenchmarkRunArgs) -> anyhow::Result<()> {
    let BenchmarkRunArgs {
        scenario,
        format,
        output,
        dry_run,
    } = args;

    let scenario = match validate_benchmark_scenario(&scenario) {
        Ok(scenario) => scenario,
        Err(error) => {
            println!("{}", render_benchmark_error(&scenario, &error));
            process::exit(1);
        }
    };

    if !dry_run {
        anyhow::bail!(
            "live benchmark execution is not implemented yet; rerun with --dry-run to produce a structural report"
        );
    }

    let results = prepare_benchmark_results(&scenario);
    let report = BenchmarkRunReport::new(scenario, results, dry_run);

    match format {
        OutputFormat::Json => {
            let report = report.render_json();
            if let Some(output) = output {
                if let Some(parent) = output.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("create benchmark report directory {}", parent.display())
                    })?;
                }
                fs::write(&output, &report)
                    .with_context(|| format!("write benchmark report {}", output.display()))?;
            }
            println!("{report}");
        }
    }

    Ok(())
}

fn run_benchmark_compare(args: BenchmarkCompareArgs) -> anyhow::Result<()> {
    let BenchmarkCompareArgs { baseline, current } = args;
    let report = match compare_benchmark_reports(&baseline, &current) {
        Ok(report) => report,
        Err(error) => {
            println!(
                "{}",
                render_benchmark_report_error(&baseline, &current, &error)
            );
            process::exit(1);
        }
    };
    println!("{}", report.render_json());

    if matches!(report.outcome(), BenchmarkReportOutcome::Failed) {
        process::exit(1);
    }

    Ok(())
}

fn run_benchmark_score(args: BenchmarkScoreArgs) -> anyhow::Result<()> {
    let BenchmarkScoreArgs {
        baseline,
        current,
        format,
        release,
    } = args;
    let report = score_benchmark_reports(&baseline, &current)
        .map_err(|error| anyhow::anyhow!(redact_sensitive_text(&error.to_string())))?;
    match format {
        OutputFormat::Json => println!("{}", report.render_json()),
    }
    if release && report.release_failed() {
        process::exit(1);
    }
    Ok(())
}

fn build_policy_preview_input(
    database: &str,
    user: &str,
    application_name: Option<&str>,
    route: &str,
    shard: &str,
    query_class: RoutingQueryClass,
) -> pg_kinetic_proxy::policy::PolicyEvalInput {
    let backend_role = query_class.target_role();
    let transaction_mode = match query_class {
        RoutingQueryClass::ReadOnly | RoutingQueryClass::ReadCandidate => {
            TransactionAccessMode::ReadOnly
        }
        _ => TransactionAccessMode::ReadWrite,
    };

    pg_kinetic_proxy::policy::PolicyEvalInput {
        database: Arc::from(database),
        user: Arc::from(user),
        application_name: application_name.map(Arc::from),
        route: Some(Arc::from(route)),
        shard: Some(Arc::from(shard)),
        backend_role,
        query_class,
        transaction_mode,
        freshness_state: FreshnessStatus::Unknown,
        routing_decision: None,
        shard_route_decision: None,
        password: Some(Arc::from("preview-password")),
        bind_values: vec![Arc::from("preview-bind-value")],
        tls_certificate_body: Some(Arc::from("-----BEGIN CERTIFICATE----- preview")),
        raw_sql_text: Some(Arc::from("SELECT preview_secret")),
        secrets: vec![Arc::from("preview-secret-token")],
    }
}

fn render_policy_preview_success(
    original_route: &str,
    original_shard: Option<&str>,
    preview: &PolicyPreviewEvaluation,
) -> String {
    let (policy_adjusted_route, policy_adjusted_shard) =
        adjusted_preview_targets(original_route, original_shard, &preview.action);

    format!(
        "{{\"ok\":true,\"policy_mode\":{},\"original_route\":{},\"policy_adjusted_route\":{},\"original_shard\":{},\"policy_adjusted_shard\":{},\"action\":{},\"dry_run_outcome\":{},\"dry_run_reason\":{},\"deny_reason\":{},\"sqlstate\":{},\"context\":{}}}",
        json_string(preview.policy_mode.as_str()),
        json_string(original_route),
        json_option(policy_adjusted_route.as_deref()),
        json_option(original_shard),
        json_option(policy_adjusted_shard.as_deref()),
        json_string(preview.action.as_str()),
        json_string(preview.audit_event.outcome.as_str()),
        json_option(preview.audit_event.reason.as_deref()),
        json_option(preview.deny_reason.as_deref()),
        json_option(policy_sqlstate(&preview.action)),
        json_string(&preview.audit_event.context.to_string())
    )
}

fn render_policy_preview_error(
    original_route: &str,
    original_shard: Option<&str>,
    error: &PolicyPreviewError,
) -> String {
    format!(
        "{{\"ok\":false,\"policy_mode\":null,\"original_route\":{},\"policy_adjusted_route\":null,\"original_shard\":{},\"policy_adjusted_shard\":null,\"action\":null,\"dry_run_outcome\":null,\"dry_run_reason\":null,\"deny_reason\":null,\"sqlstate\":null,\"context\":null,\"error\":{{\"code\":{},\"message\":{}}}}}",
        json_string(original_route),
        json_option(original_shard),
        json_string(&error.code),
        json_string(&error.message)
    )
}

fn adjusted_preview_targets(
    original_route: &str,
    original_shard: Option<&str>,
    action: &PolicyAction,
) -> (Option<String>, Option<String>) {
    match action {
        PolicyAction::Allow | PolicyAction::RequirePrimary | PolicyAction::RequireReplica => (
            Some(original_route.to_owned()),
            original_shard.map(ToOwned::to_owned),
        ),
        PolicyAction::Deny { .. } => (None, None),
        PolicyAction::RouteOverride { target_id } => (
            Some(target_id.as_str().to_owned()),
            original_shard.map(ToOwned::to_owned),
        ),
        PolicyAction::ShardOverride { target_id } => (
            Some(original_route.to_owned()),
            Some(target_id.as_str().to_owned()),
        ),
    }
}

fn policy_sqlstate(action: &PolicyAction) -> Option<&'static str> {
    match action {
        PolicyAction::Deny { sqlstate, .. } => Some(*sqlstate),
        _ => None,
    }
}

fn load_route_preview_config(path: &PathBuf) -> Result<RoutePreviewFileConfig, RoutePreviewError> {
    let contents = fs::read_to_string(path).map_err(|error| {
        RoutePreviewError::new(
            "config_load_failed",
            format!("read {}: {error}", path.display()),
        )
    })?;
    toml::from_str(&contents).map_err(|error| {
        RoutePreviewError::new(
            "config_load_failed",
            format!("parse {}: {error}", path.display()),
        )
    })
}

fn load_policy_preview_config(
    path: &PathBuf,
) -> Result<PolicyPreviewFileConfig, PolicyPreviewError> {
    let contents = fs::read_to_string(path).map_err(|error| {
        PolicyPreviewError::new(
            "config_load_failed",
            format!("read {}: {error}", path.display()),
        )
    })?;
    toml::from_str(&contents).map_err(|error| {
        PolicyPreviewError::new(
            "config_load_failed",
            format!("parse {}: {error}", path.display()),
        )
    })
}

fn preview_route_label(database: &str, user: &str, application_name: Option<&str>) -> String {
    RouteKey::new(database, user, application_name, None, QueryClass::Default).metric_label()
}

fn render_preview_success(summary: &pg_kinetic_proxy::sharding::RoutePreviewSummary) -> String {
    format!(
        "{{\"ok\":true,\"route\":{},\"shard_id\":{},\"backend_role\":{},\"reason\":{},\"shard_reason\":{}}}",
        json_string(&summary.route),
        json_option(summary.shard_id.as_deref()),
        json_option(summary.backend_role.as_deref()),
        json_string(&summary.reason),
        json_option(summary.shard_reason.as_deref())
    )
}

fn render_preview_error(route: &str, error: &RoutePreviewError) -> String {
    format!(
        "{{\"ok\":false,\"route\":{},\"shard_id\":null,\"backend_role\":null,\"reason\":{},\"error\":{{\"code\":{},\"message\":{}}}}}",
        json_string(route),
        json_string(&error.code),
        json_string(&error.code),
        json_string(&error.message)
    )
}

fn render_benchmark_validation_success(scenario: &BenchmarkScenario) -> String {
    format!(
        "{{\"ok\":true,\"scenario\":{},\"targets\":{}}}",
        render_benchmark_scenario(scenario),
        render_benchmark_targets(scenario.targets())
    )
}

fn render_benchmark_error(path: &Path, error: &BenchmarkValidationError) -> String {
    format!(
        "{{\"ok\":false,\"scenario\":{},\"error\":{{\"code\":\"benchmark_validation_failed\",\"message\":{}}}}}",
        json_string(path.to_str().unwrap_or("<invalid-path>")),
        json_string(&error.to_string())
    )
}

fn render_benchmark_report_error(
    baseline: &Path,
    current: &Path,
    error: &pg_kinetic_proxy::benchmark::BenchmarkReportError,
) -> String {
    format!(
        "{{\"ok\":false,\"baseline\":{},\"current\":{},\"error\":{{\"code\":\"benchmark_report_failed\",\"message\":{}}}}}",
        json_string(baseline.to_str().unwrap_or("<invalid-path>")),
        json_string(current.to_str().unwrap_or("<invalid-path>")),
        json_string(&error.to_string())
    )
}

fn render_benchmark_scenario(scenario: &BenchmarkScenario) -> String {
    format!(
        "{{\"name\":{},\"driver\":{},\"workload\":{},\"duration_ms\":{},\"warmup_ms\":{}}}",
        json_string(scenario.name()),
        json_string(scenario.driver().as_str()),
        json_string(scenario.workload().as_str()),
        scenario.duration_ms(),
        scenario.warmup_ms()
    )
}

fn render_benchmark_targets(targets: &[BenchmarkTarget]) -> String {
    let rendered_targets = targets
        .iter()
        .map(render_benchmark_target)
        .collect::<Vec<_>>()
        .join(",");

    format!("[{}]", rendered_targets)
}

fn render_benchmark_target(target: &BenchmarkTarget) -> String {
    format!(
        "{{\"label\":{},\"comparison\":{},\"dsn\":{}}}",
        json_string(target.label()),
        json_string(target.comparison().as_str()),
        json_string(&target.redacted_dsn())
    )
}

fn json_option(value: Option<&str>) -> String {
    value
        .map(json_string)
        .unwrap_or_else(|| String::from("null"))
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0C}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write;
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}

fn parse_routing_query_class(value: &str) -> Result<RoutingQueryClass, String> {
    match value {
        "write" => Ok(RoutingQueryClass::Write),
        "read_only" => Ok(RoutingQueryClass::ReadOnly),
        "read_candidate" => Ok(RoutingQueryClass::ReadCandidate),
        "transaction_control" => Ok(RoutingQueryClass::TransactionControl),
        "session_mutation" => Ok(RoutingQueryClass::SessionMutation),
        "copy" => Ok(RoutingQueryClass::Copy),
        "unknown" => Ok(RoutingQueryClass::Unknown),
        _ => Err(format!(
            "invalid query class '{value}', expected one of: write, read_only, read_candidate, transaction_control, session_mutation, copy, unknown"
        )),
    }
}

fn parse_profile_tool(value: &str) -> Result<ProfileTool, String> {
    match value {
        "flamegraph" => Ok(ProfileTool::Flamegraph),
        "perf" => Ok(ProfileTool::Perf),
        _ => Err(format!(
            "invalid profile kind '{value}', expected one of: flamegraph, perf"
        )),
    }
}

fn parse_compatibility_language(value: &str) -> Result<CompatibilityLanguage, String> {
    value.parse()
}

fn parse_compatibility_target(value: &str) -> Result<CompatibilityTarget, String> {
    value.parse()
}
