use std::{fs, path::PathBuf, process, sync::Arc};

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use pg_kinetic::config::Config;
use pg_kinetic::core::benchmark::{
    BenchmarkComparison, BenchmarkDriver, BenchmarkMetric, BenchmarkResult, BenchmarkScenario,
    BenchmarkTarget, BenchmarkValidationError,
};
use pg_kinetic::core::{
    lsn::FreshnessStatus, policy::PolicyAction, routing::QueryClass as RoutingQueryClass,
    session::TransactionAccessMode,
};
use pg_kinetic::route::{QueryClass, RouteKey};
use pg_kinetic_proxy::benchmark::{
    prepare_benchmark_results, validate_benchmark_scenario,
};
use pg_kinetic_proxy::policy::{preview_policy, PolicyPreviewError, PolicyPreviewEvaluation};
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

#[derive(Debug, Subcommand)]
enum BenchmarkCommand {
    Validate(BenchmarkValidateArgs),
    Run(BenchmarkRunArgs),
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
    }
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
    let BenchmarkRunArgs { scenario, format } = args;

    let scenario = match validate_benchmark_scenario(&scenario) {
        Ok(scenario) => scenario,
        Err(error) => {
            println!("{}", render_benchmark_error(&scenario, &error));
            process::exit(1);
        }
    };

    let results = prepare_benchmark_results(&scenario);

    match format {
        OutputFormat::Json => {
            println!("{}", render_benchmark_run_success(&scenario, &results));
        }
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

fn render_benchmark_run_success(scenario: &BenchmarkScenario, results: &[BenchmarkResult]) -> String {
    let rendered_results = results
        .iter()
        .map(render_benchmark_result)
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "{{\"ok\":true,\"scenario\":{},\"results\":[{}]}}",
        render_benchmark_scenario(scenario),
        rendered_results
    )
}

fn render_benchmark_error(path: &PathBuf, error: &BenchmarkValidationError) -> String {
    format!(
        "{{\"ok\":false,\"scenario\":{},\"error\":{{\"code\":\"benchmark_validation_failed\",\"message\":{}}}}}",
        json_string(path.to_str().unwrap_or("<invalid-path>")),
        json_string(&error.to_string())
    )
}

fn render_benchmark_scenario(scenario: &BenchmarkScenario) -> String {
    format!(
        "{{\"name\":{},\"driver\":{},\"duration_ms\":{},\"warmup_ms\":{}}}",
        json_string(scenario.name()),
        json_string(scenario.driver().as_str()),
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

fn render_benchmark_result(result: &BenchmarkResult) -> String {
    format!(
        "{{\"scenario\":{},\"target\":{},\"driver\":{},\"duration_ms\":{},\"metrics\":{}}}",
        json_string(result.scenario()),
        render_benchmark_target(result.target()),
        json_string(result.driver().as_str()),
        result.duration_ms(),
        render_benchmark_metric(result.metrics())
    )
}

fn render_benchmark_metric(metric: &BenchmarkMetric) -> String {
    format!(
        "{{\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"throughput_qps\":{},\"cpu_label\":{},\"memory_label\":{},\"error_rate\":{}}}",
        metric.p50_ms(),
        metric.p95_ms(),
        metric.p99_ms(),
        metric.throughput_qps(),
        json_string(metric.cpu_label()),
        json_string(metric.memory_label()),
        metric.error_rate()
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
