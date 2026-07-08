use std::{fs, path::PathBuf, process};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use pg_kinetic::config::Config;
use pg_kinetic::route::{QueryClass, RouteKey};
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

#[derive(Debug, Deserialize)]
struct RoutePreviewFileConfig {
    sharding: pg_kinetic::config::ShardingConfig,
}

fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let Cli { config, command } = cli;

    if let Some(Command::RoutePreview(args)) = command {
        return run_route_preview(config, args);
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?
        .block_on(pg_kinetic::run(config))
        .context("pg-kinetic runtime failed")
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
