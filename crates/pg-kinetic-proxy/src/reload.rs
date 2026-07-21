use std::{path::Path, sync::Arc};

use anyhow::Context;
use tokio::{
    sync::RwLock,
    time::{interval, MissedTickBehavior},
};
use tokio_rustls::rustls::ServerConfig;
use toml::Value;

use crate::{
    auth,
    config::{ClientTlsMode, Config, PolicyConfig, ReloadConfig},
    policy::{PolicyReloadResult, PolicyStore},
    sharding::RouteMapReloadResult,
    snapshot::{PolicyReloadSnapshot, RouteMapReloadSnapshot, SnapshotStore},
    tls,
};
use pg_kinetic_core::secrets::UserStore;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadDecision {
    Applied,
    Rejected,
    Unchanged,
}

pub fn record_route_map_reload(snapshot_store: &SnapshotStore, result: &RouteMapReloadResult) {
    snapshot_store.set_route_map_reload_snapshot(RouteMapReloadSnapshot::from(result));
}

pub fn record_policy_reload(snapshot_store: &SnapshotStore, result: &PolicyReloadResult) {
    snapshot_store.set_policy_reload_snapshot(PolicyReloadSnapshot::from(result));
}

pub fn load_effective_config(base: &Config) -> anyhow::Result<Config> {
    let Some(config_file) = base.reload.config_file.as_deref() else {
        return Ok(base.clone());
    };

    let file_config = load_file_config(config_file)?;
    merge_file_config(base, &file_config)
}

pub fn load_client_tls_server_config(config: &Config) -> anyhow::Result<Option<Arc<ServerConfig>>> {
    match config.tls.client_tls_mode {
        ClientTlsMode::Disable => Ok(None),
        _ => tls::load_server_config(&config.tls).map(Some),
    }
}

pub fn load_auth_users(config: &Config) -> anyhow::Result<Option<Arc<UserStore>>> {
    config
        .auth
        .auth_users_file
        .as_deref()
        .map(|path| auth::load_user_store(Some(path)))
        .transpose()
        .map(|store| store.map(Arc::new))
}

pub fn load_backend_credential_provider(
    config: &Config,
) -> anyhow::Result<Option<Arc<dyn auth::BackendCredentialProvider>>> {
    auth::load_backend_credential_provider(&config.auth)
}

pub fn validate_runtime_assets(config: &Config) -> anyhow::Result<()> {
    let _ = load_client_tls_server_config(config)?;
    let _ = load_auth_users(config)?;
    let _ = load_backend_credential_provider(config)?;
    Ok(())
}

pub async fn reload_once(
    base: &Config,
    active_config: &Arc<RwLock<Config>>,
) -> anyhow::Result<ReloadDecision> {
    let next_config = load_effective_config(base)?;
    let current_config = active_config.read().await.clone();

    if next_config == current_config {
        return Ok(ReloadDecision::Unchanged);
    }

    if !current_config.is_reload_compatible_with(&next_config) {
        return Ok(ReloadDecision::Rejected);
    }

    validate_runtime_assets(&next_config)?;
    *active_config.write().await = next_config;
    Ok(ReloadDecision::Applied)
}

pub fn reload_policy_once<R, S>(
    policy_store: &PolicyStore,
    next_policy: &PolicyConfig,
    active_routes: R,
    sharding_enabled: bool,
    active_shards: S,
    snapshot_store: Option<&SnapshotStore>,
) -> PolicyReloadResult
where
    R: IntoIterator,
    R::Item: AsRef<str>,
    S: IntoIterator,
    S::Item: AsRef<str>,
{
    let result = policy_store.reload(next_policy, active_routes, sharding_enabled, active_shards);
    if let Some(snapshot_store) = snapshot_store {
        record_policy_reload(snapshot_store, &result);
    }
    result
}

pub async fn spawn_reload_loop(
    base: Config,
    reload_config: ReloadConfig,
    active_config: Arc<RwLock<Config>>,
) {
    if base.reload.config_file.is_none() {
        return;
    }

    let interval_duration = reload_config.config_reload_interval();
    let mut ticker = interval(interval_duration);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        match reload_once(&base, &active_config).await {
            Ok(ReloadDecision::Applied) => {
                metrics_crate::counter!("pg_kinetic_config_reload_total", "outcome" => "applied")
                    .increment(1);
                tracing::info!("applied config reload");
            }
            Ok(ReloadDecision::Rejected) => {
                metrics_crate::counter!("pg_kinetic_config_reload_total", "outcome" => "rejected")
                    .increment(1);
                tracing::warn!("rejected unsafe config reload");
            }
            Ok(ReloadDecision::Unchanged) => {
                metrics_crate::counter!("pg_kinetic_config_reload_total", "outcome" => "unchanged")
                    .increment(1);
            }
            Err(error) => {
                metrics_crate::counter!("pg_kinetic_config_reload_total", "outcome" => "error")
                    .increment(1);
                tracing::error!(error = %error, "config reload failed");
            }
        }
    }
}

fn load_file_config(path: &Path) -> anyhow::Result<Config> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read config file {}", path.display()))?;
    toml::from_str(&contents).with_context(|| format!("parse config file {}", path.display()))
}

fn merge_file_config(base: &Config, file: &Config) -> anyhow::Result<Config> {
    let mut merged = Value::try_from(file).context("serialize file config")?;
    let base_value = Value::try_from(base).context("serialize base config")?;
    let default_value = Value::try_from(Config::default()).context("serialize default config")?;

    apply_base_overrides(&mut merged, &base_value, Some(&default_value));
    merged.try_into().context("deserialize merged config")
}

fn apply_base_overrides(target: &mut Value, base: &Value, default: Option<&Value>) {
    if is_default_value(base, default) {
        return;
    }

    match (target, base, default) {
        (
            Value::Table(target_table),
            Value::Table(base_table),
            Some(Value::Table(default_table)),
        ) => {
            for (key, base_child) in base_table {
                let default_child = default_table.get(key);
                if let Some(target_child) = target_table.get_mut(key) {
                    apply_base_overrides(target_child, base_child, default_child);
                } else if !is_default_value(base_child, default_child) {
                    target_table.insert(key.clone(), base_child.clone());
                }
            }
        }
        (Value::Table(target_table), Value::Table(base_table), _) => {
            for (key, base_child) in base_table {
                if let Some(target_child) = target_table.get_mut(key) {
                    apply_base_overrides(target_child, base_child, None);
                } else if !is_default_value(base_child, None) {
                    target_table.insert(key.clone(), base_child.clone());
                }
            }
        }
        (target_value, base_value, _) => {
            *target_value = base_value.clone();
        }
    }
}

fn is_default_value(base: &Value, default: Option<&Value>) -> bool {
    default.is_some_and(|default| base == default)
}
