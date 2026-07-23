use std::{fmt, path::Path, sync::Arc};

use anyhow::Context;
use arc_swap::ArcSwapOption;
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
    pool::{RoutePoolRegistry, RoutePoolRetirementTargets, RoutePools},
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

#[derive(Clone)]
pub struct BackendCredentialCache {
    credentials: Arc<ArcSwapOption<auth::BackendCredentials>>,
}

impl fmt::Debug for BackendCredentialCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendCredentialCache")
            .field("configured", &self.credentials.load().is_some())
            .finish()
    }
}

impl BackendCredentialCache {
    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        let cache = Self {
            credentials: Arc::new(ArcSwapOption::empty()),
        };
        cache.reload_from_config(config)?;
        Ok(cache)
    }

    pub fn load(&self) -> Option<Arc<auth::BackendCredentials>> {
        self.credentials.load_full()
    }

    fn reload_from_config(&self, config: &Config) -> anyhow::Result<()> {
        self.store(auth::load_backend_credentials(&config.auth)?.map(Arc::new));
        Ok(())
    }

    fn store(&self, credentials: Option<Arc<auth::BackendCredentials>>) {
        self.credentials.store(credentials);
    }
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
    reload_once_with_pools_and_credentials(base, active_config, None, None, None).await
}

pub async fn reload_once_with_pools(
    base: &Config,
    active_config: &Arc<RwLock<Config>>,
    route_pools: Option<&Arc<RoutePools>>,
    route_pool_registry: Option<&Arc<RoutePoolRegistry>>,
) -> anyhow::Result<ReloadDecision> {
    reload_once_with_pools_and_credentials(
        base,
        active_config,
        route_pools,
        route_pool_registry,
        None,
    )
    .await
}

pub async fn reload_once_with_pools_and_credentials(
    base: &Config,
    active_config: &Arc<RwLock<Config>>,
    route_pools: Option<&Arc<RoutePools>>,
    route_pool_registry: Option<&Arc<RoutePoolRegistry>>,
    backend_credentials: Option<&BackendCredentialCache>,
) -> anyhow::Result<ReloadDecision> {
    let next_config = load_effective_config(base)?;
    let next_backend_credentials = backend_credentials
        .map(|_| {
            auth::load_backend_credentials(&next_config.auth)
                .map(|credentials| credentials.map(Arc::new))
        })
        .transpose()?;
    let mut current_config = active_config.write().await;

    if next_config == *current_config {
        return Ok(ReloadDecision::Unchanged);
    }

    if !current_config.is_reload_compatible_with(&next_config) {
        return Ok(ReloadDecision::Rejected);
    }

    validate_runtime_assets(&next_config)?;
    *current_config = next_config;
    if let (Some(backend_credentials), Some(next_backend_credentials)) =
        (backend_credentials, next_backend_credentials)
    {
        backend_credentials.store(next_backend_credentials);
    }
    drop(current_config);
    if let Some(route_pool_registry) = route_pool_registry.filter(|registry| !registry.is_empty()) {
        route_pool_registry.retire_idle_backends().await;
    } else if let Some(route_pools) = route_pools {
        route_pools.retire_idle_backends().await;
    }
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
    route_pools: Arc<RoutePools>,
    route_pool_registry: Arc<RoutePoolRegistry>,
    backend_credentials: BackendCredentialCache,
    route_pool_retirement_targets: RoutePoolRetirementTargets,
) {
    if base.reload.config_file.is_none() {
        return;
    }

    let interval_duration = reload_config.config_reload_interval();
    let mut ticker = interval(interval_duration);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        match reload_once_with_pools_and_credentials(
            &base,
            &active_config,
            Some(&route_pools),
            Some(&route_pool_registry),
            Some(&backend_credentials),
        )
        .await
        {
            Ok(ReloadDecision::Applied) => {
                route_pool_retirement_targets.retire_idle_backends().await;
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
