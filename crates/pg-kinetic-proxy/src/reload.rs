use std::{path::Path, path::PathBuf, sync::Arc};

use anyhow::{bail, Context};
use serde::Deserialize;
use tokio::{
    sync::RwLock,
    time::{interval, MissedTickBehavior},
};
use tokio_rustls::rustls::ServerConfig;

use crate::{
    auth,
    config::{
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, CapacityConfig,
        ClientTlsMode, Config, ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig,
        PerformanceConfig, QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    tls,
};
use pg_kinetic_core::{recovery::RecoveryMode, secrets::UserStore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadDecision {
    Applied,
    Rejected,
    Unchanged,
}

#[derive(Clone, Debug)]
pub struct ReloadableConfig {
    pub performance: PerformanceConfig,
    pub qos: QosConfig,
    pub socket: SocketConfig,
    pub client_tls_server_config: Option<Arc<ServerConfig>>,
    pub auth_users: Option<Arc<UserStore>>,
}

impl ReloadableConfig {
    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        let client_tls_server_config = match config.tls.client_tls_mode {
            ClientTlsMode::Disable => None,
            _ => Some(tls::load_server_config(&config.tls)?),
        };

        let auth_users = config
            .auth
            .auth_users_file
            .as_deref()
            .map(|path| auth::load_user_store(Some(path)))
            .transpose()?
            .map(Arc::new);

        Ok(Self {
            performance: config.performance.clone(),
            qos: config.qos.clone(),
            socket: config.socket.clone(),
            client_tls_server_config,
            auth_users,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    connection: Option<FileConnectionConfig>,
    #[serde(default)]
    capacity: Option<FileCapacityConfig>,
    #[serde(default)]
    performance: Option<FilePerformanceConfig>,
    #[serde(default)]
    qos: Option<FileQosConfig>,
    #[serde(default)]
    observability: Option<FileObservabilityConfig>,
    #[serde(default)]
    tls: Option<FileTlsConfig>,
    #[serde(default)]
    auth: Option<FileAuthConfig>,
    #[serde(default)]
    reload: Option<FileReloadConfig>,
    #[serde(default)]
    drain: Option<FileDrainConfig>,
    #[serde(default)]
    health: Option<FileHealthConfig>,
    #[serde(default)]
    socket: Option<FileSocketConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConnectionConfig {
    listen_addr: Option<std::net::SocketAddr>,
    backend_addr: Option<std::net::SocketAddr>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileCapacityConfig {
    max_clients: Option<usize>,
    max_backends: Option<usize>,
    max_checkout_waiters: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FilePerformanceConfig {
    checkout_timeout_ms: Option<u64>,
    recovery_mode: Option<String>,
    recovery_timeout_ms: Option<u64>,
    backend_reset_query: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileQosConfig {
    max_route_in_flight: Option<usize>,
    max_route_waiters: Option<usize>,
    query_timeout_ms: Option<u64>,
    idle_client_timeout_ms: Option<u64>,
    idle_transaction_timeout_ms: Option<u64>,
    max_client_buffer_bytes: Option<usize>,
    max_backend_buffer_bytes: Option<usize>,
    overload_error_code: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileObservabilityConfig {
    metrics_addr: Option<std::net::SocketAddr>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTlsConfig {
    client_tls_mode: Option<ClientTlsMode>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    client_ca_path: Option<PathBuf>,
    backend_tls_mode: Option<BackendTlsMode>,
    backend_ca_path: Option<PathBuf>,
    backend_server_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileAuthConfig {
    auth_mode: Option<AuthMode>,
    auth_users_file: Option<PathBuf>,
    backend_user: Option<String>,
    backend_password_env_var_name: Option<String>,
    auth_failure_message_mode: Option<AuthFailureMessageMode>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileReloadConfig {
    config_reload_interval_ms: Option<u64>,
    reload_enabled: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileDrainConfig {
    drain_timeout_ms: Option<u64>,
    reject_new_clients_during_drain: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileHealthConfig {
    health_addr: Option<std::net::SocketAddr>,
    readiness_backend_check_interval_ms: Option<u64>,
    readiness_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileSocketConfig {
    tcp_nodelay: Option<bool>,
    tcp_keepalive: Option<bool>,
    tcp_keepalive_idle_ms: Option<u64>,
    tcp_keepalive_interval_ms: Option<u64>,
    tcp_keepalive_retries: Option<u32>,
    tcp_user_timeout_ms: Option<u64>,
    tcp_send_buffer_bytes: Option<usize>,
    tcp_recv_buffer_bytes: Option<usize>,
    strict_socket_option_mode: Option<bool>,
}

pub fn load_effective_config(base: &Config) -> anyhow::Result<Config> {
    let Some(config_file) = base.reload.config_file.as_deref() else {
        return Ok(base.clone());
    };

    let file_config = load_file_config(config_file)?;
    merge_file_config(base, &file_config)
}

pub fn build_reloadable_config(config: &Config) -> anyhow::Result<ReloadableConfig> {
    ReloadableConfig::from_config(config)
}

pub async fn reload_once(
    base: &Config,
    active_config: &Arc<RwLock<Config>>,
    runtime: &Arc<RwLock<ReloadableConfig>>,
) -> anyhow::Result<ReloadDecision> {
    let next_config = load_effective_config(base)?;
    let current_config = active_config.read().await.clone();

    if next_config == current_config {
        return Ok(ReloadDecision::Unchanged);
    }

    if !has_only_safe_changes(&current_config, &next_config) {
        return Ok(ReloadDecision::Rejected);
    }

    let next_runtime = ReloadableConfig::from_config(&next_config)?;
    *active_config.write().await = next_config;
    *runtime.write().await = next_runtime;
    Ok(ReloadDecision::Applied)
}

pub async fn spawn_reload_loop(
    base: Config,
    reload_config: ReloadConfig,
    active_config: Arc<RwLock<Config>>,
    runtime: Arc<RwLock<ReloadableConfig>>,
) {
    if base.reload.config_file.is_none() {
        return;
    }

    let interval_duration = reload_config.config_reload_interval();
    let mut ticker = interval(interval_duration);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        match reload_once(&base, &active_config, &runtime).await {
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

fn load_file_config(path: &Path) -> anyhow::Result<FileConfig> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read config file {}", path.display()))?;
    toml::from_str(&contents).with_context(|| format!("parse config file {}", path.display()))
}

fn merge_file_config(base: &Config, file: &FileConfig) -> anyhow::Result<Config> {
    let mut merged = base.clone();

    if let Some(connection) = file.connection.as_ref() {
        merge_connection_config(&mut merged.connection, connection);
    }
    if let Some(capacity) = file.capacity.as_ref() {
        merge_capacity_config(&mut merged.capacity, capacity);
    }
    if let Some(performance) = file.performance.as_ref() {
        merge_performance_config(&mut merged.performance, performance)?;
    }
    if let Some(qos) = file.qos.as_ref() {
        merge_qos_config(&mut merged.qos, qos);
    }
    if let Some(observability) = file.observability.as_ref() {
        merge_observability_config(&mut merged.observability, observability);
    }
    if let Some(tls_config) = file.tls.as_ref() {
        merge_tls_config(&mut merged.tls, tls_config);
    }
    if let Some(auth_config) = file.auth.as_ref() {
        merge_auth_config(&mut merged.auth, auth_config);
    }
    if let Some(reload) = file.reload.as_ref() {
        merge_reload_config(&mut merged.reload, reload);
    }
    if let Some(drain) = file.drain.as_ref() {
        merge_drain_config(&mut merged.drain, drain);
    }
    if let Some(health) = file.health.as_ref() {
        merge_health_config(&mut merged.health, health);
    }
    if let Some(socket) = file.socket.as_ref() {
        merge_socket_config(&mut merged.socket, socket);
    }

    Ok(merged)
}

fn merge_connection_config(target: &mut ConnectionConfig, file: &FileConnectionConfig) {
    let defaults = ConnectionConfig::default();
    copy_if_default(
        &mut target.listen_addr,
        defaults.listen_addr,
        file.listen_addr,
    );
    copy_if_default(
        &mut target.backend_addr,
        defaults.backend_addr,
        file.backend_addr,
    );
}

fn merge_capacity_config(target: &mut CapacityConfig, file: &FileCapacityConfig) {
    let defaults = CapacityConfig::default();
    copy_if_default(
        &mut target.max_clients,
        defaults.max_clients,
        file.max_clients,
    );
    copy_if_default(
        &mut target.max_backends,
        defaults.max_backends,
        file.max_backends,
    );
    copy_if_default(
        &mut target.max_checkout_waiters,
        defaults.max_checkout_waiters,
        file.max_checkout_waiters,
    );
}

fn merge_performance_config(
    target: &mut PerformanceConfig,
    file: &FilePerformanceConfig,
) -> anyhow::Result<()> {
    let defaults = PerformanceConfig::default();
    copy_if_default(
        &mut target.checkout_timeout_ms,
        defaults.checkout_timeout_ms,
        file.checkout_timeout_ms,
    );
    copy_if_default(
        &mut target.recovery_timeout_ms,
        defaults.recovery_timeout_ms,
        file.recovery_timeout_ms,
    );
    copy_if_default(
        &mut target.backend_reset_query,
        defaults.backend_reset_query.clone(),
        file.backend_reset_query.clone(),
    );

    if target.recovery_mode == defaults.recovery_mode {
        if let Some(mode) = file.recovery_mode.as_deref() {
            target.recovery_mode = parse_recovery_mode(mode)?;
        }
    }

    Ok(())
}

fn merge_qos_config(target: &mut QosConfig, file: &FileQosConfig) {
    let defaults = QosConfig::default();
    copy_if_default(
        &mut target.max_route_in_flight,
        defaults.max_route_in_flight,
        file.max_route_in_flight,
    );
    copy_if_default(
        &mut target.max_route_waiters,
        defaults.max_route_waiters,
        file.max_route_waiters,
    );
    copy_if_default(
        &mut target.query_timeout_ms,
        defaults.query_timeout_ms,
        file.query_timeout_ms,
    );
    copy_if_default(
        &mut target.idle_client_timeout_ms,
        defaults.idle_client_timeout_ms,
        file.idle_client_timeout_ms,
    );
    copy_if_default(
        &mut target.idle_transaction_timeout_ms,
        defaults.idle_transaction_timeout_ms,
        file.idle_transaction_timeout_ms,
    );
    copy_if_default(
        &mut target.max_client_buffer_bytes,
        defaults.max_client_buffer_bytes,
        file.max_client_buffer_bytes,
    );
    copy_if_default(
        &mut target.max_backend_buffer_bytes,
        defaults.max_backend_buffer_bytes,
        file.max_backend_buffer_bytes,
    );
    copy_if_default(
        &mut target.overload_error_code,
        defaults.overload_error_code,
        file.overload_error_code.clone(),
    );
}

fn merge_observability_config(target: &mut ObservabilityConfig, file: &FileObservabilityConfig) {
    let defaults = ObservabilityConfig::default();
    copy_if_default(
        &mut target.metrics_addr,
        defaults.metrics_addr,
        Some(file.metrics_addr),
    );
}

fn merge_tls_config(target: &mut TlsConfig, file: &FileTlsConfig) {
    let defaults = TlsConfig::default();
    copy_if_default(
        &mut target.client_tls_mode,
        defaults.client_tls_mode,
        file.client_tls_mode,
    );
    copy_if_default(
        &mut target.client_cert_path,
        defaults.client_cert_path,
        Some(file.client_cert_path.clone()),
    );
    copy_if_default(
        &mut target.client_key_path,
        defaults.client_key_path,
        Some(file.client_key_path.clone()),
    );
    copy_if_default(
        &mut target.client_ca_path,
        defaults.client_ca_path,
        Some(file.client_ca_path.clone()),
    );
    copy_if_default(
        &mut target.backend_tls_mode,
        defaults.backend_tls_mode,
        file.backend_tls_mode,
    );
    copy_if_default(
        &mut target.backend_ca_path,
        defaults.backend_ca_path,
        Some(file.backend_ca_path.clone()),
    );
    copy_if_default(
        &mut target.backend_server_name,
        defaults.backend_server_name,
        Some(file.backend_server_name.clone()),
    );
}

fn merge_auth_config(target: &mut AuthConfig, file: &FileAuthConfig) {
    let defaults = AuthConfig::default();
    copy_if_default(&mut target.auth_mode, defaults.auth_mode, file.auth_mode);
    copy_if_default(
        &mut target.auth_users_file,
        defaults.auth_users_file,
        Some(file.auth_users_file.clone()),
    );
    copy_if_default(
        &mut target.backend_user,
        defaults.backend_user,
        Some(file.backend_user.clone()),
    );
    copy_if_default(
        &mut target.backend_password_env_var_name,
        defaults.backend_password_env_var_name,
        Some(file.backend_password_env_var_name.clone()),
    );
    copy_if_default(
        &mut target.auth_failure_message_mode,
        defaults.auth_failure_message_mode,
        file.auth_failure_message_mode,
    );
}

fn merge_reload_config(target: &mut ReloadConfig, file: &FileReloadConfig) {
    let defaults = ReloadConfig::default();
    copy_if_default(
        &mut target.config_reload_interval_ms,
        defaults.config_reload_interval_ms,
        file.config_reload_interval_ms,
    );
    copy_if_default(
        &mut target.reload_enabled,
        defaults.reload_enabled,
        file.reload_enabled,
    );
}

fn merge_drain_config(target: &mut DrainConfig, file: &FileDrainConfig) {
    let defaults = DrainConfig::default();
    copy_if_default(
        &mut target.drain_timeout_ms,
        defaults.drain_timeout_ms,
        file.drain_timeout_ms,
    );
    copy_if_default(
        &mut target.reject_new_clients_during_drain,
        defaults.reject_new_clients_during_drain,
        file.reject_new_clients_during_drain,
    );
}

fn merge_health_config(target: &mut HealthConfig, file: &FileHealthConfig) {
    let defaults = HealthConfig::default();
    copy_if_default(
        &mut target.health_addr,
        defaults.health_addr,
        Some(file.health_addr),
    );
    copy_if_default(
        &mut target.readiness_backend_check_interval_ms,
        defaults.readiness_backend_check_interval_ms,
        file.readiness_backend_check_interval_ms,
    );
    copy_if_default(
        &mut target.readiness_timeout_ms,
        defaults.readiness_timeout_ms,
        file.readiness_timeout_ms,
    );
}

fn merge_socket_config(target: &mut SocketConfig, file: &FileSocketConfig) {
    let defaults = SocketConfig::default();
    copy_if_default(
        &mut target.tcp_nodelay,
        defaults.tcp_nodelay,
        file.tcp_nodelay,
    );
    copy_if_default(
        &mut target.tcp_keepalive,
        defaults.tcp_keepalive,
        file.tcp_keepalive,
    );
    copy_if_default(
        &mut target.tcp_keepalive_idle_ms,
        defaults.tcp_keepalive_idle_ms,
        Some(file.tcp_keepalive_idle_ms),
    );
    copy_if_default(
        &mut target.tcp_keepalive_interval_ms,
        defaults.tcp_keepalive_interval_ms,
        Some(file.tcp_keepalive_interval_ms),
    );
    copy_if_default(
        &mut target.tcp_keepalive_retries,
        defaults.tcp_keepalive_retries,
        Some(file.tcp_keepalive_retries),
    );
    copy_if_default(
        &mut target.tcp_user_timeout_ms,
        defaults.tcp_user_timeout_ms,
        Some(file.tcp_user_timeout_ms),
    );
    copy_if_default(
        &mut target.tcp_send_buffer_bytes,
        defaults.tcp_send_buffer_bytes,
        Some(file.tcp_send_buffer_bytes),
    );
    copy_if_default(
        &mut target.tcp_recv_buffer_bytes,
        defaults.tcp_recv_buffer_bytes,
        Some(file.tcp_recv_buffer_bytes),
    );
    copy_if_default(
        &mut target.strict_socket_option_mode,
        defaults.strict_socket_option_mode,
        file.strict_socket_option_mode,
    );
}

fn copy_if_default<T: PartialEq>(target: &mut T, default: T, value: Option<T>) {
    if *target == default {
        if let Some(value) = value {
            *target = value;
        }
    }
}

fn parse_recovery_mode(value: &str) -> anyhow::Result<RecoveryMode> {
    match value {
        "recover" => Ok(RecoveryMode::Recover),
        "rollback_only" => Ok(RecoveryMode::RollbackOnly),
        "drop" => Ok(RecoveryMode::Drop),
        other => bail!("invalid recovery mode '{other}'"),
    }
}

fn has_only_safe_changes(current: &Config, next: &Config) -> bool {
    current.connection == next.connection
        && current.capacity == next.capacity
        && current.observability == next.observability
        && current.tls.client_tls_mode == next.tls.client_tls_mode
        && current.tls.backend_tls_mode == next.tls.backend_tls_mode
        && current.tls.backend_ca_path == next.tls.backend_ca_path
        && current.tls.backend_server_name == next.tls.backend_server_name
        && current.auth.auth_mode == next.auth.auth_mode
        && current.auth.backend_user == next.auth.backend_user
        && current.auth.backend_password_env_var_name == next.auth.backend_password_env_var_name
        && current.auth.auth_failure_message_mode == next.auth.auth_failure_message_mode
        && current.reload == next.reload
        && current.drain == next.drain
        && current.health == next.health
}
