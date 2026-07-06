use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Args, Parser, ValueEnum};

use pg_kinetic_core::{
    constants::{BufferDefaults, QosDefaults, TimeoutDefaults},
    recovery::RecoveryMode,
    security::{
        AuthMode as CoreAuthMode, BackendTlsMode as CoreBackendTlsMode,
        ClientTlsMode as CoreClientTlsMode,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum ClientTlsMode {
    Disable,
    Allow,
    Require,
    VerifyClient,
}

impl ClientTlsMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Allow => "allow",
            Self::Require => "require",
            Self::VerifyClient => "verify_client",
        }
    }
}

impl From<ClientTlsMode> for CoreClientTlsMode {
    fn from(mode: ClientTlsMode) -> Self {
        match mode {
            ClientTlsMode::Disable => Self::Disable,
            ClientTlsMode::Allow => Self::Allow,
            ClientTlsMode::Require => Self::Require,
            ClientTlsMode::VerifyClient => Self::VerifyClient,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum BackendTlsMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl BackendTlsMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify_ca",
            Self::VerifyFull => "verify_full",
        }
    }
}

impl From<BackendTlsMode> for CoreBackendTlsMode {
    fn from(mode: BackendTlsMode) -> Self {
        match mode {
            BackendTlsMode::Disable => Self::Disable,
            BackendTlsMode::Prefer => Self::Prefer,
            BackendTlsMode::Require => Self::Require,
            BackendTlsMode::VerifyCa => Self::VerifyCa,
            BackendTlsMode::VerifyFull => Self::VerifyFull,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AuthMode {
    PassThrough,
    Trust,
    #[value(name = "scram_sha_256", alias = "scram_sha256")]
    ScramSha256,
}

impl AuthMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PassThrough => "pass_through",
            Self::Trust => "trust",
            Self::ScramSha256 => "scram_sha_256",
        }
    }
}

impl From<AuthMode> for CoreAuthMode {
    fn from(mode: AuthMode) -> Self {
        match mode {
            AuthMode::PassThrough => Self::PassThrough,
            AuthMode::Trust => Self::Trust,
            AuthMode::ScramSha256 => Self::ScramSha256,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AuthFailureMessageMode {
    Generic,
    Detailed,
}

impl AuthFailureMessageMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Detailed => "detailed",
        }
    }
}

#[derive(Clone, Debug, Parser)]
#[command(name = "pg-kinetic")]
#[command(about = "Low-overhead PostgreSQL wire proxy")]
pub struct Config {
    #[command(flatten)]
    pub connection: ConnectionConfig,

    #[command(flatten)]
    pub capacity: CapacityConfig,

    #[command(flatten)]
    pub performance: PerformanceConfig,

    #[command(flatten)]
    pub qos: QosConfig,

    #[command(flatten)]
    pub observability: ObservabilityConfig,

    #[command(flatten)]
    pub tls: TlsConfig,

    #[command(flatten)]
    pub auth: AuthConfig,

    #[command(flatten)]
    pub reload: ReloadConfig,

    #[command(flatten)]
    pub drain: DrainConfig,

    #[command(flatten)]
    pub health: HealthConfig,

    #[command(flatten)]
    pub socket: SocketConfig,
}

#[derive(Clone, Debug, Args)]
pub struct ConnectionConfig {
    #[arg(long, env = "PG_KINETIC_LISTEN_ADDR", default_value = "127.0.0.1:6543")]
    pub listen_addr: SocketAddr,

    #[arg(
        long,
        env = "PG_KINETIC_BACKEND_ADDR",
        default_value = "127.0.0.1:5432"
    )]
    pub backend_addr: SocketAddr,
}

#[derive(Clone, Debug, Args)]
pub struct CapacityConfig {
    #[arg(long, env = "PG_KINETIC_MAX_CLIENTS", default_value_t = 10_000)]
    pub max_clients: usize,

    #[arg(long, env = "PG_KINETIC_MAX_BACKENDS", default_value_t = 100)]
    pub max_backends: usize,

    #[arg(long, env = "PG_KINETIC_MAX_CHECKOUT_WAITERS", default_value_t = 1_000)]
    pub max_checkout_waiters: usize,
}

#[derive(Clone, Debug, Args)]
pub struct PerformanceConfig {
    #[arg(long, env = "PG_KINETIC_CHECKOUT_TIMEOUT_MS", default_value_t = 1_000)]
    pub checkout_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_RECOVERY_MODE",
        value_enum,
        default_value_t = RecoveryMode::Recover
    )]
    pub recovery_mode: RecoveryMode,

    #[arg(long, env = "PG_KINETIC_RECOVERY_TIMEOUT_MS", default_value_t = 5_000)]
    pub recovery_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_BACKEND_RESET_QUERY",
        default_value = "DISCARD ALL"
    )]
    pub backend_reset_query: String,
}

#[derive(Clone, Debug, Args)]
pub struct QosConfig {
    #[arg(
        long,
        env = "PG_KINETIC_MAX_ROUTE_IN_FLIGHT",
        default_value_t = QosDefaults::MAX_ROUTE_IN_FLIGHT
    )]
    pub max_route_in_flight: usize,

    #[arg(
        long,
        env = "PG_KINETIC_MAX_ROUTE_WAITERS",
        default_value_t = QosDefaults::MAX_ROUTE_WAITERS
    )]
    pub max_route_waiters: usize,

    #[arg(
        long,
        env = "PG_KINETIC_QUERY_TIMEOUT_MS",
        default_value_t = TimeoutDefaults::QUERY_TIMEOUT_MS
    )]
    pub query_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_IDLE_CLIENT_TIMEOUT_MS",
        default_value_t = TimeoutDefaults::IDLE_CLIENT_TIMEOUT_MS
    )]
    pub idle_client_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_IDLE_TRANSACTION_TIMEOUT_MS",
        default_value_t = TimeoutDefaults::IDLE_TRANSACTION_TIMEOUT_MS
    )]
    pub idle_transaction_timeout_ms: u64,

    #[arg(
        long,
        env = "PG_KINETIC_MAX_CLIENT_BUFFER_BYTES",
        default_value_t = BufferDefaults::MAX_CLIENT_BUFFER_BYTES
    )]
    pub max_client_buffer_bytes: usize,

    #[arg(
        long,
        env = "PG_KINETIC_MAX_BACKEND_BUFFER_BYTES",
        default_value_t = BufferDefaults::MAX_BACKEND_BUFFER_BYTES
    )]
    pub max_backend_buffer_bytes: usize,

    #[arg(long, env = "PG_KINETIC_OVERLOAD_ERROR_CODE", default_value = "53300")]
    pub overload_error_code: String,
}

#[derive(Clone, Debug, Args)]
pub struct ObservabilityConfig {
    #[arg(long, env = "PG_KINETIC_METRICS_ADDR")]
    pub metrics_addr: Option<SocketAddr>,
}

#[derive(Clone, Debug, Args)]
pub struct TlsConfig {
    #[arg(
        long,
        env = "PG_KINETIC_CLIENT_TLS_MODE",
        value_enum,
        default_value_t = ClientTlsMode::Disable
    )]
    pub client_tls_mode: ClientTlsMode,

    #[arg(long, env = "PG_KINETIC_CLIENT_TLS_CERT_PATH")]
    pub client_cert_path: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_CLIENT_TLS_KEY_PATH")]
    pub client_key_path: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_CLIENT_TLS_CA_PATH")]
    pub client_ca_path: Option<PathBuf>,

    #[arg(
        long,
        env = "PG_KINETIC_BACKEND_TLS_MODE",
        value_enum,
        default_value_t = BackendTlsMode::Disable
    )]
    pub backend_tls_mode: BackendTlsMode,

    #[arg(long, env = "PG_KINETIC_BACKEND_TLS_CA_PATH")]
    pub backend_ca_path: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_BACKEND_TLS_SERVER_NAME")]
    pub backend_server_name: Option<String>,
}

impl TlsConfig {
    #[must_use]
    pub fn client_tls_mode_core(&self) -> CoreClientTlsMode {
        self.client_tls_mode.into()
    }

    #[must_use]
    pub fn backend_tls_mode_core(&self) -> CoreBackendTlsMode {
        self.backend_tls_mode.into()
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            client_tls_mode: ClientTlsMode::Disable,
            client_cert_path: None,
            client_key_path: None,
            client_ca_path: None,
            backend_tls_mode: BackendTlsMode::Disable,
            backend_ca_path: None,
            backend_server_name: None,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct AuthConfig {
    #[arg(
        long,
        env = "PG_KINETIC_AUTH_MODE",
        value_enum,
        default_value_t = AuthMode::PassThrough
    )]
    pub auth_mode: AuthMode,

    #[arg(long, env = "PG_KINETIC_AUTH_USERS_FILE")]
    pub auth_users_file: Option<PathBuf>,

    #[arg(long, env = "PG_KINETIC_BACKEND_USER")]
    pub backend_user: Option<String>,

    #[arg(long, env = "PG_KINETIC_BACKEND_PASSWORD_ENV_VAR_NAME")]
    pub backend_password_env_var_name: Option<String>,

    #[arg(
        long,
        env = "PG_KINETIC_AUTH_FAILURE_MESSAGE_MODE",
        value_enum,
        default_value_t = AuthFailureMessageMode::Generic
    )]
    pub auth_failure_message_mode: AuthFailureMessageMode,
}

impl AuthConfig {
    #[must_use]
    pub fn auth_mode_core(&self) -> CoreAuthMode {
        self.auth_mode.into()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            auth_mode: AuthMode::PassThrough,
            auth_users_file: None,
            backend_user: None,
            backend_password_env_var_name: None,
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct ReloadConfig {
    #[arg(long, env = "PG_KINETIC_CONFIG_FILE")]
    pub config_file: Option<PathBuf>,

    #[arg(
        long,
        env = "PG_KINETIC_CONFIG_RELOAD_INTERVAL_MS",
        default_value_t = 5_000
    )]
    pub config_reload_interval_ms: u64,

    #[arg(long, env = "PG_KINETIC_CONFIG_RELOAD_ENABLED")]
    pub reload_enabled: bool,
}

impl ReloadConfig {
    #[must_use]
    pub const fn config_reload_interval(&self) -> Duration {
        Duration::from_millis(self.config_reload_interval_ms)
    }
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            config_file: None,
            config_reload_interval_ms: 5_000,
            reload_enabled: false,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct DrainConfig {
    #[arg(long, env = "PG_KINETIC_DRAIN_TIMEOUT_MS", default_value_t = 30_000)]
    pub drain_timeout_ms: u64,

    #[arg(long, env = "PG_KINETIC_REJECT_NEW_CLIENTS_DURING_DRAIN")]
    pub reject_new_clients_during_drain: bool,
}

impl DrainConfig {
    #[must_use]
    pub const fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.drain_timeout_ms)
    }
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            drain_timeout_ms: 30_000,
            reject_new_clients_during_drain: false,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct HealthConfig {
    #[arg(long, env = "PG_KINETIC_HEALTH_ADDR")]
    pub health_addr: Option<SocketAddr>,

    #[arg(
        long,
        env = "PG_KINETIC_READINESS_BACKEND_CHECK_INTERVAL_MS",
        default_value_t = 1_000
    )]
    pub readiness_backend_check_interval_ms: u64,

    #[arg(long, env = "PG_KINETIC_READINESS_TIMEOUT_MS", default_value_t = 5_000)]
    pub readiness_timeout_ms: u64,
}

impl HealthConfig {
    #[must_use]
    pub const fn readiness_backend_check_interval(&self) -> Duration {
        Duration::from_millis(self.readiness_backend_check_interval_ms)
    }

    #[must_use]
    pub const fn readiness_timeout(&self) -> Duration {
        Duration::from_millis(self.readiness_timeout_ms)
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            health_addr: None,
            readiness_backend_check_interval_ms: 1_000,
            readiness_timeout_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct SocketConfig {
    #[arg(long, env = "PG_KINETIC_TCP_NODELAY", default_value_t = true)]
    pub tcp_nodelay: bool,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE")]
    pub tcp_keepalive: bool,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE_IDLE_MS")]
    pub tcp_keepalive_idle_ms: Option<u64>,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE_INTERVAL_MS")]
    pub tcp_keepalive_interval_ms: Option<u64>,

    #[arg(long, env = "PG_KINETIC_TCP_KEEPALIVE_RETRIES")]
    pub tcp_keepalive_retries: Option<u32>,

    #[arg(long, env = "PG_KINETIC_TCP_USER_TIMEOUT_MS")]
    pub tcp_user_timeout_ms: Option<u64>,

    #[arg(long, env = "PG_KINETIC_TCP_SEND_BUFFER_BYTES")]
    pub tcp_send_buffer_bytes: Option<usize>,

    #[arg(long, env = "PG_KINETIC_TCP_RECV_BUFFER_BYTES")]
    pub tcp_recv_buffer_bytes: Option<usize>,

    #[arg(long, env = "PG_KINETIC_STRICT_SOCKET_OPTION_MODE")]
    pub strict_socket_option_mode: bool,
}

impl SocketConfig {
    #[must_use]
    pub fn tcp_keepalive_idle(&self) -> Option<Duration> {
        self.tcp_keepalive_idle_ms.map(Duration::from_millis)
    }

    #[must_use]
    pub fn tcp_keepalive_interval(&self) -> Option<Duration> {
        self.tcp_keepalive_interval_ms.map(Duration::from_millis)
    }

    #[must_use]
    pub fn tcp_user_timeout(&self) -> Option<Duration> {
        self.tcp_user_timeout_ms.map(Duration::from_millis)
    }
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            tcp_keepalive: false,
            tcp_keepalive_idle_ms: None,
            tcp_keepalive_interval_ms: None,
            tcp_keepalive_retries: None,
            tcp_user_timeout_ms: None,
            tcp_send_buffer_bytes: None,
            tcp_recv_buffer_bytes: None,
            strict_socket_option_mode: false,
        }
    }
}

impl Config {
    #[must_use]
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

impl PerformanceConfig {
    #[must_use]
    pub const fn checkout_timeout(&self) -> Duration {
        Duration::from_millis(self.checkout_timeout_ms)
    }

    #[must_use]
    pub const fn recovery_timeout(&self) -> Duration {
        Duration::from_millis(self.recovery_timeout_ms)
    }
}

impl QosConfig {
    #[must_use]
    pub const fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout_ms)
    }

    #[must_use]
    pub const fn idle_client_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_client_timeout_ms)
    }

    #[must_use]
    pub const fn idle_transaction_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_transaction_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthFailureMessageMode, AuthMode, BackendTlsMode, ClientTlsMode, Config, SocketConfig,
    };
    use clap::Parser;
    use std::{net::SocketAddr, path::PathBuf, time::Duration};

    #[test]
    fn config_parses_defaults() {
        let config = Config::try_parse_from(["pg-kinetic"]).expect("defaults parse");

        assert_eq!(
            config.connection.listen_addr,
            "127.0.0.1:6543"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(
            config.connection.backend_addr,
            "127.0.0.1:5432"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(config.capacity.max_clients, 10_000);
        assert_eq!(config.capacity.max_backends, 100);
        assert_eq!(config.capacity.max_checkout_waiters, 1_000);
        assert_eq!(
            config.performance.checkout_timeout(),
            Duration::from_secs(1)
        );
        assert_eq!(
            config.performance.recovery_mode,
            pg_kinetic_core::recovery::RecoveryMode::Recover
        );
        assert_eq!(
            config.performance.recovery_timeout(),
            Duration::from_secs(5)
        );
        assert_eq!(config.performance.backend_reset_query, "DISCARD ALL");
        assert_eq!(config.qos.max_route_in_flight, 100);
        assert_eq!(config.qos.max_route_waiters, 1_000);
        assert_eq!(config.qos.query_timeout(), Duration::from_secs(30));
        assert_eq!(config.qos.idle_client_timeout(), Duration::from_secs(300));
        assert_eq!(
            config.qos.idle_transaction_timeout(),
            Duration::from_secs(60)
        );
        assert_eq!(config.qos.max_client_buffer_bytes, 1_048_576);
        assert_eq!(config.qos.max_backend_buffer_bytes, 4_194_304);
        assert_eq!(config.qos.overload_error_code, "53300");
        assert_eq!(config.observability.metrics_addr, None);

        assert_eq!(config.tls.client_tls_mode, ClientTlsMode::Disable);
        assert_eq!(config.tls.client_cert_path, None);
        assert_eq!(config.tls.client_key_path, None);
        assert_eq!(config.tls.client_ca_path, None);
        assert_eq!(config.tls.backend_tls_mode, BackendTlsMode::Disable);
        assert_eq!(config.tls.backend_ca_path, None);
        assert_eq!(config.tls.backend_server_name, None);

        assert_eq!(config.auth.auth_mode, AuthMode::PassThrough);
        assert_eq!(config.auth.auth_users_file, None);
        assert_eq!(config.auth.backend_user, None);
        assert_eq!(config.auth.backend_password_env_var_name, None);
        assert_eq!(
            config.auth.auth_failure_message_mode,
            AuthFailureMessageMode::Generic
        );

        assert_eq!(config.reload.config_file, None);
        assert_eq!(config.reload.config_reload_interval_ms, 5_000);
        assert!(!config.reload.reload_enabled);

        assert_eq!(config.drain.drain_timeout_ms, 30_000);
        assert!(!config.drain.reject_new_clients_during_drain);

        assert_eq!(config.health.health_addr, None);
        assert_eq!(config.health.readiness_backend_check_interval_ms, 1_000);
        assert_eq!(config.health.readiness_timeout_ms, 5_000);

        assert!(config.socket.tcp_nodelay);
        assert!(!config.socket.tcp_keepalive);
        assert_eq!(config.socket.tcp_keepalive_idle_ms, None);
        assert_eq!(config.socket.tcp_keepalive_interval_ms, None);
        assert_eq!(config.socket.tcp_keepalive_retries, None);
        assert_eq!(config.socket.tcp_user_timeout_ms, None);
        assert_eq!(config.socket.tcp_send_buffer_bytes, None);
        assert_eq!(config.socket.tcp_recv_buffer_bytes, None);
        assert!(!config.socket.strict_socket_option_mode);
    }

    #[test]
    fn config_parses_explicit_flags() {
        let config = Config::try_parse_from([
            "pg-kinetic",
            "--listen-addr",
            "0.0.0.0:6432",
            "--backend-addr",
            "127.0.0.1:5433",
            "--max-clients",
            "500",
            "--max-backends",
            "25",
            "--max-checkout-waiters",
            "12",
            "--checkout-timeout-ms",
            "250",
            "--recovery-mode",
            "drop",
            "--recovery-timeout-ms",
            "7500",
            "--backend-reset-query",
            "DISCARD TEMP",
            "--max-route-in-flight",
            "7",
            "--max-route-waiters",
            "9",
            "--query-timeout-ms",
            "1234",
            "--idle-client-timeout-ms",
            "5678",
            "--idle-transaction-timeout-ms",
            "9012",
            "--max-client-buffer-bytes",
            "111",
            "--max-backend-buffer-bytes",
            "222",
            "--overload-error-code",
            "53301",
            "--client-tls-mode",
            "verify_client",
            "--client-cert-path",
            "client-cert.pem",
            "--client-key-path",
            "client-key.pem",
            "--client-ca-path",
            "client-ca.pem",
            "--backend-tls-mode",
            "verify_full",
            "--backend-ca-path",
            "backend-ca.pem",
            "--backend-server-name",
            "db.example.com",
            "--auth-mode",
            "scram_sha_256",
            "--auth-users-file",
            "auth-users.toml",
            "--backend-user",
            "proxy_user",
            "--backend-password-env-var-name",
            "PG_KINETIC_BACKEND_PASSWORD",
            "--auth-failure-message-mode",
            "detailed",
            "--config-file",
            "pg-kinetic.toml",
            "--config-reload-interval-ms",
            "7500",
            "--reload-enabled",
            "--drain-timeout-ms",
            "45000",
            "--reject-new-clients-during-drain",
            "--health-addr",
            "127.0.0.1:9091",
            "--readiness-backend-check-interval-ms",
            "333",
            "--readiness-timeout-ms",
            "4444",
            "--tcp-keepalive",
            "--tcp-keepalive-idle-ms",
            "1111",
            "--tcp-keepalive-interval-ms",
            "2222",
            "--tcp-keepalive-retries",
            "3",
            "--tcp-user-timeout-ms",
            "3333",
            "--tcp-send-buffer-bytes",
            "4444",
            "--tcp-recv-buffer-bytes",
            "5555",
            "--strict-socket-option-mode",
        ])
        .expect("flags parse");

        assert_eq!(
            config.connection.listen_addr,
            "0.0.0.0:6432".parse::<SocketAddr>().expect("valid socket")
        );
        assert_eq!(
            config.connection.backend_addr,
            "127.0.0.1:5433"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(config.capacity.max_clients, 500);
        assert_eq!(config.capacity.max_backends, 25);
        assert_eq!(config.capacity.max_checkout_waiters, 12);
        assert_eq!(
            config.performance.checkout_timeout(),
            Duration::from_millis(250)
        );
        assert_eq!(
            config.performance.recovery_mode,
            pg_kinetic_core::recovery::RecoveryMode::Drop
        );
        assert_eq!(
            config.performance.recovery_timeout(),
            Duration::from_millis(7_500)
        );
        assert_eq!(config.performance.backend_reset_query, "DISCARD TEMP");
        assert_eq!(config.qos.max_route_in_flight, 7);
        assert_eq!(config.qos.max_route_waiters, 9);
        assert_eq!(config.qos.query_timeout(), Duration::from_millis(1_234));
        assert_eq!(
            config.qos.idle_client_timeout(),
            Duration::from_millis(5_678)
        );
        assert_eq!(
            config.qos.idle_transaction_timeout(),
            Duration::from_millis(9_012)
        );
        assert_eq!(config.qos.max_client_buffer_bytes, 111);
        assert_eq!(config.qos.max_backend_buffer_bytes, 222);
        assert_eq!(config.qos.overload_error_code, "53301");

        assert_eq!(config.tls.client_tls_mode, ClientTlsMode::VerifyClient);
        assert_eq!(
            config.tls.client_cert_path,
            Some(PathBuf::from("client-cert.pem"))
        );
        assert_eq!(
            config.tls.client_key_path,
            Some(PathBuf::from("client-key.pem"))
        );
        assert_eq!(
            config.tls.client_ca_path,
            Some(PathBuf::from("client-ca.pem"))
        );
        assert_eq!(config.tls.backend_tls_mode, BackendTlsMode::VerifyFull);
        assert_eq!(
            config.tls.backend_ca_path,
            Some(PathBuf::from("backend-ca.pem"))
        );
        assert_eq!(
            config.tls.backend_server_name,
            Some(String::from("db.example.com"))
        );

        assert_eq!(config.auth.auth_mode, AuthMode::ScramSha256);
        assert_eq!(
            config.auth.auth_users_file,
            Some(PathBuf::from("auth-users.toml"))
        );
        assert_eq!(config.auth.backend_user, Some(String::from("proxy_user")));
        assert_eq!(
            config.auth.backend_password_env_var_name,
            Some(String::from("PG_KINETIC_BACKEND_PASSWORD"))
        );
        assert_eq!(
            config.auth.auth_failure_message_mode,
            AuthFailureMessageMode::Detailed
        );

        assert_eq!(
            config.reload.config_file,
            Some(PathBuf::from("pg-kinetic.toml"))
        );
        assert_eq!(config.reload.config_reload_interval_ms, 7_500);
        assert!(config.reload.reload_enabled);

        assert_eq!(config.drain.drain_timeout_ms, 45_000);
        assert!(config.drain.reject_new_clients_during_drain);

        assert_eq!(
            config.health.health_addr,
            Some("127.0.0.1:9091".parse().expect("valid socket"))
        );
        assert_eq!(config.health.readiness_backend_check_interval_ms, 333);
        assert_eq!(config.health.readiness_timeout_ms, 4_444);

        assert!(config.socket.tcp_nodelay);
        assert!(config.socket.tcp_keepalive);
        assert_eq!(config.socket.tcp_keepalive_idle_ms, Some(1_111));
        assert_eq!(config.socket.tcp_keepalive_interval_ms, Some(2_222));
        assert_eq!(config.socket.tcp_keepalive_retries, Some(3));
        assert_eq!(config.socket.tcp_user_timeout_ms, Some(3_333));
        assert_eq!(config.socket.tcp_send_buffer_bytes, Some(4_444));
        assert_eq!(config.socket.tcp_recv_buffer_bytes, Some(5_555));
        assert!(config.socket.strict_socket_option_mode);
    }

    #[test]
    fn config_converts_named_modes() {
        let config = Config::try_parse_from([
            "pg-kinetic",
            "--client-tls-mode",
            "require",
            "--backend-tls-mode",
            "prefer",
            "--auth-mode",
            "trust",
        ])
        .expect("flags parse");

        assert_eq!(
            config.tls.client_tls_mode_core(),
            pg_kinetic_core::security::ClientTlsMode::Require
        );
        assert_eq!(
            config.tls.backend_tls_mode_core(),
            pg_kinetic_core::security::BackendTlsMode::Prefer
        );
        assert_eq!(
            config.auth.auth_mode_core(),
            pg_kinetic_core::security::AuthMode::Trust
        );
    }

    #[test]
    fn socket_config_helpers_convert_millis() {
        let socket = SocketConfig {
            tcp_nodelay: true,
            tcp_keepalive: true,
            tcp_keepalive_idle_ms: Some(1_500),
            tcp_keepalive_interval_ms: Some(2_500),
            tcp_keepalive_retries: Some(4),
            tcp_user_timeout_ms: Some(3_500),
            tcp_send_buffer_bytes: Some(4_500),
            tcp_recv_buffer_bytes: Some(5_500),
            strict_socket_option_mode: false,
        };

        assert_eq!(
            socket.tcp_keepalive_idle(),
            Some(Duration::from_millis(1_500))
        );
        assert_eq!(
            socket.tcp_keepalive_interval(),
            Some(Duration::from_millis(2_500))
        );
        assert_eq!(
            socket.tcp_user_timeout(),
            Some(Duration::from_millis(3_500))
        );
    }
}
