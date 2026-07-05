use std::{net::SocketAddr, time::Duration};

use clap::{Args, Parser};

use pg_kinetic_core::recovery::RecoveryMode;

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
    pub observability: ObservabilityConfig,
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
pub struct ObservabilityConfig {
    #[arg(long, env = "PG_KINETIC_METRICS_ADDR")]
    pub metrics_addr: Option<SocketAddr>,
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

#[cfg(test)]
mod tests {
    use super::Config;
    use clap::Parser;
    use std::{net::SocketAddr, time::Duration};

    #[test]
    fn parses_defaults() {
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
        assert_eq!(config.observability.metrics_addr, None);
    }

    #[test]
    fn parses_explicit_flags() {
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
    }

    #[test]
    fn parses_pool_and_metrics_flags() {
        let config = Config::try_parse_from([
            "pg-kinetic",
            "--max-backends",
            "8",
            "--max-checkout-waiters",
            "16",
            "--backend-reset-query",
            "DISCARD ALL",
            "--metrics-addr",
            "127.0.0.1:9099",
        ])
        .expect("flags parse");

        assert_eq!(config.capacity.max_backends, 8);
        assert_eq!(config.capacity.max_checkout_waiters, 16);
        assert_eq!(config.performance.backend_reset_query, "DISCARD ALL");
        assert_eq!(
            config.observability.metrics_addr,
            Some("127.0.0.1:9099".parse().expect("valid socket"))
        );
    }

    #[test]
    fn disables_metrics_by_default() {
        let config = Config::try_parse_from(["pg-kinetic"]).expect("defaults parse");

        assert_eq!(config.observability.metrics_addr, None);
    }
}
