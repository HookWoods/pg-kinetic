use std::{net::SocketAddr, time::Duration};

use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(name = "pg-kinetic")]
#[command(about = "Low-overhead PostgreSQL wire proxy")]
pub struct Config {
    #[arg(long, env = "PG_KINETIC_LISTEN_ADDR", default_value = "127.0.0.1:6543")]
    pub listen_addr: SocketAddr,

    #[arg(
        long,
        env = "PG_KINETIC_BACKEND_ADDR",
        default_value = "127.0.0.1:5432"
    )]
    pub backend_addr: SocketAddr,

    #[arg(long, env = "PG_KINETIC_MAX_CLIENTS", default_value_t = 10_000)]
    pub max_clients: usize,

    #[arg(long, env = "PG_KINETIC_MAX_BACKENDS", default_value_t = 100)]
    pub max_backends: usize,

    #[arg(long, env = "PG_KINETIC_CHECKOUT_TIMEOUT_MS", default_value_t = 1_000)]
    pub checkout_timeout_ms: u64,
}

impl Config {
    #[must_use]
    pub fn parse_args() -> Self {
        Self::parse()
    }

    #[must_use]
    pub const fn checkout_timeout(&self) -> Duration {
        Duration::from_millis(self.checkout_timeout_ms)
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
            config.listen_addr,
            "127.0.0.1:6543"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(
            config.backend_addr,
            "127.0.0.1:5432"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(config.max_clients, 10_000);
        assert_eq!(config.max_backends, 100);
        assert_eq!(config.checkout_timeout(), Duration::from_secs(1));
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
            "--checkout-timeout-ms",
            "250",
        ])
        .expect("flags parse");

        assert_eq!(
            config.listen_addr,
            "0.0.0.0:6432".parse::<SocketAddr>().expect("valid socket")
        );
        assert_eq!(
            config.backend_addr,
            "127.0.0.1:5433"
                .parse::<SocketAddr>()
                .expect("valid socket")
        );
        assert_eq!(config.max_clients, 500);
        assert_eq!(config.max_backends, 25);
        assert_eq!(config.checkout_timeout(), Duration::from_millis(250));
    }
}
