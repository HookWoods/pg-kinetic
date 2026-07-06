use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;

use crate::config::SocketConfig;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SocketOptionOutcome {
    #[default]
    Applied,
    Unsupported,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SocketOptions {
    pub tcp_nodelay: bool,
    pub tcp_keepalive: bool,
    pub tcp_keepalive_idle: Option<Duration>,
    pub tcp_keepalive_interval: Option<Duration>,
    pub tcp_keepalive_retries: Option<u32>,
    pub tcp_user_timeout: Option<Duration>,
    pub tcp_send_buffer_bytes: Option<usize>,
    pub tcp_recv_buffer_bytes: Option<usize>,
    pub strict_socket_option_mode: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SocketOptionReport {
    pub tcp_nodelay: SocketOptionOutcome,
    pub tcp_keepalive: SocketOptionOutcome,
    pub tcp_user_timeout: SocketOptionOutcome,
    pub tcp_send_buffer_bytes: SocketOptionOutcome,
    pub tcp_recv_buffer_bytes: SocketOptionOutcome,
}

impl From<&SocketConfig> for SocketOptions {
    fn from(config: &SocketConfig) -> Self {
        Self {
            tcp_nodelay: config.tcp_nodelay,
            tcp_keepalive: config.tcp_keepalive,
            tcp_keepalive_idle: config.tcp_keepalive_idle(),
            tcp_keepalive_interval: config.tcp_keepalive_interval(),
            tcp_keepalive_retries: config.tcp_keepalive_retries,
            tcp_user_timeout: config.tcp_user_timeout(),
            tcp_send_buffer_bytes: config.tcp_send_buffer_bytes,
            tcp_recv_buffer_bytes: config.tcp_recv_buffer_bytes,
            strict_socket_option_mode: config.strict_socket_option_mode,
        }
    }
}

impl SocketOptions {
    pub fn apply_to(
        self,
        stream: &TcpStream,
        socket_kind: &'static str,
    ) -> anyhow::Result<SocketOptionReport> {
        apply_socket_options(stream, &self, socket_kind)
    }
}

pub fn apply_socket_options(
    stream: &TcpStream,
    options: &SocketOptions,
    socket_kind: &'static str,
) -> anyhow::Result<SocketOptionReport> {
    let socket = SockRef::from(stream);
    let mut report = SocketOptionReport::default();
    let mut strict_error = None;

    report.tcp_nodelay = apply_socket_option(
        socket_kind,
        "tcp_nodelay",
        options.strict_socket_option_mode,
        || socket.set_tcp_nodelay(options.tcp_nodelay),
        &mut strict_error,
    );
    report.tcp_keepalive = apply_keepalive_option(&socket, options, socket_kind, &mut strict_error);
    report.tcp_user_timeout = apply_user_timeout_option(
        &socket,
        options.tcp_user_timeout,
        options.strict_socket_option_mode,
        socket_kind,
        &mut strict_error,
    );
    report.tcp_send_buffer_bytes = apply_socket_option(
        socket_kind,
        "tcp_send_buffer_bytes",
        options.strict_socket_option_mode,
        || match options.tcp_send_buffer_bytes {
            Some(size) => socket.set_send_buffer_size(size),
            None => Ok(()),
        },
        &mut strict_error,
    );
    report.tcp_recv_buffer_bytes = apply_socket_option(
        socket_kind,
        "tcp_recv_buffer_bytes",
        options.strict_socket_option_mode,
        || match options.tcp_recv_buffer_bytes {
            Some(size) => socket.set_recv_buffer_size(size),
            None => Ok(()),
        },
        &mut strict_error,
    );

    match strict_error {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

fn apply_keepalive_option(
    socket: &SockRef<'_>,
    options: &SocketOptions,
    socket_kind: &'static str,
    strict_error: &mut Option<anyhow::Error>,
) -> SocketOptionOutcome {
    let outcome = if options.tcp_keepalive {
        let mut keepalive = TcpKeepalive::new();
        let mut unsupported = false;

        if let Some(idle) = options.tcp_keepalive_idle {
            keepalive = keepalive.with_time(idle);
        }

        if let Some(interval) = options.tcp_keepalive_interval {
            keepalive = add_keepalive_interval(keepalive, interval, &mut unsupported);
        }

        if let Some(retries) = options.tcp_keepalive_retries {
            keepalive = add_keepalive_retries(keepalive, retries, &mut unsupported);
        }

        match socket.set_tcp_keepalive(&keepalive) {
            Ok(()) if unsupported => SocketOptionOutcome::Unsupported,
            Ok(()) => SocketOptionOutcome::Applied,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => handle_unsupported(
                socket_kind,
                "tcp_keepalive",
                options.strict_socket_option_mode,
                strict_error,
                error,
            ),
            Err(error) => handle_failed(socket_kind, "tcp_keepalive", strict_error, error),
        }
    } else {
        match socket.set_keepalive(false) {
            Ok(()) => SocketOptionOutcome::Applied,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => handle_unsupported(
                socket_kind,
                "tcp_keepalive",
                options.strict_socket_option_mode,
                strict_error,
                error,
            ),
            Err(error) => handle_failed(socket_kind, "tcp_keepalive", strict_error, error),
        }
    };

    record_socket_option(socket_kind, "tcp_keepalive", outcome);
    outcome
}

fn apply_user_timeout_option(
    socket: &SockRef<'_>,
    timeout: Option<Duration>,
    strict: bool,
    socket_kind: &'static str,
    strict_error: &mut Option<anyhow::Error>,
) -> SocketOptionOutcome {
    let outcome = match timeout {
        None => SocketOptionOutcome::Applied,
        Some(timeout) => apply_user_timeout(socket, timeout, strict, socket_kind, strict_error),
    };

    record_socket_option(socket_kind, "tcp_user_timeout", outcome);
    outcome
}

fn apply_user_timeout(
    socket: &SockRef<'_>,
    timeout: Duration,
    strict: bool,
    socket_kind: &'static str,
    strict_error: &mut Option<anyhow::Error>,
) -> SocketOptionOutcome {
    #[cfg(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "linux",
        target_os = "cygwin"
    ))]
    {
        match socket.set_tcp_user_timeout(Some(timeout)) {
            Ok(()) => SocketOptionOutcome::Applied,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                handle_unsupported(socket_kind, "tcp_user_timeout", strict, strict_error, error)
            }
            Err(error) => handle_failed(socket_kind, "tcp_user_timeout", strict_error, error),
        }
    }

    #[cfg(not(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "linux",
        target_os = "cygwin"
    )))]
    {
        let _ = socket;
        let _ = timeout;
        handle_unsupported(
            socket_kind,
            "tcp_user_timeout",
            strict,
            strict_error,
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "tcp user timeout unsupported",
            ),
        )
    }
}

fn apply_socket_option<F>(
    socket_kind: &'static str,
    option_name: &'static str,
    strict: bool,
    apply: F,
    strict_error: &mut Option<anyhow::Error>,
) -> SocketOptionOutcome
where
    F: FnOnce() -> std::io::Result<()>,
{
    match apply() {
        Ok(()) => {
            let outcome = SocketOptionOutcome::Applied;
            record_socket_option(socket_kind, option_name, outcome);
            outcome
        }
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
            let outcome = handle_unsupported(socket_kind, option_name, strict, strict_error, error);
            record_socket_option(socket_kind, option_name, outcome);
            outcome
        }
        Err(error) => {
            let outcome = handle_failed(socket_kind, option_name, strict_error, error);
            record_socket_option(socket_kind, option_name, outcome);
            outcome
        }
    }
}

fn add_keepalive_interval(
    keepalive: TcpKeepalive,
    interval: Duration,
    _unsupported: &mut bool,
) -> TcpKeepalive {
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        all(target_os = "wasi", not(target_env = "p1")),
    ))]
    {
        keepalive.with_interval(interval)
    }

    #[cfg(not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        all(target_os = "wasi", not(target_env = "p1")),
    )))]
    {
        let _ = interval;
        *_unsupported = true;
        keepalive
    }
}

fn add_keepalive_retries(
    keepalive: TcpKeepalive,
    retries: u32,
    _unsupported: &mut bool,
) -> TcpKeepalive {
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        all(target_os = "wasi", not(target_env = "p1")),
    ))]
    {
        keepalive.with_retries(retries)
    }

    #[cfg(not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        all(target_os = "wasi", not(target_env = "p1")),
    )))]
    {
        let _ = retries;
        *_unsupported = true;
        keepalive
    }
}

fn handle_unsupported(
    socket_kind: &'static str,
    option_name: &'static str,
    strict: bool,
    strict_error: &mut Option<anyhow::Error>,
    error: std::io::Error,
) -> SocketOptionOutcome {
    tracing::warn!(
        socket_kind,
        option_name,
        strict,
        error = %error,
        "socket option unsupported"
    );
    if strict {
        *strict_error = Some(anyhow::Error::new(error).context(format!(
            "socket option {option_name} unsupported for {socket_kind}"
        )));
    }
    SocketOptionOutcome::Unsupported
}

fn handle_failed(
    socket_kind: &'static str,
    option_name: &'static str,
    strict_error: &mut Option<anyhow::Error>,
    error: std::io::Error,
) -> SocketOptionOutcome {
    tracing::warn!(
        socket_kind,
        option_name,
        error = %error,
        "socket option failed"
    );
    *strict_error = Some(anyhow::Error::new(error).context(format!(
        "apply socket option {option_name} for {socket_kind}"
    )));
    SocketOptionOutcome::Failed
}

fn record_socket_option(
    socket_kind: &'static str,
    option_name: &'static str,
    outcome: SocketOptionOutcome,
) {
    let outcome_label = match outcome {
        SocketOptionOutcome::Applied => "applied",
        SocketOptionOutcome::Unsupported => "unsupported",
        SocketOptionOutcome::Failed => "failed",
    };
    metrics_crate::counter!(
        "pg_kinetic_socket_option_total",
        "socket" => socket_kind,
        "option" => option_name,
        "outcome" => outcome_label
    )
    .increment(1);
}
