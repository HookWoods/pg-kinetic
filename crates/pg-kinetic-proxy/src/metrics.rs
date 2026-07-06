use std::net::SocketAddr;

use crate::socket::SocketOptionOutcome;
use metrics_exporter_prometheus::PrometheusBuilder;
use pg_kinetic_core::{
    cleanup::CleanupAction,
    constants::{MetricName, PreparedEvent},
    route::RouteKey,
    security::{AuthMode, BackendTlsMode, ClientTlsMode, DrainState, HealthStatus},
};
use pg_kinetic_core::{
    recovery::{RecoveryAction, RecoveryTrigger},
    virtual_session::PinReason,
};
use pg_kinetic_wire::sqlstate::SqlState;

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub listen_addr: Option<SocketAddr>,
}

pub fn install(config: MetricsConfig) -> anyhow::Result<()> {
    if let Some(addr) = config.listen_addr {
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()
            .map_err(|error| anyhow::anyhow!("install prometheus exporter: {error}"))?;
        tracing::info!(%addr, "metrics listener enabled");
    }

    describe_metrics();
    Ok(())
}

pub fn record_pool_checkout(wait_ms: f64, outcome: &'static str) {
    metrics_crate::histogram!(
        MetricName::PoolCheckoutWaitMs.as_str(),
        "outcome" => outcome
    )
    .record(wait_ms);
}

pub fn increment_client_connections() {
    metrics_crate::counter!(MetricName::ClientConnectionsTotal.as_str()).increment(1);
}

pub fn increment_prepared_event(event: PreparedEvent) {
    metrics_crate::counter!(
        MetricName::PreparedEventsTotal.as_str(),
        "event" => event.as_str()
    )
    .increment(1);
}

pub fn increment_pin(reason: PinReason) {
    metrics_crate::counter!(
        MetricName::BackendPinTotal.as_str(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

pub fn increment_cleanup(action: CleanupAction) {
    metrics_crate::counter!(
        MetricName::BackendCleanupTotal.as_str(),
        "action" => action.metric_label()
    )
    .increment(1);
}

pub fn increment_recovery(trigger: RecoveryTrigger, action: RecoveryAction, outcome: &'static str) {
    metrics_crate::counter!(
        MetricName::BackendRecoveryTotal.as_str(),
        "trigger" => trigger.metric_label(),
        "action" => action.metric_label(),
        "outcome" => outcome
    )
    .increment(1);
}

pub fn increment_sqlstate(sqlstate: SqlState) {
    metrics_crate::counter!(
        MetricName::BackendSqlstateTotal.as_str(),
        "sqlstate" => sqlstate.as_str().to_string()
    )
    .increment(1);
}

pub fn increment_backpressure_event(route: &RouteKey, outcome: &'static str) {
    metrics_crate::counter!(
        MetricName::BackpressureEvents.as_str(),
        "route" => route.metric_label(),
        "outcome" => outcome
    )
    .increment(1);
}

pub fn record_route_wait(route: &RouteKey, wait_ms: f64, outcome: &'static str) {
    metrics_crate::histogram!(
        MetricName::RouteCheckoutWaitMs.as_str(),
        "route" => route.metric_label(),
        "outcome" => outcome
    )
    .record(wait_ms);
}

pub fn record_route_in_flight(route: &RouteKey, in_flight: usize) {
    metrics_crate::gauge!(
        MetricName::RouteInFlight.as_str(),
        "route" => route.metric_label(),
        "scope" => QueueScope::Route.as_str()
    )
    .set(in_flight as f64);
}

pub fn record_route_waiting(route: &RouteKey, waiting: usize) {
    metrics_crate::gauge!(
        MetricName::RouteWaiting.as_str(),
        "route" => route.metric_label(),
        "scope" => QueueScope::Route.as_str()
    )
    .set(waiting as f64);
}

pub fn increment_timeout(kind: &'static str) {
    metrics_crate::counter!(
        MetricName::TimeoutTotal.as_str(),
        "kind" => kind
    )
    .increment(1);
}

pub fn increment_buffer_limit(kind: &'static str) {
    metrics_crate::counter!(
        MetricName::BufferLimitTotal.as_str(),
        "kind" => kind
    )
    .increment(1);
}

pub fn record_tls_handshake<M: MetricLabel>(scope: TlsScope, mode: M) {
    metrics_crate::counter!(
        OperationalMetricName::TlsHandshakesTotal.as_str(),
        "scope" => scope.metric_label(),
        "mode" => mode.metric_label()
    )
    .increment(1);
}

pub fn record_tls_failure<M: MetricLabel>(scope: TlsScope, mode: M, reason: TlsFailureReason) {
    metrics_crate::counter!(
        OperationalMetricName::TlsFailuresTotal.as_str(),
        "scope" => scope.metric_label(),
        "mode" => mode.metric_label(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

pub fn record_auth_attempt(mode: AuthMode) {
    metrics_crate::counter!(
        OperationalMetricName::AuthAttemptsTotal.as_str(),
        "mode" => mode.metric_label()
    )
    .increment(1);
}

pub fn record_auth_failure(mode: AuthMode, reason: AuthFailureReason) {
    metrics_crate::counter!(
        OperationalMetricName::AuthFailuresTotal.as_str(),
        "mode" => mode.metric_label(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

pub fn record_config_reload(outcome: ReloadOutcome) {
    metrics_crate::counter!(
        OperationalMetricName::ConfigReloadTotal.as_str(),
        "outcome" => outcome.metric_label()
    )
    .increment(1);
}

pub fn record_drain_state(state: DrainState) {
    for candidate in [
        DrainState::Accepting,
        DrainState::Draining,
        DrainState::Drained,
    ] {
        metrics_crate::gauge!(
            OperationalMetricName::DrainState.as_str(),
            "state" => candidate.metric_label()
        )
        .set(if candidate == state { 1.0 } else { 0.0 });
    }
}

pub fn record_health_status(kind: HealthKind, status: HealthStatus) {
    for candidate in [
        HealthStatus::Ready,
        HealthStatus::NotReady,
        HealthStatus::Live,
        HealthStatus::Degraded,
    ] {
        metrics_crate::gauge!(
            OperationalMetricName::HealthStatus.as_str(),
            "kind" => kind.metric_label(),
            "status" => candidate.metric_label()
        )
        .set(if candidate == status { 1.0 } else { 0.0 });
    }
}

pub fn record_socket_option<S: MetricLabel, O: MetricLabel>(
    socket_kind: S,
    option: O,
    outcome: SocketOptionOutcome,
) {
    metrics_crate::counter!(
        OperationalMetricName::SocketOptionTotal.as_str(),
        "socket" => socket_kind.metric_label(),
        "option" => option.metric_label(),
        "outcome" => outcome.metric_label()
    )
    .increment(1);
}

fn describe_metrics() {
    metrics_crate::describe_counter!(
        MetricName::ClientConnectionsTotal.as_str(),
        "Total accepted client connections"
    );
    metrics_crate::describe_histogram!(
        MetricName::PoolCheckoutWaitMs.as_str(),
        "Backend checkout wait time in milliseconds"
    );
    metrics_crate::describe_counter!(
        MetricName::PreparedEventsTotal.as_str(),
        "Prepared statement virtualization events"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendPinTotal.as_str(),
        "Backend pin decisions by reason"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendCleanupTotal.as_str(),
        "Backend cleanup decisions by action"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendRecoveryTotal.as_str(),
        "Backend recovery attempts by trigger, action, and outcome"
    );
    metrics_crate::describe_counter!(
        MetricName::BackendSqlstateTotal.as_str(),
        "Backend ErrorResponse counts by SQLSTATE"
    );
    metrics_crate::describe_counter!(
        MetricName::BackpressureEvents.as_str(),
        "Backpressure outcomes by route"
    );
    metrics_crate::describe_histogram!(
        MetricName::RouteCheckoutWaitMs.as_str(),
        "Route checkout wait time in milliseconds"
    );
    metrics_crate::describe_gauge!(
        MetricName::RouteInFlight.as_str(),
        "Route in-flight checkout count"
    );
    metrics_crate::describe_gauge!(
        MetricName::RouteWaiting.as_str(),
        "Route waiting checkout count"
    );
    metrics_crate::describe_counter!(MetricName::TimeoutTotal.as_str(), "Timeouts by kind");
    metrics_crate::describe_counter!(
        MetricName::BufferLimitTotal.as_str(),
        "Buffer limit breaches by kind"
    );
    metrics_crate::describe_counter!(
        OperationalMetricName::TlsHandshakesTotal.as_str(),
        "Successful PostgreSQL TLS handshakes by scope and mode"
    );
    metrics_crate::describe_counter!(
        OperationalMetricName::TlsFailuresTotal.as_str(),
        "Failed PostgreSQL TLS handshakes by scope, mode, and reason"
    );
    metrics_crate::describe_counter!(
        OperationalMetricName::AuthAttemptsTotal.as_str(),
        "Authentication attempts by auth mode"
    );
    metrics_crate::describe_counter!(
        OperationalMetricName::AuthFailuresTotal.as_str(),
        "Authentication failures by auth mode and reason"
    );
    metrics_crate::describe_counter!(
        OperationalMetricName::ConfigReloadTotal.as_str(),
        "Config reload decisions by outcome"
    );
    metrics_crate::describe_gauge!(
        OperationalMetricName::DrainState.as_str(),
        "Current drain state series (1.0 for the active state, 0.0 otherwise)"
    );
    metrics_crate::describe_gauge!(
        OperationalMetricName::HealthStatus.as_str(),
        "Current health state by kind and status series (1.0 for the active state, 0.0 otherwise)"
    );
    metrics_crate::describe_counter!(
        OperationalMetricName::SocketOptionTotal.as_str(),
        "Socket option outcomes by socket kind, option, and result"
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueScope {
    Route,
}

impl QueueScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Route => "route",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalMetricName {
    TlsHandshakesTotal,
    TlsFailuresTotal,
    AuthAttemptsTotal,
    AuthFailuresTotal,
    ConfigReloadTotal,
    DrainState,
    HealthStatus,
    SocketOptionTotal,
}

impl OperationalMetricName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TlsHandshakesTotal => "pg_kinetic_tls_handshakes_total",
            Self::TlsFailuresTotal => "pg_kinetic_tls_failures_total",
            Self::AuthAttemptsTotal => "pg_kinetic_auth_attempts_total",
            Self::AuthFailuresTotal => "pg_kinetic_auth_failures_total",
            Self::ConfigReloadTotal => "pg_kinetic_config_reload_total",
            Self::DrainState => "pg_kinetic_drain_state",
            Self::HealthStatus => "pg_kinetic_health_status",
            Self::SocketOptionTotal => "pg_kinetic_socket_option_total",
        }
    }
}

pub trait MetricLabel {
    fn metric_label(self) -> &'static str;
}

impl MetricLabel for TlsScope {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Backend => "backend",
        }
    }
}

impl MetricLabel for ClientTlsMode {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabel for BackendTlsMode {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabel for AuthMode {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabel for DrainState {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabel for HealthStatus {
    fn metric_label(self) -> &'static str {
        self.as_str()
    }
}

impl MetricLabel for SocketOptionOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }
}

impl MetricLabel for TlsFailureReason {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::HandshakeError => "handshake_error",
            Self::VerificationFailed => "verification_failed",
            Self::IoError => "io_error",
        }
    }
}

impl MetricLabel for AuthFailureReason {
    fn metric_label(self) -> &'static str {
        match self {
            Self::UnknownUser => "unknown_user",
            Self::PasswordRequired => "password_required",
            Self::InvalidPassword => "invalid_password",
            Self::ProtocolError => "protocol_error",
            Self::IoError => "io_error",
        }
    }
}

impl MetricLabel for ReloadOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Unchanged => "unchanged",
            Self::Error => "error",
        }
    }
}

impl MetricLabel for HealthKind {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Ready => "ready",
            Self::Backend => "backend",
        }
    }
}

impl MetricLabel for SocketKind {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Backend => "backend",
        }
    }
}

impl MetricLabel for SocketOption {
    fn metric_label(self) -> &'static str {
        match self {
            Self::TcpNodelay => "tcp_nodelay",
            Self::TcpKeepalive => "tcp_keepalive",
            Self::TcpUserTimeout => "tcp_user_timeout",
            Self::TcpSendBufferBytes => "tcp_send_buffer_bytes",
            Self::TcpRecvBufferBytes => "tcp_recv_buffer_bytes",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsScope {
    Client,
    Backend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsFailureReason {
    Denied,
    HandshakeError,
    VerificationFailed,
    IoError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthFailureReason {
    UnknownUser,
    PasswordRequired,
    InvalidPassword,
    ProtocolError,
    IoError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadOutcome {
    Applied,
    Rejected,
    Unchanged,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthKind {
    Process,
    Ready,
    Backend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketKind {
    Client,
    Backend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketOption {
    TcpNodelay,
    TcpKeepalive,
    TcpUserTimeout,
    TcpSendBufferBytes,
    TcpRecvBufferBytes,
}
