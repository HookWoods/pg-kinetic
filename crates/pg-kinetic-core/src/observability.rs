#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolPhase {
    Startup,
    Auth,
    TlsHandshake,
    BackendCheckout,
    Parse,
    Bind,
    Execute,
    Rows,
    Drain,
    Reset,
    Cancel,
    Close,
}

impl ProtocolPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Auth => "auth",
            Self::TlsHandshake => "tls_handshake",
            Self::BackendCheckout => "backend_checkout",
            Self::Parse => "parse",
            Self::Bind => "bind",
            Self::Execute => "execute",
            Self::Rows => "rows",
            Self::Drain => "drain",
            Self::Reset => "reset",
            Self::Cancel => "cancel",
            Self::Close => "close",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceEvent {
    ClientAccepted,
    StartupComplete,
    BackendCheckedOut,
    BackendReleased,
    BackendDiscarded,
    QueryStarted,
    QueryFinished,
    RecoveryStarted,
    RecoveryFinished,
    OverloadRejected,
}

impl TraceEvent {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientAccepted => "client_accepted",
            Self::StartupComplete => "startup_complete",
            Self::BackendCheckedOut => "backend_checked_out",
            Self::BackendReleased => "backend_released",
            Self::BackendDiscarded => "backend_discarded",
            Self::QueryStarted => "query_started",
            Self::QueryFinished => "query_finished",
            Self::RecoveryStarted => "recovery_started",
            Self::RecoveryFinished => "recovery_finished",
            Self::OverloadRejected => "overload_rejected",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricName {
    BackpressureEvents,
    PoolCheckoutWaitMs,
    ClientConnectionsTotal,
    PreparedEventsTotal,
    BackendPinTotal,
    BackendCleanupTotal,
    BackendRecoveryTotal,
    BackendSqlstateTotal,
    RouteCheckoutWaitMs,
    RouteInFlight,
    RouteWaiting,
    TimeoutTotal,
    BufferLimitTotal,
    ProtocolPhaseDuration,
}

impl MetricName {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BackpressureEvents => "pg_kinetic_backpressure_events_total",
            Self::PoolCheckoutWaitMs => "pg_kinetic_pool_checkout_wait_ms",
            Self::ClientConnectionsTotal => "pg_kinetic_client_connections_total",
            Self::PreparedEventsTotal => "pg_kinetic_prepared_events_total",
            Self::BackendPinTotal => "pg_kinetic_backend_pin_total",
            Self::BackendCleanupTotal => "pg_kinetic_backend_cleanup_total",
            Self::BackendRecoveryTotal => "pg_kinetic_backend_recovery_total",
            Self::BackendSqlstateTotal => "pg_kinetic_backend_sqlstate_total",
            Self::RouteCheckoutWaitMs => "pg_kinetic_route_checkout_wait_ms",
            Self::RouteInFlight => "pg_kinetic_route_in_flight",
            Self::RouteWaiting => "pg_kinetic_route_waiting",
            Self::TimeoutTotal => "pg_kinetic_timeout_total",
            Self::BufferLimitTotal => "pg_kinetic_buffer_limit_total",
            Self::ProtocolPhaseDuration => "pg_kinetic_protocol_phase_duration_ms",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricOutcome {
    Ok,
    Error,
    Timeout,
    Rejected,
    Canceled,
    Discarded,
}

impl MetricOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Rejected => "rejected",
            Self::Canceled => "canceled",
            Self::Discarded => "discarded",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LabelPolicy;

impl LabelPolicy {
    pub const PHASE: &'static str = "phase";
    pub const OUTCOME: &'static str = "outcome";
    pub const ROUTE: &'static str = "route";
    pub const VIEW: &'static str = "view";
    pub const STATE: &'static str = "state";
    pub const EVENT: &'static str = "event";
    pub const ALLOWED_LABELS: [&'static str; 6] = [
        Self::PHASE,
        Self::OUTCOME,
        Self::ROUTE,
        Self::VIEW,
        Self::STATE,
        Self::EVENT,
    ];

    #[must_use]
    pub fn allows(label: &str) -> bool {
        Self::ALLOWED_LABELS.contains(&label)
    }
}
