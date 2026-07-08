#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QosDefaults;

impl QosDefaults {
    pub const MAX_ROUTE_IN_FLIGHT: usize = 100;
    pub const MAX_ROUTE_WAITERS: usize = 1_000;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeoutDefaults;

impl TimeoutDefaults {
    pub const QUERY_TIMEOUT_MS: u64 = 30_000;
    pub const IDLE_CLIENT_TIMEOUT_MS: u64 = 300_000;
    pub const IDLE_TRANSACTION_TIMEOUT_MS: u64 = 60_000;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferDefaults;

impl BufferDefaults {
    pub const MAX_CLIENT_BUFFER_BYTES: usize = 1_048_576;
    pub const MAX_BACKEND_BUFFER_BYTES: usize = 4_194_304;
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
    ReadAfterWriteTotal,
    RouteCheckoutWaitMs,
    RouteInFlight,
    RouteWaiting,
    TimeoutTotal,
    BufferLimitTotal,
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
            Self::ReadAfterWriteTotal => "pg_kinetic_read_after_write_total",
            Self::RouteCheckoutWaitMs => "pg_kinetic_route_checkout_wait_ms",
            Self::RouteInFlight => "pg_kinetic_route_in_flight",
            Self::RouteWaiting => "pg_kinetic_route_waiting",
            Self::TimeoutTotal => "pg_kinetic_timeout_total",
            Self::BufferLimitTotal => "pg_kinetic_buffer_limit_total",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedEvent {
    Parse,
    Bind,
    Materialize,
    Close,
    Invalidate,
}

impl PreparedEvent {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::Bind => "bind",
            Self::Materialize => "materialize",
            Self::Close => "close",
            Self::Invalidate => "invalidate",
        }
    }
}
