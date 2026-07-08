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
    ReadAfterWriteTotal,
    ReadAfterWriteWaitMs,
    ReadAfterWriteRejectionsTotal,
    RouteDecisionsTotal,
    RouteFallbacksTotal,
    ReplicaHealth,
    ReplicaLagMs,
    ReplicaReplayLsn,
    SplitBrainWarningsTotal,
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
            Self::ReadAfterWriteTotal => "pg_kinetic_read_after_write_total",
            Self::ReadAfterWriteWaitMs => "pg_kinetic_read_after_write_wait_ms",
            Self::ReadAfterWriteRejectionsTotal => "pg_kinetic_read_after_write_rejections_total",
            Self::RouteDecisionsTotal => "pg_kinetic_route_decisions_total",
            Self::RouteFallbacksTotal => "pg_kinetic_route_fallbacks_total",
            Self::ReplicaHealth => "pg_kinetic_replica_health",
            Self::ReplicaLagMs => "pg_kinetic_replica_lag_ms",
            Self::ReplicaReplayLsn => "pg_kinetic_replica_replay_lsn",
            Self::SplitBrainWarningsTotal => "pg_kinetic_split_brain_warnings_total",
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
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricLabel {
    Action,
    Event,
    Endpoint,
    Kind,
    LagState,
    FallbackPolicy,
    Health,
    Mode,
    Option,
    Outcome,
    Phase,
    Reason,
    QueryClass,
    Route,
    Scope,
    Socket,
    Sqlstate,
    State,
    Status,
    TargetRole,
    Trigger,
}

impl MetricLabel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Action => "action",
            Self::Event => "event",
            Self::Endpoint => "endpoint",
            Self::Kind => "kind",
            Self::LagState => "lag_state",
            Self::FallbackPolicy => "fallback_policy",
            Self::Health => "health",
            Self::Mode => "mode",
            Self::Option => "option",
            Self::Outcome => "outcome",
            Self::Phase => "phase",
            Self::Reason => "reason",
            Self::QueryClass => "query_class",
            Self::Route => "route",
            Self::Scope => "scope",
            Self::Socket => "socket",
            Self::Sqlstate => "sqlstate",
            Self::State => "state",
            Self::Status => "status",
            Self::TargetRole => "target_role",
            Self::Trigger => "trigger",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricDescriptor {
    pub name: &'static str,
    pub kind: MetricKind,
    pub unit: &'static str,
    pub description: &'static str,
    pub labels: &'static [MetricLabel],
    pub cardinality_note: &'static str,
}

impl MetricDescriptor {
    pub const fn new(
        name: &'static str,
        kind: MetricKind,
        unit: &'static str,
        description: &'static str,
        labels: &'static [MetricLabel],
        cardinality_note: &'static str,
    ) -> Self {
        Self {
            name,
            kind,
            unit,
            description,
            labels,
            cardinality_note,
        }
    }
}

const NO_LABELS: &[MetricLabel] = &[];
const EVENT_LABELS: &[MetricLabel] = &[MetricLabel::Event];
const REASON_LABELS: &[MetricLabel] = &[MetricLabel::Reason];
const ACTION_LABELS: &[MetricLabel] = &[MetricLabel::Action];
const OUTCOME_LABELS: &[MetricLabel] = &[MetricLabel::Outcome];
const ROUTE_DECISION_LABELS: &[MetricLabel] = &[
    MetricLabel::Route,
    MetricLabel::TargetRole,
    MetricLabel::QueryClass,
];
const ROUTE_FALLBACK_LABELS: &[MetricLabel] = &[
    MetricLabel::Route,
    MetricLabel::Reason,
    MetricLabel::FallbackPolicy,
];
const ROUTE_OUTCOME_LABELS: &[MetricLabel] = &[MetricLabel::Route, MetricLabel::Outcome];
const ROUTE_SCOPE_LABELS: &[MetricLabel] = &[MetricLabel::Route, MetricLabel::Scope];
const TRIGGER_ACTION_OUTCOME_LABELS: &[MetricLabel] = &[
    MetricLabel::Trigger,
    MetricLabel::Action,
    MetricLabel::Outcome,
];
const KIND_LABELS: &[MetricLabel] = &[MetricLabel::Kind];
const PHASE_OUTCOME_LABELS: &[MetricLabel] = &[MetricLabel::Phase, MetricLabel::Outcome];
const SCOPE_MODE_LABELS: &[MetricLabel] = &[MetricLabel::Scope, MetricLabel::Mode];
const SCOPE_MODE_REASON_LABELS: &[MetricLabel] =
    &[MetricLabel::Scope, MetricLabel::Mode, MetricLabel::Reason];
const MODE_LABELS: &[MetricLabel] = &[MetricLabel::Mode];
const MODE_REASON_LABELS: &[MetricLabel] = &[MetricLabel::Mode, MetricLabel::Reason];
const OUTCOME_ONLY_LABELS: &[MetricLabel] = &[MetricLabel::Outcome];
const ENDPOINT_HEALTH_LABELS: &[MetricLabel] = &[MetricLabel::Endpoint, MetricLabel::Health];
const ENDPOINT_LAG_LABELS: &[MetricLabel] = &[MetricLabel::Endpoint, MetricLabel::LagState];
const ENDPOINT_TARGET_ROLE_LABELS: &[MetricLabel] =
    &[MetricLabel::Endpoint, MetricLabel::TargetRole];
const ENDPOINT_TARGET_ROLE_REASON_LABELS: &[MetricLabel] = &[
    MetricLabel::Endpoint,
    MetricLabel::TargetRole,
    MetricLabel::Reason,
];
const STATE_LABELS: &[MetricLabel] = &[MetricLabel::State];
const KIND_STATUS_LABELS: &[MetricLabel] = &[MetricLabel::Kind, MetricLabel::Status];
const SOCKET_OPTION_OUTCOME_LABELS: &[MetricLabel] = &[
    MetricLabel::Socket,
    MetricLabel::Option,
    MetricLabel::Outcome,
];
const SQLSTATE_LABELS: &[MetricLabel] = &[MetricLabel::Sqlstate];

static METRIC_CATALOG: &[MetricDescriptor] = &[
    MetricDescriptor::new(
        "pg_kinetic_client_connections_total",
        MetricKind::Counter,
        "1",
        "Total accepted client connections",
        NO_LABELS,
        "Single counter without labels.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_prepared_events_total",
        MetricKind::Counter,
        "1",
        "Prepared statement virtualization events",
        EVENT_LABELS,
        "Event values stay within the prepared statement lifecycle.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_pool_checkout_wait_ms",
        MetricKind::Histogram,
        "ms",
        "Backend checkout wait time in milliseconds",
        OUTCOME_ONLY_LABELS,
        "Outcome splits successful, timeout, and canceled waits.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_backend_pin_total",
        MetricKind::Counter,
        "1",
        "Backend pin decisions by reason",
        REASON_LABELS,
        "Reason values stay aligned with pinning causes.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_backend_cleanup_total",
        MetricKind::Counter,
        "1",
        "Backend cleanup decisions by action",
        ACTION_LABELS,
        "Action values stay aligned with cleanup outcomes.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_backend_recovery_total",
        MetricKind::Counter,
        "1",
        "Backend recovery attempts by trigger, action, and outcome",
        TRIGGER_ACTION_OUTCOME_LABELS,
        "Trigger, action, and outcome all come from bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_backend_sqlstate_total",
        MetricKind::Counter,
        "1",
        "Backend ErrorResponse counts by SQLSTATE",
        SQLSTATE_LABELS,
        "SQLSTATE is a normalized error code, not SQL text.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_read_after_write_total",
        MetricKind::Counter,
        "1",
        "Read-after-write freshness outcomes",
        OUTCOME_LABELS,
        "Outcome values stay aligned with freshness states.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_read_after_write_wait_ms",
        MetricKind::Histogram,
        "ms",
        "Read-after-write wait time in milliseconds by route and outcome",
        ROUTE_OUTCOME_LABELS,
        "Route labels omit raw client addresses and outcome values stay bounded.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_read_after_write_rejections_total",
        MetricKind::Counter,
        "1",
        "Read-after-write rejections by route and outcome",
        ROUTE_OUTCOME_LABELS,
        "Route labels omit raw client addresses and outcome values stay bounded.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_route_decisions_total",
        MetricKind::Counter,
        "1",
        "Routing decisions by route, target role, and query class",
        ROUTE_DECISION_LABELS,
        "Route, target role, and query class stay aligned with routing enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_route_fallbacks_total",
        MetricKind::Counter,
        "1",
        "Routing fallbacks by route, reason, and fallback policy",
        ROUTE_FALLBACK_LABELS,
        "Fallback reasons and policies stay aligned with routing enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_replica_health",
        MetricKind::Gauge,
        "1",
        "Replica health series by endpoint and health state",
        ENDPOINT_HEALTH_LABELS,
        "Endpoint identifiers are stable numeric ids and health states are bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_replica_lag_ms",
        MetricKind::Gauge,
        "ms",
        "Replica lag in milliseconds by endpoint and lag state",
        ENDPOINT_LAG_LABELS,
        "Endpoint identifiers are stable numeric ids and lag states are bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_replica_replay_lsn",
        MetricKind::Gauge,
        "lsn",
        "Replica replay LSN by endpoint and target role",
        ENDPOINT_TARGET_ROLE_LABELS,
        "Endpoint identifiers are stable numeric ids and target roles are bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_split_brain_warnings_total",
        MetricKind::Counter,
        "1",
        "Split-brain warnings by endpoint, target role, and reason",
        ENDPOINT_TARGET_ROLE_REASON_LABELS,
        "Endpoint identifiers are stable numeric ids and role mismatch reasons stay bounded.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_backpressure_events_total",
        MetricKind::Counter,
        "1",
        "Backpressure outcomes by route",
        ROUTE_OUTCOME_LABELS,
        "Route labels omit raw client addresses and stay derived from route identity only.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_route_checkout_wait_ms",
        MetricKind::Histogram,
        "ms",
        "Route checkout wait time in milliseconds",
        ROUTE_OUTCOME_LABELS,
        "Route labels omit raw client addresses and stay derived from route identity only.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_route_in_flight",
        MetricKind::Gauge,
        "1",
        "Route in-flight checkout count",
        ROUTE_SCOPE_LABELS,
        "Route labels omit raw client addresses and stay derived from route identity only.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_route_waiting",
        MetricKind::Gauge,
        "1",
        "Route waiting checkout count",
        ROUTE_SCOPE_LABELS,
        "Route labels omit raw client addresses and stay derived from route identity only.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_timeout_total",
        MetricKind::Counter,
        "1",
        "Timeouts by kind",
        KIND_LABELS,
        "Kind values stay aligned with timeout causes.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_buffer_limit_total",
        MetricKind::Counter,
        "1",
        "Buffer limit breaches by kind",
        KIND_LABELS,
        "Kind values stay aligned with buffer limit causes.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_tls_handshakes_total",
        MetricKind::Counter,
        "1",
        "Successful PostgreSQL TLS handshakes by scope and mode",
        SCOPE_MODE_LABELS,
        "Scope and mode both come from bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_tls_failures_total",
        MetricKind::Counter,
        "1",
        "Failed PostgreSQL TLS handshakes by scope, mode, and reason",
        SCOPE_MODE_REASON_LABELS,
        "Scope, mode, and reason all come from bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_auth_attempts_total",
        MetricKind::Counter,
        "1",
        "Authentication attempts by auth mode",
        MODE_LABELS,
        "Mode values stay aligned with configured authentication modes.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_auth_failures_total",
        MetricKind::Counter,
        "1",
        "Authentication failures by auth mode and reason",
        MODE_REASON_LABELS,
        "Mode and reason both come from bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_config_reload_total",
        MetricKind::Counter,
        "1",
        "Config reload decisions by outcome",
        OUTCOME_LABELS,
        "Outcome values stay aligned with reload decisions.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_drain_state",
        MetricKind::Gauge,
        "1",
        "Current drain state series (1.0 for the active state, 0.0 otherwise)",
        STATE_LABELS,
        "State values stay aligned with drain lifecycle states.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_health_status",
        MetricKind::Gauge,
        "1",
        "Current health state by kind and status series (1.0 for the active state, 0.0 otherwise)",
        KIND_STATUS_LABELS,
        "Kind and status both come from bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_socket_option_total",
        MetricKind::Counter,
        "1",
        "Socket option outcomes by socket kind, option, and result",
        SOCKET_OPTION_OUTCOME_LABELS,
        "Socket, option, and outcome all come from bounded enums.",
    ),
    MetricDescriptor::new(
        "pg_kinetic_protocol_phase_duration_ms",
        MetricKind::Histogram,
        "ms",
        "Protocol phase duration in milliseconds",
        PHASE_OUTCOME_LABELS,
        "Phase and outcome are both bounded by protocol enums.",
    ),
];

#[must_use]
pub const fn metric_catalog() -> &'static [MetricDescriptor] {
    METRIC_CATALOG
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LabelPolicy;

impl LabelPolicy {
    pub const PHASE: &'static str = "phase";
    pub const OUTCOME: &'static str = "outcome";
    pub const ENDPOINT: &'static str = "endpoint";
    pub const ROUTE: &'static str = "route";
    pub const VIEW: &'static str = "view";
    pub const STATE: &'static str = "state";
    pub const EVENT: &'static str = "event";
    pub const TARGET_ROLE: &'static str = "target_role";
    pub const QUERY_CLASS: &'static str = "query_class";
    pub const REASON: &'static str = "reason";
    pub const FALLBACK_POLICY: &'static str = "fallback_policy";
    pub const HEALTH: &'static str = "health";
    pub const LAG_STATE: &'static str = "lag_state";
    pub const ALLOWED_LABELS: [&'static str; 13] = [
        Self::PHASE,
        Self::OUTCOME,
        Self::ENDPOINT,
        Self::ROUTE,
        Self::TARGET_ROLE,
        Self::QUERY_CLASS,
        Self::REASON,
        Self::FALLBACK_POLICY,
        Self::HEALTH,
        Self::LAG_STATE,
        Self::VIEW,
        Self::STATE,
        Self::EVENT,
    ];

    #[must_use]
    pub fn allows(label: &str) -> bool {
        Self::ALLOWED_LABELS.contains(&label)
    }
}
