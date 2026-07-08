use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use pg_kinetic_core::{
    ha::{HealthProbeOutcome, ReplicaLagState, RoleProbeOutcome},
    lsn::{FreshnessStatus, PgLsn},
    observability::MetricOutcome,
    prepare::PreparedStatementSnapshot,
    recovery::{RecoveryAction, RecoveryTrigger},
    route::RouteKey,
    routing::{BackendRole, FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
    session::PinReason,
    sharding::ShardId,
};

use crate::config::{AuthFailureMessageMode, AuthMode, BackendTlsMode, ClientTlsMode, Config};
use crate::metrics;
use crate::routing::RoutingTarget;
use crate::sharding::{RouteMapReloadErrorCode, RouteMapReloadResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientSnapshot {
    pub client_id: u64,
    pub user: Option<String>,
    pub database: Option<String>,
    pub application_name: Option<String>,
    pub route_key: Option<RouteKey>,
    pub state: String,
    pub connected_duration: Duration,
}

impl ClientSnapshot {
    #[must_use]
    pub fn new(client_id: u64) -> Self {
        Self {
            client_id,
            user: None,
            database: None,
            application_name: None,
            route_key: None,
            state: String::from("connected"),
            connected_duration: Duration::ZERO,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PoolSnapshot {
    pub configured_backends: usize,
    pub active_backends: usize,
    pub idle_backends: usize,
    pub waiting_clients: usize,
}

impl PoolSnapshot {
    #[must_use]
    pub const fn new(configured_backends: usize, active_backends: usize) -> Self {
        Self {
            configured_backends,
            active_backends,
            idle_backends: configured_backends.saturating_sub(active_backends),
            waiting_clients: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerSnapshot {
    pub backend_id: u64,
    pub route_key: Option<RouteKey>,
    pub state: String,
    pub age: Duration,
    pub in_transaction: bool,
}

impl ServerSnapshot {
    #[must_use]
    pub fn new(backend_id: u64, state: impl Into<String>, age: Duration) -> Self {
        Self {
            backend_id,
            route_key: None,
            state: state.into(),
            age,
            in_transaction: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaHealthSnapshot {
    pub endpoint_id: u64,
    pub endpoint_addr: SocketAddr,
    pub expected_role: BackendRole,
    pub health: HealthProbeOutcome,
    pub role: RoleProbeOutcome,
    pub replay_lsn: Option<PgLsn>,
    pub replay_timestamp: Option<SystemTime>,
    pub lag_duration: Option<Duration>,
    pub lag_state: ReplicaLagState,
    pub last_successful_probe_at: Option<SystemTime>,
    pub last_error: Option<String>,
}

impl ReplicaHealthSnapshot {
    #[must_use]
    pub fn new(endpoint_id: u64, endpoint_addr: SocketAddr, expected_role: BackendRole) -> Self {
        Self {
            endpoint_id,
            endpoint_addr,
            expected_role,
            health: HealthProbeOutcome::new(
                pg_kinetic_core::ha::EndpointHealth::Unhealthy,
                false,
                0,
            ),
            role: RoleProbeOutcome::new(pg_kinetic_core::ha::EndpointRoleState::Unknown, None),
            replay_lsn: None,
            replay_timestamp: None,
            lag_duration: None,
            lag_state: ReplicaLagState::Unknown,
            last_successful_probe_at: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreparedSnapshot {
    pub statement_count: usize,
    pub materialization_count: usize,
    pub statements: Vec<PreparedStatementSnapshot>,
}

impl PreparedSnapshot {
    #[must_use]
    pub fn new(statement_count: usize, materialization_count: usize) -> Self {
        Self {
            statement_count,
            materialization_count,
            statements: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_statements(mut self, statements: Vec<PreparedStatementSnapshot>) -> Self {
        self.statements = statements;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinningSnapshot {
    pub client_id: u64,
    pub backend_id: Option<u64>,
    pub route_key: Option<RouteKey>,
    pub reason: PinReason,
    pub duration: Duration,
}

impl PinningSnapshot {
    #[must_use]
    pub fn new(
        client_id: u64,
        backend_id: Option<u64>,
        route_key: Option<RouteKey>,
        reason: PinReason,
        duration: Duration,
    ) -> Self {
        Self {
            client_id,
            backend_id,
            route_key,
            reason,
            duration,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverySnapshot {
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub outcome: MetricOutcome,
    pub count: u64,
    pub last_error: Option<String>,
}

impl RecoverySnapshot {
    #[must_use]
    pub fn new(trigger: RecoveryTrigger, action: RecoveryAction, outcome: MetricOutcome) -> Self {
        Self {
            trigger,
            action,
            outcome,
            count: 0,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackpressureSnapshot {
    pub route_key: RouteKey,
    pub waiting: usize,
    pub in_flight: usize,
    pub rejected: u64,
    pub timed_out: u64,
    pub canceled: u64,
}

impl BackpressureSnapshot {
    #[must_use]
    pub fn new(route_key: RouteKey) -> Self {
        Self {
            route_key,
            waiting: 0,
            in_flight: 0,
            rejected: 0,
            timed_out: 0,
            canceled: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteSnapshot {
    pub route_key: RouteKey,
    pub client_count: usize,
    pub backend_count: usize,
}

impl RouteSnapshot {
    #[must_use]
    pub fn new(route_key: RouteKey) -> Self {
        Self {
            route_key,
            client_count: 0,
            backend_count: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutePolicySnapshot {
    pub route_key: RouteKey,
    pub primary_count: usize,
    pub replica_count: usize,
    pub read_routing_mode: ReadRoutingMode,
    pub fallback_policy: FallbackPolicy,
    pub freshness_policy: FreshnessPolicy,
    pub read_after_write_timeout_ms: u64,
}

impl RoutePolicySnapshot {
    #[must_use]
    pub fn new(route_key: RouteKey) -> Self {
        Self {
            route_key,
            primary_count: 0,
            replica_count: 0,
            read_routing_mode: ReadRoutingMode::default(),
            fallback_policy: FallbackPolicy::Primary,
            freshness_policy: FreshnessPolicy::SessionWriteLsn,
            read_after_write_timeout_ms: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteCheckoutSnapshot {
    pub route_key: RouteKey,
    pub decision: RoutingTarget,
    pub freshness_outcome: Option<FreshnessStatus>,
    pub required_session_write_lsn: Option<PgLsn>,
}

impl RouteCheckoutSnapshot {
    #[must_use]
    pub fn new(
        route_key: RouteKey,
        decision: RoutingTarget,
        freshness_outcome: Option<FreshnessStatus>,
    ) -> Self {
        Self {
            route_key,
            decision,
            freshness_outcome,
            required_session_write_lsn: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteMapReloadSnapshot {
    pub route_map_generation_id: u64,
    pub success: bool,
    pub error_code: Option<RouteMapReloadErrorCode>,
    pub draining_shard_ids: Vec<ShardId>,
}

impl RouteMapReloadSnapshot {
    #[must_use]
    pub fn new(route_map_generation_id: u64, success: bool) -> Self {
        Self {
            route_map_generation_id,
            success,
            error_code: None,
            draining_shard_ids: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingsSnapshot {
    pub listen_addr: SocketAddr,
    pub backend_addr: SocketAddr,
    pub client_tls_mode: ClientTlsMode,
    pub backend_tls_mode: BackendTlsMode,
    pub auth_mode: AuthMode,
    pub auth_failure_message_mode: AuthFailureMessageMode,
    pub backend_user: Option<String>,
    pub backend_reset_query: String,
    pub recovery_mode: pg_kinetic_core::recovery::RecoveryMode,
    pub reload_enabled: bool,
    pub config_reload_interval: Duration,
    pub drain_timeout: Duration,
    pub reject_new_clients_during_drain: bool,
    pub health_addr: Option<SocketAddr>,
    pub readiness_backend_check_interval: Duration,
    pub readiness_timeout: Duration,
    pub metrics_addr: Option<SocketAddr>,
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

impl Default for SettingsSnapshot {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            backend_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            client_tls_mode: ClientTlsMode::Disable,
            backend_tls_mode: BackendTlsMode::Disable,
            auth_mode: AuthMode::PassThrough,
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
            backend_user: None,
            backend_reset_query: String::new(),
            recovery_mode: pg_kinetic_core::recovery::RecoveryMode::Recover,
            reload_enabled: false,
            config_reload_interval: Duration::ZERO,
            drain_timeout: Duration::ZERO,
            reject_new_clients_during_drain: false,
            health_addr: None,
            readiness_backend_check_interval: Duration::ZERO,
            readiness_timeout: Duration::ZERO,
            metrics_addr: None,
            tcp_nodelay: false,
            tcp_keepalive: false,
            tcp_keepalive_idle: None,
            tcp_keepalive_interval: None,
            tcp_keepalive_retries: None,
            tcp_user_timeout: None,
            tcp_send_buffer_bytes: None,
            tcp_recv_buffer_bytes: None,
            strict_socket_option_mode: false,
        }
    }
}

impl SettingsSnapshot {
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            listen_addr: config.connection.listen_addr,
            backend_addr: config.connection.backend_addr,
            client_tls_mode: config.tls.client_tls_mode,
            backend_tls_mode: config.tls.backend_tls_mode,
            auth_mode: config.auth.auth_mode,
            auth_failure_message_mode: config.auth.auth_failure_message_mode,
            backend_user: config.auth.backend_user.clone(),
            backend_reset_query: config.performance.backend_reset_query.clone(),
            recovery_mode: config.performance.recovery_mode,
            reload_enabled: config.reload.reload_enabled,
            config_reload_interval: config.reload.config_reload_interval(),
            drain_timeout: config.drain.drain_timeout(),
            reject_new_clients_during_drain: config.drain.reject_new_clients_during_drain,
            health_addr: config.health.health_addr,
            readiness_backend_check_interval: config.health.readiness_backend_check_interval(),
            readiness_timeout: config.health.readiness_timeout(),
            metrics_addr: config.observability.metrics_addr,
            tcp_nodelay: config.socket.tcp_nodelay,
            tcp_keepalive: config.socket.tcp_keepalive,
            tcp_keepalive_idle: config.socket.tcp_keepalive_idle(),
            tcp_keepalive_interval: config.socket.tcp_keepalive_interval(),
            tcp_keepalive_retries: config.socket.tcp_keepalive_retries,
            tcp_user_timeout: config.socket.tcp_user_timeout(),
            tcp_send_buffer_bytes: config.socket.tcp_send_buffer_bytes,
            tcp_recv_buffer_bytes: config.socket.tcp_recv_buffer_bytes,
            strict_socket_option_mode: config.socket.strict_socket_option_mode,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LimitsSnapshot {
    pub max_clients: usize,
    pub max_backends: usize,
    pub max_checkout_waiters: usize,
    pub max_route_in_flight: usize,
    pub max_route_waiters: usize,
    pub checkout_timeout: Duration,
    pub query_timeout: Duration,
    pub idle_client_timeout: Duration,
    pub idle_transaction_timeout: Duration,
    pub max_client_buffer_bytes: usize,
    pub max_backend_buffer_bytes: usize,
    pub recovery_timeout: Duration,
    pub drain_timeout: Duration,
    pub readiness_backend_check_interval: Duration,
    pub readiness_timeout: Duration,
    pub config_reload_interval: Duration,
    pub overload_error_code: String,
}

impl LimitsSnapshot {
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_clients: config.capacity.max_clients,
            max_backends: config.capacity.max_backends,
            max_checkout_waiters: config.capacity.max_checkout_waiters,
            max_route_in_flight: config.qos.max_route_in_flight,
            max_route_waiters: config.qos.max_route_waiters,
            checkout_timeout: config.performance.checkout_timeout(),
            query_timeout: config.qos.query_timeout(),
            idle_client_timeout: config.qos.idle_client_timeout(),
            idle_transaction_timeout: Duration::from_millis(config.qos.idle_transaction_timeout_ms),
            max_client_buffer_bytes: config.qos.max_client_buffer_bytes,
            max_backend_buffer_bytes: config.qos.max_backend_buffer_bytes,
            recovery_timeout: config.performance.recovery_timeout(),
            drain_timeout: config.drain.drain_timeout(),
            readiness_backend_check_interval: config.health.readiness_backend_check_interval(),
            readiness_timeout: config.health.readiness_timeout(),
            config_reload_interval: config.reload.config_reload_interval(),
            overload_error_code: config.qos.overload_error_code.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotStore {
    inner: Arc<RwLock<SnapshotStoreInner>>,
}

#[derive(Debug, Default)]
struct SnapshotStoreInner {
    clients: BTreeMap<u64, ClientSnapshot>,
    pool: PoolSnapshot,
    servers: BTreeMap<u64, ServerSnapshot>,
    replica_health: BTreeMap<u64, ReplicaHealthSnapshot>,
    prepared: PreparedSnapshot,
    pinning: BTreeMap<u64, PinningSnapshot>,
    recoveries: Vec<RecoverySnapshot>,
    backpressure: HashMap<RouteKey, BackpressureSnapshot>,
    routes: HashMap<RouteKey, RouteSnapshot>,
    route_policies: HashMap<RouteKey, RoutePolicySnapshot>,
    route_checkouts: HashMap<RouteKey, RouteCheckoutSnapshot>,
    route_map_reloads: Vec<RouteMapReloadSnapshot>,
    settings: SettingsSnapshot,
    limits: LimitsSnapshot,
}

impl Default for SnapshotStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SnapshotStoreInner::default())),
        }
    }
}

impl SnapshotStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn client_handle(&self) -> ClientSnapshotHandle {
        ClientSnapshotHandle::new(Arc::clone(&self.inner))
    }

    #[must_use]
    pub fn pool_handle(&self) -> PoolSnapshotHandle {
        PoolSnapshotHandle::new(Arc::clone(&self.inner))
    }

    #[must_use]
    pub fn prepared_handle(&self) -> PreparedSnapshotHandle {
        PreparedSnapshotHandle::new(Arc::clone(&self.inner))
    }

    #[must_use]
    pub fn recovery_handle(&self) -> RecoverySnapshotHandle {
        RecoverySnapshotHandle::new(Arc::clone(&self.inner))
    }

    #[must_use]
    pub fn backpressure_handle(&self) -> BackpressureSnapshotHandle {
        BackpressureSnapshotHandle::new(Arc::clone(&self.inner))
    }

    pub fn register_client(&self, snapshot: ClientSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .clients
            .insert(snapshot.client_id, snapshot);
    }

    pub fn remove_client(&self, client_id: u64) -> Option<ClientSnapshot> {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .clients
            .remove(&client_id)
    }

    #[must_use]
    pub fn client_snapshots(&self) -> Vec<ClientSnapshot> {
        let clients = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .clients
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_key(clients, |snapshot| snapshot.client_id)
    }

    pub fn set_pool_snapshot(&self, snapshot: PoolSnapshot) {
        self.inner.write().expect("snapshot store poisoned").pool = snapshot;
    }

    #[must_use]
    pub fn pool_snapshot(&self) -> PoolSnapshot {
        self.inner
            .read()
            .expect("snapshot store poisoned")
            .pool
            .clone()
    }

    pub fn set_server_snapshot(&self, snapshot: ServerSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .servers
            .insert(snapshot.backend_id, snapshot);
    }

    pub fn remove_server_snapshot(&self, backend_id: u64) -> Option<ServerSnapshot> {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .servers
            .remove(&backend_id)
    }

    #[must_use]
    pub fn server_snapshots(&self) -> Vec<ServerSnapshot> {
        let servers = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .servers
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_key(servers, |snapshot| snapshot.backend_id)
    }

    pub fn set_replica_health_snapshot(&self, snapshot: ReplicaHealthSnapshot) {
        metrics::record_replica_health_snapshot(&snapshot);
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .replica_health
            .insert(snapshot.endpoint_id, snapshot);
    }

    pub fn remove_replica_health_snapshot(
        &self,
        endpoint_id: u64,
    ) -> Option<ReplicaHealthSnapshot> {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .replica_health
            .remove(&endpoint_id)
    }

    #[must_use]
    pub fn replica_health_snapshots(&self) -> Vec<ReplicaHealthSnapshot> {
        let replica_health = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .replica_health
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_key(replica_health, |snapshot| snapshot.endpoint_id)
    }

    pub fn set_prepared_snapshot(&self, snapshot: PreparedSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .prepared = snapshot;
    }

    #[must_use]
    pub fn prepared_snapshot(&self) -> PreparedSnapshot {
        self.inner
            .read()
            .expect("snapshot store poisoned")
            .prepared
            .clone()
    }

    pub fn set_pinning_snapshot(&self, snapshot: PinningSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .pinning
            .insert(snapshot.client_id, snapshot);
    }

    pub fn remove_pinning_snapshot(&self, client_id: u64) -> Option<PinningSnapshot> {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .pinning
            .remove(&client_id)
    }

    #[must_use]
    pub fn pinning_snapshots(&self) -> Vec<PinningSnapshot> {
        let pinning = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .pinning
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_key(pinning, |snapshot| snapshot.client_id)
    }

    pub fn record_recovery(
        &self,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        outcome: MetricOutcome,
    ) {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        if let Some(snapshot) = inner.recoveries.iter_mut().find(|candidate| {
            candidate.trigger == trigger
                && candidate.action == action
                && candidate.outcome == outcome
        }) {
            snapshot.count += 1;
        } else {
            let mut snapshot = RecoverySnapshot::new(trigger, action, outcome);
            snapshot.count = 1;
            inner.recoveries.push(snapshot);
        }
    }

    pub fn set_recovery_last_error(
        &self,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        outcome: MetricOutcome,
        error: impl Into<String>,
    ) {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        if let Some(snapshot) = inner.recoveries.iter_mut().find(|candidate| {
            candidate.trigger == trigger
                && candidate.action == action
                && candidate.outcome == outcome
        }) {
            snapshot.last_error = Some(error.into());
        }
    }

    #[must_use]
    pub fn recovery_snapshots(&self) -> Vec<RecoverySnapshot> {
        let recoveries = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .recoveries
            .clone();
        sort_by_key(recoveries, |snapshot| {
            (
                snapshot.trigger.metric_label().to_owned(),
                snapshot.action.metric_label().to_owned(),
                snapshot.outcome.as_str().to_owned(),
            )
        })
    }

    pub fn set_backpressure_snapshot(&self, snapshot: BackpressureSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .backpressure
            .insert(snapshot.route_key.clone(), snapshot);
    }

    pub fn set_backpressure_route(&self, route_key: RouteKey, waiting: usize, in_flight: usize) {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        let snapshot = inner
            .backpressure
            .entry(route_key.clone())
            .or_insert_with(|| BackpressureSnapshot::new(route_key));
        snapshot.waiting = waiting;
        snapshot.in_flight = in_flight;
    }

    pub fn increment_backpressure_rejected(&self, route_key: RouteKey) {
        self.backpressure_update(route_key, |snapshot| {
            snapshot.rejected += 1;
        });
    }

    pub fn increment_backpressure_timed_out(&self, route_key: RouteKey) {
        self.backpressure_update(route_key, |snapshot| {
            snapshot.timed_out += 1;
        });
    }

    pub fn increment_backpressure_canceled(&self, route_key: RouteKey) {
        self.backpressure_update(route_key, |snapshot| {
            snapshot.canceled += 1;
        });
    }

    #[must_use]
    pub fn backpressure_snapshots(&self) -> Vec<BackpressureSnapshot> {
        let backpressure = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .backpressure
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_route_key(backpressure)
    }

    pub fn set_route_snapshot(&self, snapshot: RouteSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .routes
            .insert(snapshot.route_key.clone(), snapshot);
    }

    #[must_use]
    pub fn route_snapshots(&self) -> Vec<RouteSnapshot> {
        let routes = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .routes
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_route_key(routes)
    }

    pub fn set_route_policy_snapshot(&self, snapshot: RoutePolicySnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .route_policies
            .insert(snapshot.route_key.clone(), snapshot);
    }

    #[must_use]
    pub fn route_policy_snapshot(&self, route_key: &RouteKey) -> Option<RoutePolicySnapshot> {
        self.inner
            .read()
            .expect("snapshot store poisoned")
            .route_policies
            .get(route_key)
            .cloned()
    }

    #[must_use]
    pub fn route_policy_snapshots(&self) -> Vec<RoutePolicySnapshot> {
        let route_policies = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .route_policies
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_route_key(route_policies)
    }

    pub fn set_route_checkout_snapshot(&self, snapshot: RouteCheckoutSnapshot) {
        metrics::record_route_checkout_snapshot(&snapshot);
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .route_checkouts
            .insert(snapshot.route_key.clone(), snapshot);
    }

    #[must_use]
    pub fn route_checkout_snapshot(&self, route_key: &RouteKey) -> Option<RouteCheckoutSnapshot> {
        self.inner
            .read()
            .expect("snapshot store poisoned")
            .route_checkouts
            .get(route_key)
            .cloned()
    }

    #[must_use]
    pub fn route_checkout_snapshots(&self) -> Vec<RouteCheckoutSnapshot> {
        let route_checkouts = self
            .inner
            .read()
            .expect("snapshot store poisoned")
            .route_checkouts
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sort_by_route_key(route_checkouts)
    }

    pub fn set_route_map_reload_snapshot(&self, snapshot: RouteMapReloadSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .route_map_reloads
            .push(snapshot);
    }

    #[must_use]
    pub fn route_map_reload_snapshots(&self) -> Vec<RouteMapReloadSnapshot> {
        self.inner
            .read()
            .expect("snapshot store poisoned")
            .route_map_reloads
            .clone()
    }

    pub fn set_settings_snapshot(&self, snapshot: SettingsSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .settings = snapshot;
    }

    #[must_use]
    pub fn settings_snapshot(&self) -> SettingsSnapshot {
        self.inner
            .read()
            .expect("snapshot store poisoned")
            .settings
            .clone()
    }

    pub fn set_limits_snapshot(&self, snapshot: LimitsSnapshot) {
        self.inner.write().expect("snapshot store poisoned").limits = snapshot;
    }

    #[must_use]
    pub fn limits_snapshot(&self) -> LimitsSnapshot {
        self.inner
            .read()
            .expect("snapshot store poisoned")
            .limits
            .clone()
    }

    fn backpressure_update<F>(&self, route_key: RouteKey, update: F)
    where
        F: FnOnce(&mut BackpressureSnapshot),
    {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        let snapshot = inner
            .backpressure
            .entry(route_key.clone())
            .or_insert_with(|| BackpressureSnapshot::new(route_key));
        update(snapshot);
    }
}

#[derive(Clone, Debug)]
pub struct ClientSnapshotHandle {
    inner: Arc<RwLock<SnapshotStoreInner>>,
}

impl ClientSnapshotHandle {
    fn new(inner: Arc<RwLock<SnapshotStoreInner>>) -> Self {
        Self { inner }
    }

    pub fn register(&self, client_id: u64) {
        self.upsert(ClientSnapshot::new(client_id));
    }

    pub fn upsert(&self, snapshot: ClientSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .clients
            .insert(snapshot.client_id, snapshot);
    }

    pub fn remove(&self, client_id: u64) -> Option<ClientSnapshot> {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .clients
            .remove(&client_id)
    }
}

#[derive(Clone, Debug)]
pub struct PoolSnapshotHandle {
    inner: Arc<RwLock<SnapshotStoreInner>>,
}

impl PoolSnapshotHandle {
    fn new(inner: Arc<RwLock<SnapshotStoreInner>>) -> Self {
        Self { inner }
    }

    pub fn set(&self, snapshot: PoolSnapshot) {
        self.inner.write().expect("snapshot store poisoned").pool = snapshot;
    }
}

#[derive(Clone, Debug)]
pub struct PreparedSnapshotHandle {
    inner: Arc<RwLock<SnapshotStoreInner>>,
}

impl PreparedSnapshotHandle {
    fn new(inner: Arc<RwLock<SnapshotStoreInner>>) -> Self {
        Self { inner }
    }

    pub fn set(&self, snapshot: PreparedSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .prepared = snapshot;
    }

    pub fn increment_statement_count(&self) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .prepared
            .statement_count += 1;
    }

    pub fn increment_materialization_count(&self) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .prepared
            .materialization_count += 1;
    }

    pub fn set_statements(&self, statements: Vec<PreparedStatementSnapshot>) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .prepared
            .statements = statements;
    }
}

impl From<&RouteMapReloadResult> for RouteMapReloadSnapshot {
    fn from(result: &RouteMapReloadResult) -> Self {
        let mut draining_shard_ids = result.draining_shard_ids.clone();
        draining_shard_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));

        Self {
            route_map_generation_id: result.route_map_generation_id,
            success: result.success,
            error_code: result.error_code,
            draining_shard_ids,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecoverySnapshotHandle {
    inner: Arc<RwLock<SnapshotStoreInner>>,
}

impl RecoverySnapshotHandle {
    fn new(inner: Arc<RwLock<SnapshotStoreInner>>) -> Self {
        Self { inner }
    }

    pub fn record(&self, trigger: RecoveryTrigger, action: RecoveryAction, outcome: MetricOutcome) {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        if let Some(snapshot) = inner.recoveries.iter_mut().find(|candidate| {
            candidate.trigger == trigger
                && candidate.action == action
                && candidate.outcome == outcome
        }) {
            snapshot.count += 1;
        } else {
            let mut snapshot = RecoverySnapshot::new(trigger, action, outcome);
            snapshot.count = 1;
            inner.recoveries.push(snapshot);
        }
    }

    pub fn set_last_error(
        &self,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        outcome: MetricOutcome,
        error: impl Into<String>,
    ) {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        if let Some(snapshot) = inner.recoveries.iter_mut().find(|candidate| {
            candidate.trigger == trigger
                && candidate.action == action
                && candidate.outcome == outcome
        }) {
            snapshot.last_error = Some(error.into());
        }
    }
}

#[derive(Clone, Debug)]
pub struct BackpressureSnapshotHandle {
    inner: Arc<RwLock<SnapshotStoreInner>>,
}

impl BackpressureSnapshotHandle {
    fn new(inner: Arc<RwLock<SnapshotStoreInner>>) -> Self {
        Self { inner }
    }

    pub fn set(&self, snapshot: BackpressureSnapshot) {
        self.inner
            .write()
            .expect("snapshot store poisoned")
            .backpressure
            .insert(snapshot.route_key.clone(), snapshot);
    }

    pub fn set_route(&self, route_key: RouteKey, waiting: usize, in_flight: usize) {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        let snapshot = inner
            .backpressure
            .entry(route_key.clone())
            .or_insert_with(|| BackpressureSnapshot::new(route_key));
        snapshot.waiting = waiting;
        snapshot.in_flight = in_flight;
    }

    pub fn increment_rejected(&self, route_key: RouteKey) {
        self.update(route_key, |snapshot| {
            snapshot.rejected += 1;
        });
    }

    pub fn increment_timed_out(&self, route_key: RouteKey) {
        self.update(route_key, |snapshot| {
            snapshot.timed_out += 1;
        });
    }

    pub fn increment_canceled(&self, route_key: RouteKey) {
        self.update(route_key, |snapshot| {
            snapshot.canceled += 1;
        });
    }

    fn update<F>(&self, route_key: RouteKey, update: F)
    where
        F: FnOnce(&mut BackpressureSnapshot),
    {
        let mut inner = self.inner.write().expect("snapshot store poisoned");
        let snapshot = inner
            .backpressure
            .entry(route_key.clone())
            .or_insert_with(|| BackpressureSnapshot::new(route_key));
        update(snapshot);
    }
}

fn sort_by_key<T, F, K>(mut items: Vec<T>, key: F) -> Vec<T>
where
    F: FnMut(&T) -> K,
    K: Ord,
{
    items.sort_by_key(key);
    items
}

fn sort_by_route_key<T>(mut items: Vec<T>) -> Vec<T>
where
    T: RouteSnapshotView,
{
    items.sort_by_key(|snapshot| snapshot.route_sort_key());
    items
}

trait RouteSnapshotView {
    fn route_sort_key(&self) -> (String, String, Option<String>, String, String);
}

impl RouteSnapshotView for BackpressureSnapshot {
    fn route_sort_key(&self) -> (String, String, Option<String>, String, String) {
        route_sort_key(&self.route_key)
    }
}

impl RouteSnapshotView for RouteSnapshot {
    fn route_sort_key(&self) -> (String, String, Option<String>, String, String) {
        route_sort_key(&self.route_key)
    }
}

impl RouteSnapshotView for RoutePolicySnapshot {
    fn route_sort_key(&self) -> (String, String, Option<String>, String, String) {
        route_sort_key(&self.route_key)
    }
}

impl RouteSnapshotView for RouteCheckoutSnapshot {
    fn route_sort_key(&self) -> (String, String, Option<String>, String, String) {
        route_sort_key(&self.route_key)
    }
}

fn route_sort_key(route_key: &RouteKey) -> (String, String, Option<String>, String, String) {
    (
        route_key.database().to_owned(),
        route_key.user().to_owned(),
        route_key
            .application_name()
            .map(|application_name| application_name.to_owned()),
        route_key
            .client_addr()
            .map(|address| address.to_string())
            .unwrap_or_default(),
        route_key.query_class().to_string(),
    )
}

fn pin_reason_label(reason: PinReason) -> &'static str {
    match reason {
        PinReason::OpenTransaction => "open_transaction",
        PinReason::FailedTransaction => "failed_transaction",
        PinReason::SessionState => "session_state",
        PinReason::Copy => "copy",
        PinReason::ListenNotify => "listen_notify",
        PinReason::ExtendedQueryCycle => "extended_query_cycle",
        PinReason::UnknownProtocolState => "unknown_protocol_state",
    }
}

#[must_use]
pub fn pin_reason_sort_key(reason: PinReason) -> &'static str {
    pin_reason_label(reason)
}
