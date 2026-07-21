use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use bytes::{Buf, BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{RwLock, Semaphore},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::{rustls::ServerConfig, server::TlsStream};

use crate::routing::{
    choose_routing_target, ReadRoutingPlanner, ReplicaCandidate, RouteHealthSnapshot,
    RoutingContext, RoutingReason, RoutingTarget,
};
use crate::{
    adaptive::AdaptiveController,
    admin, auth,
    buffers::{ProxyBufferPool, SessionBufferSet},
    config::{Config, RouteConfig},
    drain::DrainController,
    health,
    lifecycle::{wait_for_shutdown_signal, LifecycleController, ShutdownCoordinator},
    metrics,
    mirror::{MirrorDispatcher, MirrorOutcomeRecorder, MirrorTask},
    pool::{
        BackendPool, BackendPoolRef, CheckoutMode as PoolCheckoutMode, PooledBackend,
        ReplicaSelectionStrategy, ReplicaSelector, RoutePools,
    },
    reload,
    snapshot::{
        ClientSnapshot, ClientSnapshotHandle, LimitsSnapshot, PinningSnapshot,
        PreparedSnapshotHandle, RecoverySnapshotHandle, RouteCheckoutSnapshot, SettingsSnapshot,
        SnapshotStore,
    },
    socket,
    telemetry::{self, DebugSample, DebugSampler, PhaseTimer},
    tls,
};
use pg_kinetic_core::routing::{
    BackendRole, FallbackPolicy, FreshnessPolicy, ReadRoutingMode,
    RoutingReason as CoreRoutingReason,
};
use pg_kinetic_core::{
    cleanup::{cleanup_action, CleanupAction},
    constants::{MetricName, PreparedEvent},
    lsn::{FreshnessStatus, PgLsn},
    observability::{MetricOutcome, ProtocolPhase},
    pin::PinnedBackend,
    policy::{
        PolicyAction, PolicyAuditEvent, PolicyAuditKind, PolicyDecision, PolicyMode,
        POLICY_DENY_SQLSTATE,
    },
    prepare::{InvalidationScope, PreparedCatalog},
    recovery::{recovery_action, RecoveryAction, RecoveryTrigger},
    route::{QueryClass, RouteKey},
    runtime::ShutdownReason,
    session::PinReason as SessionPinReason,
    session::TransactionState,
    shard_extract::{extract_shard_hint, ShardHint},
    sharding::{MultiShardPolicy, ShardId},
    sql::{classify, SetScope, SqlCommand},
    sql_classify::{analyze_sql, SqlAnalysis},
    virtual_session::{PinReason, ReadAfterWriteState, VirtualSession},
};
use pg_kinetic_wire::{
    backend::{build_error_response, parse_backend_frame, BackendFrame, ReadyStatus},
    error::WireError,
    frame::{parse_frontend_frame, FrontendFrame},
    message::{
        parse_bind_statement_name, parse_close_target, parse_describe_target, parse_parse_message,
        parse_simple_query, CloseTarget, DescribeTarget,
    },
    protocol::{BackendTag, FrontendTag, ReadyStatusByte},
    rewrite::{
        build_parse_frame, encode_frontend_frame, rewrite_bind_statement_name,
        rewrite_close_statement_name, rewrite_describe_statement_name,
        rewrite_parse_statement_name,
    },
    sqlstate::SqlState,
    startup::{parse_startup_packet, StartupPacket},
};
use std::borrow::Cow;

use crate::policy::{PolicyEvalInput, PolicyRuntime};

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
const CANNOT_CONNECT_NOW_SQLSTATE: &str = "57P03";
const CONNECTION_FAILURE_SQLSTATE: &str = "08006";
const REPLICA_UNAVAILABLE_MESSAGE: &str = "no healthy replica available";
const POOL_WARMUP_MIN_IDLE_BACKENDS: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendFailureKind {
    Connect,
    Read,
    Write,
    Authentication,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDisposition {
    Never,
    RetryBeforeResponse,
}

#[must_use]
pub const fn retry_disposition(
    kind: BackendFailureKind,
    response_started: bool,
    request_is_safe_to_replay: bool,
) -> RetryDisposition {
    if matches!(kind, BackendFailureKind::Read) && !response_started && request_is_safe_to_replay {
        RetryDisposition::RetryBeforeResponse
    } else {
        RetryDisposition::Never
    }
}

#[derive(Debug)]
struct BackendFailure {
    kind: BackendFailureKind,
    response_started: bool,
    source: anyhow::Error,
}

impl std::fmt::Display for BackendFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "backend {:?} failure: {}",
            self.kind, self.source
        )
    }
}

impl std::error::Error for BackendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn backend_failure(
    kind: BackendFailureKind,
    response_started: bool,
    source: impl Into<anyhow::Error>,
) -> anyhow::Error {
    BackendFailure {
        kind,
        response_started,
        source: source.into(),
    }
    .into()
}

#[derive(Debug)]
pub(crate) struct ClientConnection {
    inner: Option<ClientTransport>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum ClientTransport {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl ClientConnection {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self {
            inner: Some(ClientTransport::Plain(stream)),
        }
    }

    pub(crate) fn is_tls(&self) -> bool {
        matches!(self.inner, Some(ClientTransport::Tls(_)))
    }

    pub(crate) fn has_peer_certificates(&self) -> bool {
        match self.inner.as_ref().expect("client stream present") {
            ClientTransport::Plain(_) => false,
            ClientTransport::Tls(stream) => stream.get_ref().1.peer_certificates().is_some(),
        }
    }

    pub(crate) async fn read_buf(&mut self, buffer: &mut BytesMut) -> std::io::Result<usize> {
        match self.inner.as_mut().expect("client stream present") {
            ClientTransport::Plain(stream) => stream.read_buf(buffer).await,
            ClientTransport::Tls(stream) => stream.read_buf(buffer).await,
        }
    }

    pub(crate) async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self.inner.as_mut().expect("client stream present") {
            ClientTransport::Plain(stream) => stream.write_all(bytes).await,
            ClientTransport::Tls(stream) => stream.write_all(bytes).await,
        }
    }

    pub(crate) async fn shutdown(&mut self) -> std::io::Result<()> {
        match self.inner.as_mut().expect("client stream present") {
            ClientTransport::Plain(stream) => stream.shutdown().await,
            ClientTransport::Tls(stream) => stream.shutdown().await,
        }
    }

    pub(crate) async fn start_tls(
        &mut self,
        server_config: &Arc<ServerConfig>,
    ) -> anyhow::Result<()> {
        let plain = match self.inner.take().context("client stream missing")? {
            ClientTransport::Plain(stream) => stream,
            ClientTransport::Tls(stream) => {
                self.inner = Some(ClientTransport::Tls(stream));
                anyhow::bail!("client TLS is already active");
            }
        };

        let tls = tls::accept_client_tls(plain, server_config).await?;
        self.inner = Some(ClientTransport::Tls(tls));
        Ok(())
    }
}

#[derive(Debug)]
pub struct Proxy {
    config: Config,
    buffer_pool: ProxyBufferPool,
    client_slots: Arc<Semaphore>,
    lifecycle: LifecycleController,
    snapshot_store: SnapshotStore,
}

impl Proxy {
    #[must_use]
    pub fn new(config: Config) -> Self {
        let client_slots = Arc::new(Semaphore::new(config.capacity.max_clients));
        let drain = Arc::new(DrainController::new());
        let lifecycle = LifecycleController::new(
            drain,
            config.drain.drain_timeout(),
            config.runtime.lifecycle.shutdown_grace(),
            config.runtime.lifecycle.readiness_fail_during_drain,
        );
        let snapshot_store = SnapshotStore::new();

        Self {
            config,
            buffer_pool: ProxyBufferPool::default(),
            client_slots,
            lifecycle,
            snapshot_store,
        }
    }

    #[must_use]
    pub fn with_buffer_pool(config: Config, buffer_pool: ProxyBufferPool) -> Self {
        let mut proxy = Self::new(config);
        proxy.buffer_pool = buffer_pool;
        proxy
    }

    #[must_use]
    pub fn drain_controller(&self) -> Arc<DrainController> {
        self.lifecycle.drain_controller()
    }

    #[must_use]
    pub fn lifecycle_controller(&self) -> LifecycleController {
        self.lifecycle.clone()
    }

    #[must_use]
    pub fn snapshot_store(&self) -> SnapshotStore {
        self.snapshot_store.clone()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let effective_config = reload::load_effective_config(&self.config)?;
        reload::validate_runtime_assets(&effective_config)?;
        let backend_credentials =
            auth::load_backend_credentials(&effective_config.auth)?.map(Arc::new);
        self.lifecycle.configure(
            effective_config.drain.drain_timeout(),
            effective_config.runtime.lifecycle.shutdown_grace(),
            effective_config
                .runtime
                .lifecycle
                .readiness_fail_during_drain,
        );
        let phase_recorder =
            telemetry::phase_timing_recorder(effective_config.observability.metrics_addr.is_some());
        let debug_sampler =
            DebugSampler::new(effective_config.observability.trace_sampling_ratio());
        self.snapshot_store
            .set_settings_snapshot(SettingsSnapshot::from_config(&effective_config));
        self.snapshot_store
            .set_limits_snapshot(LimitsSnapshot::from_config(&effective_config));
        let active_config = Arc::new(RwLock::new(effective_config.clone()));
        let route_config = effective_config
            .effective_routes()
            .into_iter()
            .next()
            .context("missing effective route config")?;
        let mirror_outcome_recorder = MirrorOutcomeRecorder::default();
        let mirror_dispatcher = Arc::new(MirrorDispatcher::disabled(
            route_config.primary.address,
            effective_config.tls.clone(),
            effective_config.socket.clone(),
            mirror_outcome_recorder.clone(),
        ));
        let route_pools = Arc::new(build_route_pools(
            &effective_config,
            &route_config,
            self.snapshot_store.clone(),
        ));
        self.lifecycle.mark_backend_pools_initialized();
        let routing_planner = ReadRoutingPlanner::new(
            route_config.read_routing.read_routing_mode,
            route_config.read_routing.fallback_policy,
            route_config.freshness.freshness_policy,
            route_config.freshness.max_replica_lag_ms,
        );

        let listener = TcpListener::bind(effective_config.connection.listen_addr)
            .await
            .with_context(|| {
                format!("bind listener {}", effective_config.connection.listen_addr)
            })?;

        let drain = self.lifecycle.drain_controller();
        let _health_handle = if let Some(health_addr) = effective_config.health.health_addr {
            Some(
                health::spawn(
                    health_addr,
                    Arc::clone(&drain),
                    route_config.primary.address,
                    effective_config.tls.clone(),
                    effective_config.socket.clone(),
                    effective_config.health.readiness_timeout(),
                    effective_config.health.readiness_backend_check_interval(),
                )
                .await?,
            )
        } else {
            None
        };
        let _admin_handle = if let Some(admin_addr) = effective_config.admin.admin_addr {
            Some(
                admin::spawn(
                    admin_addr,
                    effective_config.clone(),
                    Arc::clone(&drain),
                    self.snapshot_store.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        self.lifecycle.mark_listeners_initialized();

        if effective_config.reload.reload_enabled && effective_config.reload.config_file.is_some() {
            let base_config = self.config.clone();
            let reload_config = effective_config.reload.clone();
            let active_config = Arc::clone(&active_config);
            tokio::spawn(async move {
                reload::spawn_reload_loop(base_config, reload_config, active_config).await;
            });
        }

        if effective_config.runtime.production.adaptive_enabled {
            let controller = AdaptiveController::new(
                self.snapshot_store.clone(),
                mirror_outcome_recorder.clone(),
                Arc::clone(&active_config),
            );
            tokio::spawn(async move {
                controller.run().await;
            });
        }

        tracing::info!(listen_addr = %effective_config.connection.listen_addr, "listening");

        let mut shutdown = Box::pin(wait_for_shutdown_signal());
        let mut drain_start_wait = Box::pin(drain.wait_for_drain_start());
        let mut client_tasks = JoinSet::new();
        let mut draining = false;

        loop {
            if draining {
                let shutdown_coordinator = ShutdownCoordinator::new(self.lifecycle.clone());
                let coordinator = shutdown_coordinator.clone();
                let mut shutdown_completion =
                    Box::pin(async move { coordinator.coordinate().await });
                loop {
                    tokio::select! {
                        biased;
                        outcome = &mut shutdown_completion => {
                            if outcome.forced_sessions() > 0 {
                                tracing::warn!(
                                    active_clients = outcome.forced_sessions(),
                                    "shutdown grace expired; force-closing client sessions"
                                );
                                client_tasks.abort_all();
                                while let Some(result) = client_tasks.join_next().await {
                                    if let Err(error) = result {
                                        if !error.is_cancelled() {
                                            tracing::warn!(error = %error, "client task failed during shutdown");
                                        }
                                    }
                                }
                            } else {
                                tracing::info!(active_clients = drain.active_clients(), "drain completed");
                            }
                            shutdown_coordinator.complete();
                            return Ok(());
                        }
                        joined = client_tasks.join_next(), if !client_tasks.is_empty() => {
                            if let Some(Err(error)) = joined {
                                tracing::warn!(error = %error, "client task failed");
                            }
                        }
                        accept = listener.accept() => {
                            let (client, client_addr) = accept.context("accept draining client")?;
                            let config_snapshot = active_config.read().await.clone();
                            let socket_options = socket::SocketOptions::from(&config_snapshot.socket);
                            socket::apply_socket_options(&client, &socket_options, "client")
                                .context("apply draining client socket options")?;
                            let mut client = ClientConnection::new(client);
                            metrics::increment_client_connections();
                            reject_client_during_drain(&mut client, phase_recorder.as_ref()).await?;
                            tracing::info!(%client_addr, "rejected client during drain");
                        }
                    }
                }
            }

            tokio::select! {
                biased;
                result = &mut shutdown => {
                    let reason = result.context("wait for shutdown signal")?;
                    if self.lifecycle.begin_drain(reason) {
                        tracing::info!("received shutdown signal; beginning drain");
                    }
                    draining = true;
                }
                _ = &mut drain_start_wait => {
                    self.lifecycle.begin_drain(ShutdownReason::AdminRequest);
                    draining = true;
                }
                joined = client_tasks.join_next(), if !client_tasks.is_empty() => {
                    if let Some(Err(error)) = joined {
                        tracing::warn!(error = %error, "client task failed");
                    }
                }
                accept = listener.accept() => {
                    let (client, client_addr) = accept.context("accept client")?;
                    let config_snapshot = active_config.read().await.clone();
                    let socket_options = socket::SocketOptions::from(&config_snapshot.socket);
                    socket::apply_socket_options(&client, &socket_options, "client")
                        .context("apply client socket options")?;
                    let client = ClientConnection::new(client);
                    metrics::increment_client_connections();

                    let Some(client_guard) = self.lifecycle.drain_token().try_enter() else {
                        let mut client = client;
                        reject_client_during_drain(&mut client, phase_recorder.as_ref()).await?;
                        tracing::info!(%client_addr, "rejected client during drain");
                        continue;
                    };

                    let permit = self.client_slots.clone().acquire_owned().await?;
                    let route_pools = Arc::clone(&route_pools);
                    let snapshot_store = self.snapshot_store.clone();
                    let client_snapshot_handle = snapshot_store.client_handle();
                    let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
                    client_snapshot_handle.register(session_id);
                    telemetry::emit_debug_sample_with(
                        &debug_sampler,
                        session_id,
                        || DebugSample::client_accepted(
                            session_id,
                            client_addr,
                            config_snapshot.tls.client_tls_mode.as_str(),
                            client.has_peer_certificates(),
                        ),
                    );
                    let phase_recorder = Arc::clone(&phase_recorder);

                    let mirror_dispatcher = Arc::clone(&mirror_dispatcher);
                    let buffer_pool = self.buffer_pool.clone();
                    let backend_credentials = backend_credentials.clone();

                    client_tasks.spawn(async move {
                        let _client_guard = client_guard;
                        let result = handle_client(
                            client,
                            client_addr,
                            route_pools,
                            config_snapshot,
                            routing_planner,
                            session_id,
                            snapshot_store,
                            client_snapshot_handle,
                            phase_recorder,
                            debug_sampler,
                            mirror_dispatcher,
                            buffer_pool,
                            backend_credentials,
                        )
                        .await;
                        drop(permit);

                        if let Err(error) = result {
                            let error_chain = format!("{error:#}");
                            tracing::warn!(%client_addr, error = %error_chain, "client connection closed with error");
                        }
                    });
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_client(
    mut client: ClientConnection,
    client_addr: SocketAddr,
    route_pools: Arc<RoutePools>,
    config: Config,
    routing_planner: ReadRoutingPlanner,
    session_id: u64,
    snapshot_store: SnapshotStore,
    client_snapshot_handle: ClientSnapshotHandle,
    phase_recorder: Arc<dyn telemetry::PhaseTimingRecorder>,
    debug_sampler: DebugSampler,
    mirror_dispatcher: Arc<MirrorDispatcher>,
    buffer_pool: ProxyBufferPool,
    backend_credentials: Option<Arc<auth::BackendCredentials>>,
) -> anyhow::Result<()> {
    let mut session_buffer_lease = buffer_pool.acquire();
    let session_buffers = session_buffer_lease.buffers_mut();
    let session_started = Instant::now();
    let _client_snapshot_guard = ClientSnapshotGuard::new(
        client_snapshot_handle.clone(),
        session_id,
        client_addr,
        session_started,
        debug_sampler,
    );
    let startup_timer = PhaseTimer::start(ProtocolPhase::Startup, phase_recorder.as_ref());
    let client_tls_mode = config.tls.client_tls_mode;
    let client_tls_server_config = reload::load_client_tls_server_config(&config)?;
    let auth_users = reload::load_auth_users(&config)?;
    let auth = config.auth.clone();
    let performance = config.performance.clone();
    let qos = config.qos.clone();
    let route_config = config
        .effective_routes()
        .into_iter()
        .next()
        .context("missing effective route config")?;
    let route_read_routing_mode = route_config.read_routing.read_routing_mode;
    let route_fallback_policy = route_config.read_routing.fallback_policy;
    let read_after_write_timeout =
        Duration::from_millis(route_config.freshness.read_after_write_timeout_ms);
    let read_after_write_protection_enabled = matches!(
        route_config.freshness.freshness_policy,
        FreshnessPolicy::SessionWriteLsn | FreshnessPolicy::SessionWriteLsnAndMaxLag
    );
    let prepared_snapshot_handle = snapshot_store.prepared_handle();
    let recovery_snapshot_handle = snapshot_store.recovery_handle();

    let mut session = VirtualSession::default();
    let mut pinned_backend = PinnedBackend::default();
    let mut prepared = PreparedCatalog::new(session_id);
    let mut held_backend: Option<PooledBackend> = None;
    let mut wait_for_client_activity_after_timeout = false;
    let mut mirror_query_id = 0_u64;

    let startup_packet = match read_startup_packet_with_buffer(
        &mut client,
        client_tls_mode,
        client_tls_server_config.as_ref(),
        qos.idle_client_timeout(),
        qos.max_client_buffer_bytes,
        session_buffers.client_read_mut(),
        phase_recorder.as_ref(),
    )
    .await
    .with_context(|| format!("proxy client {client_addr}"))
    {
        Ok(StartupRead::Packet(packet)) => packet,
        Ok(StartupRead::ClientClosed) => {
            startup_timer.finish(MetricOutcome::Canceled);
            return Ok(());
        }
        Ok(StartupRead::TimedOut) => {
            startup_timer.finish(MetricOutcome::Timeout);
            error_response_and_ready_with_state(
                &mut client,
                SqlState::OperatorIntervention.as_str(),
                "startup timed out",
                ReadyStatus::Idle,
            )
            .await?;
            return Ok(());
        }
        Ok(StartupRead::BufferLimitExceeded) => {
            startup_timer.finish(MetricOutcome::Discarded);
            record_buffer_limit(BufferBudgetKind::Client);
            return Ok(());
        }
        Err(error) => {
            startup_timer.finish(MetricOutcome::Error);
            return Err(error);
        }
    };
    session_buffers.observe_client_read();

    let (route_database, route_user, mut route_application_name) =
        startup_route_key(&startup_packet)?;
    let mut session_route = route_key(
        &route_database,
        &route_user,
        route_application_name.as_deref(),
        client_addr,
    );
    update_client_snapshot(
        &client_snapshot_handle,
        session_id,
        route_database.clone(),
        route_user.clone(),
        route_application_name.clone(),
        session_route.clone(),
        "startup",
        session_started.elapsed(),
    );

    if !matches!(auth.auth_mode, crate::config::AuthMode::PassThrough) {
        let auth_timer = PhaseTimer::start(ProtocolPhase::Auth, phase_recorder.as_ref());
        let auth_users = match auth_users.as_deref().context("auth user store unavailable") {
            Ok(users) => users,
            Err(error) => {
                auth_timer.finish(MetricOutcome::Error);
                startup_timer.finish(MetricOutcome::Error);
                return Err(error);
            }
        };
        match auth::authenticate_client(
            &mut client,
            &route_user,
            &auth,
            auth_users,
            qos.max_client_buffer_bytes,
        )
        .await
        .with_context(|| format!("authenticate client {client_addr}"))
        {
            Ok(auth::ClientAuthOutcome::PassThrough)
            | Ok(auth::ClientAuthOutcome::Authenticated) => {
                auth_timer.finish(MetricOutcome::Ok);
            }
            Ok(auth::ClientAuthOutcome::Rejected) => {
                auth_timer.finish(MetricOutcome::Rejected);
                startup_timer.finish(MetricOutcome::Rejected);
                return Ok(());
            }
            Err(error) => {
                auth_timer.finish(MetricOutcome::Error);
                startup_timer.finish(MetricOutcome::Error);
                return Err(error);
            }
        }
    }

    let backend_startup_packet = rewrite_backend_startup_user(
        &startup_packet,
        backend_credentials
            .as_deref()
            .map(auth::BackendCredentials::username),
    )?;

    let mut backend = match checkout_backend(CheckoutBackendRequest {
        route_pools: &route_pools,
        route: session_route.clone(),
        target: RoutingTarget::Primary {
            reason: RoutingReason::Off,
        },
        context: "checkout backend for startup",
        mode: if matches!(auth.auth_mode, crate::config::AuthMode::PassThrough) {
            CheckoutMode::PreferConnect
        } else {
            CheckoutMode::AllowConnect
        },
        session_id,
        debug_sampler,
        phase_recorder: phase_recorder.as_ref(),
        snapshot_store: &snapshot_store,
        startup_packet: &backend_startup_packet,
        backend_credentials: backend_credentials.as_deref(),
        read_after_write_state: ReadAfterWriteState::Disabled,
        record_snapshot: false,
        bootstrap_backend: false,
    })
    .await
    {
        Ok(backend) => backend,
        Err(CheckoutFailure::Overload(message)) => {
            startup_timer.finish(MetricOutcome::Rejected);
            error_response_and_ready(&mut client, &qos, message).await?;
            return Ok(());
        }
        Err(CheckoutFailure::Postgres { sqlstate, message }) => {
            startup_timer.finish(MetricOutcome::Rejected);
            error_response_and_ready_with_state(&mut client, sqlstate, message, ReadyStatus::Idle)
                .await?;
            return Ok(());
        }
        Err(CheckoutFailure::Close) => {
            startup_timer.finish(MetricOutcome::Canceled);
            return Ok(());
        }
        Err(CheckoutFailure::Fatal(error)) => {
            startup_timer.finish(MetricOutcome::Error);
            return Err(error);
        }
    };
    if let Err(error) = proxy_startup(
        &mut client,
        &mut backend,
        &backend_startup_packet,
        qos.max_client_buffer_bytes,
        qos.max_backend_buffer_bytes,
        matches!(auth.auth_mode, crate::config::AuthMode::PassThrough),
        matches!(auth.auth_mode, crate::config::AuthMode::PassThrough),
        backend_credentials.as_deref(),
        session_buffers,
        phase_recorder.as_ref(),
    )
    .await
    {
        startup_timer.finish(if buffer_limit_kind(&error).is_some() {
            MetricOutcome::Discarded
        } else {
            MetricOutcome::Error
        });
        if buffer_limit_kind(&error).is_some() {
            backend.discard();
            return Ok(());
        }

        backend.discard();
        return Err(error).with_context(|| format!("proxy client {client_addr}"));
    }
    backend.release().await;
    schedule_service_backend_pool_warmup(
        Arc::clone(&route_pools),
        session_route.clone(),
        backend_startup_packet.to_vec(),
        backend_credentials.clone(),
        snapshot_store.clone(),
        Arc::clone(&phase_recorder),
        debug_sampler,
        session_id,
    );
    startup_timer.finish(MetricOutcome::Ok);
    telemetry::emit_debug_sample_with(&debug_sampler, session_id, || {
        DebugSample::startup_complete(
            session_id,
            session_route.clone(),
            auth.auth_mode.as_str(),
            client_tls_mode.as_str(),
            MetricOutcome::Ok,
        )
    });
    update_client_snapshot(
        &client_snapshot_handle,
        session_id,
        route_database.clone(),
        route_user.clone(),
        route_application_name.clone(),
        session_route.clone(),
        "active",
        session_started.elapsed(),
    );

    loop {
        let idle_timeout_kind = if matches!(
            session.pin_reason(),
            Some(PinReason::OpenTransaction) | Some(PinReason::FailedTransaction)
        ) {
            IdleTimeoutKind::Transaction
        } else {
            IdleTimeoutKind::Client
        };
        let idle_timeout = if idle_timeout_kind == IdleTimeoutKind::Transaction {
            qos.idle_transaction_timeout()
        } else {
            qos.idle_client_timeout()
        };
        let cycle_timeout = if wait_for_client_activity_after_timeout {
            None
        } else {
            Some(idle_timeout)
        };

        let Some(cycle) = next_client_cycle(
            &mut client,
            session_buffers.client_read_mut(),
            cycle_timeout,
            idle_timeout_kind,
            qos.max_client_buffer_bytes,
        )
        .await?
        else {
            continue;
        };
        session_buffers.observe_client_read();
        session_buffers.trim_empty_buffers();

        wait_for_client_activity_after_timeout = false;
        match cycle {
            ClientCycle::Frames(frames) => {
                let current_query_id = mirror_query_id;
                mirror_query_id = mirror_query_id.wrapping_add(1);
                let full_routing_analysis = route_read_routing_mode != ReadRoutingMode::Off;
                let request_plans =
                    request_plans_for_frames(&prepared, &frames, full_routing_analysis)
                        .context("build request plan before backend checkout")?;
                let committed_write_transaction = update_transaction_state_from_request_plans(
                    &mut session,
                    &request_plans,
                    full_routing_analysis,
                )
                .context("update transaction state before backend checkout")?;
                let route = session_route.clone();
                let selection = ReadRoutingSelection {
                    planner: &routing_planner,
                    route_pools: &route_pools,
                    snapshot_store: &snapshot_store,
                    read_routing_mode: route_read_routing_mode,
                    fallback_policy: route_fallback_policy,
                    session: &session,
                    request_plan: request_plans.first(),
                };
                let base_checkout_target = if full_routing_analysis {
                    select_checkout_target(&selection)
                } else {
                    RoutingTarget::Primary {
                        reason: RoutingReason::Off,
                    }
                };
                let mirror_route_target = base_checkout_target.clone();
                let backend_reused = held_backend.is_some();
                let checkout_target = if backend_reused {
                    base_checkout_target.clone()
                } else if matches!(
                    base_checkout_target,
                    RoutingTarget::Wait {
                        reason: RoutingReason::FallbackWait,
                    }
                ) {
                    wait_for_checkout_target(&route, &selection, read_after_write_timeout).await
                } else {
                    base_checkout_target.clone()
                };
                let retry_target = checkout_target.clone();
                let mut backend = if let Some(backend) = held_backend.take() {
                    backend
                } else {
                    match checkout_backend(CheckoutBackendRequest {
                        route_pools: &route_pools,
                        route: route.clone(),
                        target: checkout_target,
                        context: "checkout backend for cycle",
                        mode: CheckoutMode::AllowConnect,
                        session_id,
                        debug_sampler,
                        phase_recorder: phase_recorder.as_ref(),
                        snapshot_store: &snapshot_store,
                        startup_packet: &backend_startup_packet,
                        backend_credentials: backend_credentials.as_deref(),
                        read_after_write_state: session.read_after_write_state(),
                        record_snapshot: true,
                        bootstrap_backend: true,
                    })
                    .await
                    {
                        Ok(backend) => backend,
                        Err(CheckoutFailure::Overload(message)) => {
                            error_response_and_ready(&mut client, &qos, message).await?;
                            return Ok(());
                        }
                        Err(CheckoutFailure::Postgres { sqlstate, message }) => {
                            error_response_and_ready_with_state(
                                &mut client,
                                sqlstate,
                                message,
                                ReadyStatus::Idle,
                            )
                            .await?;
                            return Ok(());
                        }
                        Err(CheckoutFailure::Close) => {
                            return Ok(());
                        }
                        Err(CheckoutFailure::Fatal(error)) => return Err(error),
                    }
                };

                let replay =
                    if should_replay_session(&session, &pinned_backend, backend.backend_id()) {
                        Some(replay_frames(&session))
                    } else {
                        None
                    };

                if let Some(replay_frames) = replay.as_ref() {
                    let status = execute_backend_batch(
                        &mut backend,
                        replay_frames,
                        qos.max_backend_buffer_bytes,
                    )
                    .await
                    .context("replay virtual session")?;
                    anyhow::ensure!(
                        status == ReadyStatus::Idle,
                        "unexpected replay status: {status:?}"
                    );
                }

                if mirror_dispatcher.classifier().mode().is_enabled() {
                    let mirror_task = MirrorTask::new(
                        session_id,
                        current_query_id,
                        route.clone(),
                        mirror_route_target,
                        mirror_sql_command_for_request_plan(request_plans.first()),
                        backend_startup_packet.clone().freeze(),
                        replay.clone().unwrap_or_default(),
                        frames.clone(),
                        session.pin_reason(),
                    );
                    let _ = mirror_dispatcher.dispatch(mirror_task);
                }

                let simple_query_commands: Vec<SqlCommand> = request_plans
                    .iter()
                    .filter(|plan| plan.updates_session_state)
                    .map(|plan| plan.command.clone())
                    .collect();
                let request_is_safe_to_replay =
                    safe_request_to_replay(&frames, &request_plans, &session);
                drop(request_plans);
                let mut retry_attempted = false;
                let (result, progress) = loop {
                    let mut progress = QueryProgress::default();
                    let mut state = ForwardCycleState {
                        session: &mut session,
                        prepared: &mut prepared,
                        prepared_snapshot_handle: prepared_snapshot_handle.clone(),
                        route_application_name: &mut route_application_name,
                        progress: &mut progress,
                    };
                    let result = timeout(
                        qos.query_timeout(),
                        forward_message_cycle(
                            &mut client,
                            &mut backend,
                            &mut state,
                            &frames,
                            &simple_query_commands,
                            qos.max_backend_buffer_bytes,
                            session_buffers,
                            phase_recorder.as_ref(),
                        ),
                    )
                    .await;
                    let retry = match &result {
                        Ok(Err(error)) => error
                            .downcast_ref::<BackendFailure>()
                            .map(|failure| {
                                !retry_attempted
                                    && retry_disposition(
                                        failure.kind,
                                        failure.response_started,
                                        request_is_safe_to_replay,
                                    ) == RetryDisposition::RetryBeforeResponse
                            })
                            .unwrap_or(false),
                        _ => false,
                    };
                    if !retry {
                        break (result, progress);
                    }

                    backend.mark_failed();
                    backend.discard();
                    let Ok(replacement) = checkout_backend(CheckoutBackendRequest {
                        route_pools: &route_pools,
                        route: route.clone(),
                        target: retry_target.clone(),
                        context: "checkout backend for failure retry",
                        mode: CheckoutMode::AllowConnect,
                        session_id,
                        debug_sampler,
                        phase_recorder: phase_recorder.as_ref(),
                        snapshot_store: &snapshot_store,
                        startup_packet: &backend_startup_packet,
                        backend_credentials: backend_credentials.as_deref(),
                        read_after_write_state: session.read_after_write_state(),
                        record_snapshot: false,
                        bootstrap_backend: true,
                    })
                    .await
                    else {
                        return Ok(());
                    };
                    backend = replacement;
                    retry_attempted = true;
                };

                let client_disconnected_after_ready = matches!(
                    &result,
                    Ok(Ok(ForwardOutcome::ClientDisconnectedAfterReady(_)))
                );

                match result {
                    Ok(Ok(ForwardOutcome::Ready(status)))
                    | Ok(Ok(ForwardOutcome::ClientDisconnectedAfterReady(status))) => {
                        session_route =
                            session_route.with_application_name(route_application_name.as_deref());
                        if committed_write_transaction
                            && read_after_write_protection_enabled
                            && status == ReadyStatus::Idle
                        {
                            let freshness_outcome = probe_read_after_write_requirement(
                                &mut backend,
                                read_after_write_timeout,
                                qos.max_backend_buffer_bytes,
                            )
                            .await;
                            match freshness_outcome {
                                Ok(lsn) => session.set_read_after_write_required(lsn),
                                Err(_) => session.set_read_after_write_unknown(),
                            }
                        }

                        telemetry::emit_debug_sample_with(&debug_sampler, session_id, || {
                            DebugSample::query_complete(
                                session_id,
                                session_route.clone(),
                                MetricOutcome::Ok,
                                0,
                                match status {
                                    ReadyStatus::Idle => "idle",
                                    ReadyStatus::InTransaction => "in_transaction",
                                    ReadyStatus::FailedTransaction => "failed_transaction",
                                },
                                None,
                                &[],
                            )
                        });
                        session.mark_ready_after_copy();
                        let action = cleanup_action(&session, status);
                        metrics::increment_cleanup(action);

                        match action {
                            CleanupAction::Reuse => {
                                clear_pinned_backend(
                                    &mut pinned_backend,
                                    &snapshot_store,
                                    session_id,
                                );
                                backend.release().await;
                            }
                            CleanupAction::ResetThenReuse => {
                                let reset_timer = PhaseTimer::start(
                                    ProtocolPhase::Reset,
                                    phase_recorder.as_ref(),
                                );
                                execute_simple_query(
                                    &mut backend,
                                    route_pools.primary().reset_query(),
                                    qos.max_backend_buffer_bytes,
                                )
                                .await
                                .context("reset backend before reuse")?;
                                reset_timer.finish(MetricOutcome::Ok);
                                clear_pinned_backend(
                                    &mut pinned_backend,
                                    &snapshot_store,
                                    session_id,
                                );
                                backend.release().await;
                            }
                            CleanupAction::KeepPinned => {
                                if let Some(reason) = session.pin_reason() {
                                    metrics::increment_pin(reason);
                                    telemetry::emit_debug_sample_with(
                                        &debug_sampler,
                                        session_id,
                                        || {
                                            DebugSample::pinning(
                                                session_id,
                                                session_route.clone(),
                                                reason,
                                                backend.backend_id(),
                                                session_started.elapsed(),
                                            )
                                        },
                                    );
                                    record_pinning_snapshot(
                                        &snapshot_store,
                                        session_id,
                                        backend.backend_id(),
                                        reason,
                                        session_route.clone(),
                                        session_started.elapsed(),
                                    );
                                }
                                pinned_backend.mark_pinned(backend.backend_id());
                                held_backend = Some(backend);
                            }
                            CleanupAction::RollbackThenReuse => {
                                execute_simple_query(
                                    &mut backend,
                                    "ROLLBACK",
                                    qos.max_backend_buffer_bytes,
                                )
                                .await
                                .context("rollback failed transaction")?;
                                session.apply_sql(classify("rollback"));
                                clear_pinned_backend(
                                    &mut pinned_backend,
                                    &snapshot_store,
                                    session_id,
                                );
                                backend.release().await;
                            }
                            CleanupAction::Discard => {
                                clear_pinned_backend(
                                    &mut pinned_backend,
                                    &snapshot_store,
                                    session_id,
                                );
                                backend.discard();
                            }
                        }

                        if client_disconnected_after_ready {
                            return Ok(());
                        }
                    }
                    Ok(Ok(ForwardOutcome::AbandonedResponse { needs_sync })) => {
                        let reused = recover_backend(
                            &mut backend,
                            session_route.clone(),
                            session_id,
                            debug_sampler,
                            RecoveryTrigger::AbandonedResponse,
                            &performance,
                            needs_sync,
                            &mut session,
                            qos.max_backend_buffer_bytes,
                            &recovery_snapshot_handle,
                        )
                        .await
                        .context("recover abandoned response")?;
                        clear_pinned_backend(&mut pinned_backend, &snapshot_store, session_id);
                        if reused {
                            backend.release().await;
                        } else {
                            backend.discard();
                        }
                        return Ok(());
                    }
                    Ok(Ok(ForwardOutcome::BufferLimitExceeded)) => {
                        clear_pinned_backend(&mut pinned_backend, &snapshot_store, session_id);
                        backend.discard();
                        return Ok(());
                    }
                    Ok(Err(error)) => {
                        if let Some(kind) = buffer_limit_kind(&error) {
                            record_buffer_limit(kind);
                            clear_pinned_backend(&mut pinned_backend, &snapshot_store, session_id);
                            backend.discard();
                            return Ok(());
                        }

                        clear_pinned_backend(&mut pinned_backend, &snapshot_store, session_id);
                        if let Some(failure) = error.downcast_ref::<BackendFailure>() {
                            backend.mark_failed();
                            if !failure.response_started {
                                error_response_and_ready_with_state(
                                    &mut client,
                                    CONNECTION_FAILURE_SQLSTATE,
                                    "backend connection failed before response",
                                    ReadyStatus::Idle,
                                )
                                .await?;
                            }
                        }
                        backend.discard();
                        if error.downcast_ref::<BackendFailure>().is_some() {
                            return Ok(());
                        }
                        return Err(error).with_context(|| format!("proxy client {client_addr}"));
                    }
                    Err(_) => {
                        let continue_client = handle_query_timeout(
                            &mut client,
                            &performance,
                            backend,
                            &mut session,
                            &mut pinned_backend,
                            &snapshot_store,
                            session_id,
                            session_route.clone(),
                            &recovery_snapshot_handle,
                            progress,
                            qos.max_backend_buffer_bytes,
                            phase_recorder.as_ref(),
                            debug_sampler,
                        )
                        .await?;

                        if !continue_client {
                            return Ok(());
                        }

                        wait_for_client_activity_after_timeout = true;
                    }
                }
            }
            ClientCycle::Terminate => {
                if let Some(backend) = held_backend.take() {
                    finalize_backend_on_disconnect(
                        backend,
                        &route_pools,
                        &performance,
                        &mut session,
                        &mut pinned_backend,
                        &snapshot_store,
                        session_id,
                        session_route.clone(),
                        &recovery_snapshot_handle,
                        &qos,
                        phase_recorder.as_ref(),
                        debug_sampler,
                    )
                    .await?;
                }
                return Ok(());
            }
            ClientCycle::IdleTimeout(kind) => {
                if let Some(backend) = held_backend.take() {
                    finalize_backend_on_disconnect(
                        backend,
                        &route_pools,
                        &performance,
                        &mut session,
                        &mut pinned_backend,
                        &snapshot_store,
                        session_id,
                        session_route.clone(),
                        &recovery_snapshot_handle,
                        &qos,
                        phase_recorder.as_ref(),
                        debug_sampler,
                    )
                    .await?;
                }

                session_buffers.client_read_mut().clear();
                session_buffers.trim_empty_buffers();
                handle_idle_timeout(&mut client, kind).await?;
                if kind == IdleTimeoutKind::Transaction {
                    return Ok(());
                }

                wait_for_client_activity_after_timeout = true;
            }
            ClientCycle::BufferLimitExceeded => {
                record_buffer_limit(BufferBudgetKind::Client);
                if let Some(backend) = held_backend.take() {
                    finalize_backend_on_disconnect(
                        backend,
                        &route_pools,
                        &performance,
                        &mut session,
                        &mut pinned_backend,
                        &snapshot_store,
                        session_id,
                        session_route.clone(),
                        &recovery_snapshot_handle,
                        &qos,
                        phase_recorder.as_ref(),
                        debug_sampler,
                    )
                    .await?;
                }

                return Ok(());
            }
        }
    }
}

struct CheckoutBackendRequest<'a> {
    route_pools: &'a Arc<RoutePools>,
    route: RouteKey,
    target: RoutingTarget,
    context: &'static str,
    mode: CheckoutMode,
    session_id: u64,
    debug_sampler: DebugSampler,
    phase_recorder: &'a dyn telemetry::PhaseTimingRecorder,
    snapshot_store: &'a SnapshotStore,
    startup_packet: &'a [u8],
    backend_credentials: Option<&'a auth::BackendCredentials>,
    read_after_write_state: ReadAfterWriteState,
    record_snapshot: bool,
    bootstrap_backend: bool,
}

async fn checkout_backend(
    request: CheckoutBackendRequest<'_>,
) -> Result<PooledBackend, CheckoutFailure> {
    let timer = PhaseTimer::start(ProtocolPhase::BackendCheckout, request.phase_recorder);
    let started = Instant::now();
    if request.record_snapshot {
        let checkout_snapshot = route_checkout_snapshot_for_target(
            request.route.clone(),
            request.target.clone(),
            request.read_after_write_state,
        );
        let target_role = checkout_snapshot
            .decision
            .clone()
            .target_role()
            .map(|role| role.as_str());
        let target_reason = checkout_snapshot.decision.clone().reason();
        let freshness_outcome = checkout_snapshot.freshness_outcome;
        if let Some(freshness_outcome) = freshness_outcome {
            metrics::record_read_after_write(freshness_outcome);
        }
        request
            .snapshot_store
            .set_route_checkout_snapshot(checkout_snapshot);
        tracing::debug!(
            route_key = ?request.route,
            checkout_mode = %checkout_mode_label(request.mode),
            target_role = ?target_role,
            reason = %target_reason.as_str(),
            "route checkout decision"
        );
    }

    let pool_mode = match request.mode {
        CheckoutMode::AllowConnect => PoolCheckoutMode::AllowConnect,
        CheckoutMode::PreferConnect => PoolCheckoutMode::PreferConnect,
    };
    if matches!(request.target, RoutingTarget::Wait { .. }) {
        telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
            DebugSample::overload_rejected(
                request.session_id,
                request.route.clone(),
                checkout_mode_label(request.mode),
            )
        });
        timer.finish(MetricOutcome::Rejected);
        return Err(CheckoutFailure::Overload(
            "backend checkout is waiting for a replica",
        ));
    }
    if matches!(request.target, RoutingTarget::Reject { .. }) {
        let (sqlstate, message) = checkout_postgres_error_for_target(&request.target)
            .unwrap_or((CANNOT_CONNECT_NOW_SQLSTATE, REPLICA_UNAVAILABLE_MESSAGE));
        telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
            DebugSample::overload_rejected(
                request.session_id,
                request.route.clone(),
                checkout_mode_label(request.mode),
            )
        });
        timer.finish(MetricOutcome::Rejected);
        return Err(CheckoutFailure::Postgres { sqlstate, message });
    }

    let backend_result = request
        .route_pools
        .checkout_target(request.route.clone(), &request.target, pool_mode)
        .await;
    let mut backend = match backend_result {
        Ok(backend) => backend,
        Err(crate::pool::PoolError::Backpressure(
            pg_kinetic_core::backpressure::BackpressureError::QueueFull,
        )) => {
            telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
                DebugSample::overload_rejected(
                    request.session_id,
                    request.route.clone(),
                    checkout_mode_label(request.mode),
                )
            });
            timer.finish(MetricOutcome::Rejected);
            return Err(CheckoutFailure::Overload("backend checkout queue is full"));
        }
        Err(crate::pool::PoolError::Backpressure(
            pg_kinetic_core::backpressure::BackpressureError::Timeout,
        )) => {
            telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
                DebugSample::overload_rejected(
                    request.session_id,
                    request.route.clone(),
                    checkout_mode_label(request.mode),
                )
            });
            timer.finish(MetricOutcome::Timeout);
            return Err(CheckoutFailure::Overload("backend checkout timed out"));
        }
        Err(crate::pool::PoolError::Backpressure(
            pg_kinetic_core::backpressure::BackpressureError::Closed,
        )) => {
            telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
                DebugSample::backend_checkout(
                    request.session_id,
                    request.route.clone(),
                    checkout_mode_label(request.mode),
                    MetricOutcome::Canceled,
                    started.elapsed(),
                )
            });
            timer.finish(MetricOutcome::Canceled);
            return Err(CheckoutFailure::Close);
        }
        Err(crate::pool::PoolError::Connect(error)) => {
            telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
                DebugSample::backend_checkout(
                    request.session_id,
                    request.route.clone(),
                    checkout_mode_label(request.mode),
                    MetricOutcome::Error,
                    started.elapsed(),
                )
            });
            timer.finish(MetricOutcome::Error);
            return Err(CheckoutFailure::Fatal(error.context(request.context)));
        }
    };

    if request.bootstrap_backend && backend.requires_startup() {
        bootstrap_backend(
            &mut backend,
            request.startup_packet,
            request.backend_credentials,
        )
        .await
        .map_err(CheckoutFailure::Fatal)?;
    }
    metrics::record_pool_checkout(started.elapsed().as_secs_f64() * 1000.0, "request", "ok");
    telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
        DebugSample::backend_checkout(
            request.session_id,
            request.route.clone(),
            checkout_mode_label(request.mode),
            MetricOutcome::Ok,
            started.elapsed(),
        )
    });
    timer.finish(MetricOutcome::Ok);
    Ok(backend)
}

fn schedule_service_backend_pool_warmup(
    route_pools: Arc<RoutePools>,
    route: RouteKey,
    startup_packet: Vec<u8>,
    backend_credentials: Option<Arc<auth::BackendCredentials>>,
    snapshot_store: SnapshotStore,
    phase_recorder: Arc<dyn telemetry::PhaseTimingRecorder>,
    debug_sampler: DebugSampler,
    session_id: u64,
) {
    let Some(backend_credentials) = backend_credentials else {
        return;
    };
    let target = RoutingTarget::Primary {
        reason: RoutingReason::Off,
    };
    let Some(pool) = route_pools.pool_for_target(&target) else {
        return;
    };
    let desired_idle_backends = POOL_WARMUP_MIN_IDLE_BACKENDS.min(pool.max_backends());
    if pool.idle_backends() >= desired_idle_backends {
        return;
    }

    tokio::spawn(async move {
        while route_pools
            .pool_for_target(&target)
            .is_some_and(|pool| pool.idle_backends() < desired_idle_backends)
        {
            let backend = checkout_backend(CheckoutBackendRequest {
                route_pools: &route_pools,
                route: route.clone(),
                target: target.clone(),
                context: "warm backend pool",
                mode: CheckoutMode::PreferConnect,
                session_id,
                debug_sampler,
                phase_recorder: phase_recorder.as_ref(),
                snapshot_store: &snapshot_store,
                startup_packet: &startup_packet,
                backend_credentials: Some(backend_credentials.as_ref()),
                read_after_write_state: ReadAfterWriteState::Disabled,
                record_snapshot: false,
                bootstrap_backend: true,
            })
            .await;

            match backend {
                Ok(backend) => backend.release().await,
                Err(error) => {
                    tracing::debug!(?error, "backend pool warm-up stopped");
                    return;
                }
            }
        }
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckoutMode {
    AllowConnect,
    PreferConnect,
}

#[derive(Debug)]
enum CheckoutFailure {
    Overload(&'static str),
    Postgres {
        sqlstate: &'static str,
        message: &'static str,
    },
    Close,
    Fatal(anyhow::Error),
}

#[must_use]
pub fn apply_policy_before_routing_target(
    planner: &ReadRoutingPlanner,
    context: RoutingContext<'_>,
    action: Option<&PolicyAction>,
) -> RoutingTarget {
    crate::routing::apply_policy_action_to_routing_target(planner, context, None, action)
}

#[must_use]
pub fn apply_policy_after_routing_target(
    planner: &ReadRoutingPlanner,
    context: RoutingContext<'_>,
    current_target: RoutingTarget,
    action: Option<&PolicyAction>,
) -> RoutingTarget {
    crate::routing::apply_policy_action_to_routing_target(
        planner,
        context,
        Some(current_target),
        action,
    )
}

#[must_use]
pub fn apply_policy_before_checkout_target(
    planner: &ReadRoutingPlanner,
    context: RoutingContext<'_>,
    current_target: RoutingTarget,
    action: Option<&PolicyAction>,
) -> RoutingTarget {
    apply_policy_after_routing_target(planner, context, current_target, action)
}

#[must_use]
pub fn apply_policy_action_to_routing_target_with_mode(
    planner: &ReadRoutingPlanner,
    context: RoutingContext<'_>,
    current_target: Option<RoutingTarget>,
    action: Option<&PolicyAction>,
    policy_mode: PolicyMode,
) -> RoutingTarget {
    let routing_context = context.clone();
    let current_target =
        current_target.unwrap_or_else(|| choose_routing_target(planner, routing_context));

    match policy_mode {
        PolicyMode::Enforce => crate::routing::apply_policy_action_to_routing_target(
            planner,
            context,
            Some(current_target),
            action,
        ),
        PolicyMode::Disabled | PolicyMode::DryRun => current_target,
    }
}

#[must_use]
pub fn policy_audit_event_from_decision(
    runtime: &PolicyRuntime,
    kind: PolicyAuditKind,
    decision: PolicyDecision,
    input: &PolicyEvalInput,
    target: &RoutingTarget,
) -> PolicyAuditEvent {
    let mut event = runtime.build_audit_event_from_input(kind, decision, input);
    event.target_role = target.target_role().map(|role| Arc::from(role.as_str()));
    event
}

#[must_use]
pub fn checkout_postgres_error_for_target(
    target: &RoutingTarget,
) -> Option<(&'static str, &'static str)> {
    match target {
        RoutingTarget::Reject {
            reason: RoutingReason::PolicyDenied,
        } => Some((POLICY_DENY_SQLSTATE, "policy denied")),
        RoutingTarget::Reject { .. } => {
            Some((CANNOT_CONNECT_NOW_SQLSTATE, REPLICA_UNAVAILABLE_MESSAGE))
        }
        RoutingTarget::Wait { .. }
        | RoutingTarget::Primary { .. }
        | RoutingTarget::Replica { .. } => None,
    }
}

#[must_use]
pub fn route_checkout_snapshot_for_target(
    route_key: RouteKey,
    target: RoutingTarget,
    read_after_write_state: ReadAfterWriteState,
) -> RouteCheckoutSnapshot {
    RouteCheckoutSnapshot::new(
        route_key,
        target.clone(),
        route_checkout_freshness_outcome(&target, read_after_write_state),
    )
}

#[must_use]
pub fn checkout_debug_fields(
    target: &RoutingTarget,
    checkout_mode: &'static str,
) -> Vec<(String, String)> {
    let target_role = target.target_role().map(|role| role.as_str().to_owned());
    let reason = target.reason().as_str().to_owned();

    vec![
        (String::from("checkout_mode"), checkout_mode.to_owned()),
        (
            String::from("target_role"),
            target_role.unwrap_or_else(|| String::from("unknown")),
        ),
        (String::from("reason"), reason),
    ]
}

fn startup_route_key(startup_packet: &[u8]) -> anyhow::Result<(String, String, Option<String>)> {
    let startup = parse_startup_packet(startup_packet).context("parse startup packet")?;
    let StartupPacket::Startup { parameters, .. } = startup else {
        anyhow::bail!("unexpected startup packet kind");
    };

    let database = startup_parameter(&parameters, "database")
        .context("startup packet missing database")?
        .to_owned();
    let user = startup_parameter(&parameters, "user")
        .context("startup packet missing user")?
        .to_owned();
    let application_name = startup_parameter(&parameters, "application_name").map(str::to_owned);

    Ok((database, user, application_name))
}

fn rewrite_backend_startup_user(
    startup_packet: &[u8],
    backend_user: Option<&str>,
) -> anyhow::Result<BytesMut> {
    let Some(backend_user) = backend_user else {
        return Ok(BytesMut::from(startup_packet));
    };
    let StartupPacket::Startup {
        protocol_major,
        protocol_minor,
        parameters,
    } = parse_startup_packet(startup_packet).context("parse backend startup packet")?
    else {
        anyhow::bail!("unexpected startup packet kind");
    };

    let mut body = BytesMut::new();
    body.put_i32(((protocol_major as i32) << 16) | (protocol_minor as i32 & 0xffff));
    let mut has_user = false;
    for (key, value) in parameters {
        body.extend_from_slice(key.as_bytes());
        body.put_u8(0);
        if key.eq_ignore_ascii_case("user") {
            body.extend_from_slice(backend_user.as_bytes());
            has_user = true;
        } else {
            body.extend_from_slice(value.as_bytes());
        }
        body.put_u8(0);
    }
    anyhow::ensure!(has_user, "startup packet missing user");
    body.put_u8(0);

    let mut packet = BytesMut::with_capacity(body.len() + 4);
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    Ok(packet)
}

fn startup_parameter<'a>(parameters: &'a [(String, String)], key: &str) -> Option<&'a str> {
    parameters
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

fn route_key(
    database: &str,
    user: &str,
    application_name: Option<&str>,
    client_addr: SocketAddr,
) -> RouteKey {
    RouteKey::new(
        database,
        user,
        application_name,
        Some(client_addr),
        QueryClass::Default,
    )
}

struct ReadRoutingSelection<'a> {
    planner: &'a ReadRoutingPlanner,
    route_pools: &'a RoutePools,
    snapshot_store: &'a SnapshotStore,
    read_routing_mode: ReadRoutingMode,
    fallback_policy: FallbackPolicy,
    session: &'a VirtualSession,
    request_plan: Option<&'a RequestPlan<'a>>,
}

fn select_checkout_target(selection: &ReadRoutingSelection<'_>) -> RoutingTarget {
    let (sql, analysis) = selection
        .request_plan
        .map_or(("", analyze_sql("")), |plan| {
            (plan.sql.as_ref(), plan.analysis())
        });
    let health = build_route_health_snapshot(selection.route_pools, selection.snapshot_store);
    let routing_context = RoutingContext::with_analysis(
        sql,
        TransactionState::Idle,
        selection.session.read_after_write_state(),
        &health,
        analysis,
    );

    let policy_before_routing_target =
        apply_policy_before_routing_target(selection.planner, routing_context.clone(), None);
    if matches!(
        policy_before_routing_target,
        RoutingTarget::Reject {
            reason: RoutingReason::PolicyDenied,
        }
    ) {
        return policy_before_routing_target;
    }

    match selection.read_routing_mode {
        ReadRoutingMode::Off => {
            let target = RoutingTarget::Primary {
                reason: RoutingReason::Off,
            };
            return apply_policy_before_checkout_target(
                selection.planner,
                routing_context,
                target,
                None,
            );
        }
        ReadRoutingMode::PrimaryOnly => {
            let target = RoutingTarget::Primary {
                reason: RoutingReason::PrimaryOnlyMode,
            };
            return apply_policy_before_checkout_target(
                selection.planner,
                routing_context,
                target,
                None,
            );
        }
        ReadRoutingMode::PreferReplica | ReadRoutingMode::RequireReplica => {}
    }

    if selection.session.transaction_cross_shard_violation() {
        let target = RoutingTarget::Reject {
            reason: RoutingReason::FallbackReject,
        };
        return apply_policy_before_checkout_target(
            selection.planner,
            routing_context,
            target,
            None,
        );
    }

    if let Some(transaction_role) = selection.session.current_transaction_target_role() {
        let reason = selection
            .session
            .current_transaction_route_reason()
            .map(routing_reason_from_core)
            .unwrap_or(RoutingReason::TransactionControl);
        let target = match transaction_role {
            BackendRole::Primary => RoutingTarget::Primary { reason },
            BackendRole::Replica => {
                if let Some(replica) = selection
                    .route_pools
                    .selector()
                    .select(selection.route_pools.replicas())
                {
                    RoutingTarget::Replica {
                        candidate: ReplicaCandidate::new(
                            replica.id(),
                            replica.is_healthy(),
                            None,
                            None,
                        ),
                        reason,
                    }
                } else {
                    fallback_target(selection.read_routing_mode, selection.fallback_policy)
                }
            }
            BackendRole::Unknown => RoutingTarget::Primary {
                reason: RoutingReason::UnknownQuery,
            },
        };
        let target = apply_policy_after_routing_target(
            selection.planner,
            routing_context.clone(),
            target,
            None,
        );
        return apply_policy_before_checkout_target(
            selection.planner,
            routing_context,
            target,
            None,
        );
    }

    let target = choose_routing_target(selection.planner, routing_context.clone());
    let target =
        apply_policy_after_routing_target(selection.planner, routing_context.clone(), target, None);
    apply_policy_before_checkout_target(selection.planner, routing_context, target, None)
}

fn fallback_target(
    read_routing_mode: ReadRoutingMode,
    fallback_policy: FallbackPolicy,
) -> RoutingTarget {
    if read_routing_mode == ReadRoutingMode::RequireReplica {
        return RoutingTarget::Reject {
            reason: RoutingReason::RequireReplicaMode,
        };
    }

    match fallback_policy {
        FallbackPolicy::Primary => RoutingTarget::Primary {
            reason: RoutingReason::FallbackPrimary,
        },
        FallbackPolicy::Reject => RoutingTarget::Reject {
            reason: RoutingReason::FallbackReject,
        },
        FallbackPolicy::Wait => RoutingTarget::Wait {
            reason: RoutingReason::FallbackWait,
        },
    }
}

fn routing_reason_from_core(reason: CoreRoutingReason) -> RoutingReason {
    match reason {
        CoreRoutingReason::Off => RoutingReason::Off,
        CoreRoutingReason::PrimaryOnlyMode => RoutingReason::PrimaryOnlyMode,
        CoreRoutingReason::PreferReplicaMode => RoutingReason::ReadCandidateQuery,
        CoreRoutingReason::RequireReplicaMode => RoutingReason::RequireReplicaMode,
        CoreRoutingReason::WriteQuery => RoutingReason::WriteQuery,
        CoreRoutingReason::ReadOnlyQuery => RoutingReason::ReadOnlyQuery,
        CoreRoutingReason::ReadCandidateQuery => RoutingReason::ReadCandidateQuery,
        CoreRoutingReason::TransactionControl => RoutingReason::TransactionControl,
        CoreRoutingReason::SessionMutation => RoutingReason::SessionMutation,
        CoreRoutingReason::Copy => RoutingReason::CopyQuery,
        CoreRoutingReason::UnknownQuery => RoutingReason::UnknownQuery,
        CoreRoutingReason::FreshnessRequired => RoutingReason::FreshnessRequired,
        CoreRoutingReason::ReplicaStale => RoutingReason::ReplicaStale,
        CoreRoutingReason::ReplicaUnavailable => RoutingReason::ReplicaUnavailable,
        CoreRoutingReason::FallbackPrimary => RoutingReason::FallbackPrimary,
        CoreRoutingReason::FallbackReject => RoutingReason::FallbackReject,
        CoreRoutingReason::FallbackWait => RoutingReason::FallbackWait,
    }
}

fn build_route_health_snapshot(
    route_pools: &RoutePools,
    snapshot_store: &SnapshotStore,
) -> RouteHealthSnapshot {
    let replica_health_snapshots = snapshot_store.replica_health_snapshots();
    RouteHealthSnapshot::new(
        route_pools
            .replicas()
            .iter()
            .map(|replica| {
                let replica_health = replica_health_snapshots
                    .iter()
                    .find(|snapshot| snapshot.endpoint_id == replica.id());
                let replay_lsn = replica_health.and_then(|snapshot| snapshot.replay_lsn);
                let lag_ms = replica_health.and_then(|snapshot| {
                    snapshot
                        .lag_duration
                        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
                });
                let split_brain_warning = replica_health.and_then(|snapshot| snapshot.role.warning);
                if let Some(warning) = split_brain_warning {
                    tracing::warn!(
                        endpoint_id = replica.id(),
                        expected_role = warning.expected_role.as_str(),
                        observed_role = warning.observed_role.as_str(),
                        "split-brain warning: replica role does not match expected role"
                    );
                }

                let mut candidate =
                    ReplicaCandidate::new(replica.id(), replica.is_healthy(), replay_lsn, lag_ms);
                candidate.split_brain = split_brain_warning.is_some();
                candidate
            })
            .collect(),
    )
}

struct RequestPlan<'a> {
    sql: Cow<'a, str>,
    command: SqlCommand,
    analysis: Option<SqlAnalysis>,
    updates_transaction_state: bool,
    updates_session_state: bool,
}

impl<'a> RequestPlan<'a> {
    fn new(
        sql: Cow<'a, str>,
        updates_transaction_state: bool,
        updates_session_state: bool,
        needs_analysis: bool,
    ) -> Self {
        let command = classify(sql.as_ref());
        let analysis = needs_analysis.then(|| analyze_sql(sql.as_ref()));
        Self {
            sql,
            command,
            analysis,
            updates_transaction_state,
            updates_session_state,
        }
    }

    fn from_prepared(statement: &'a pg_kinetic_core::prepare::PreparedStatement) -> Self {
        Self {
            sql: Cow::Borrowed(statement.query.as_str()),
            command: statement.command().clone(),
            analysis: Some(statement.analysis()),
            updates_transaction_state: false,
            updates_session_state: false,
        }
    }

    fn analysis(&self) -> SqlAnalysis {
        self.analysis
            .unwrap_or_else(|| analyze_sql(self.sql.as_ref()))
    }
}

fn request_plans_for_frames<'a>(
    prepared: &'a PreparedCatalog,
    frames: &'a [FrontendFrame],
    analyze_sql: bool,
) -> anyhow::Result<Vec<RequestPlan<'a>>> {
    let mut plans = Vec::new();
    for frame in frames {
        if let Some(query) = parse_simple_query(frame)? {
            plans.push(RequestPlan::new(
                Cow::Borrowed(query),
                true,
                true,
                analyze_sql,
            ));
            continue;
        }

        if let Some(parse) = parse_parse_message(frame)? {
            plans.push(RequestPlan::new(
                Cow::Owned(parse.query),
                true,
                false,
                analyze_sql,
            ));
            continue;
        }

        if let Some(statement_name) = parse_bind_statement_name(frame).ok().flatten() {
            if let Some(statement) = prepared.get_for_current_route_map(&statement_name) {
                plans.push(RequestPlan::from_prepared(statement));
                continue;
            }
        }

        if let Some(statement) =
            parse_describe_target(frame)
                .ok()
                .flatten()
                .and_then(|describe_target| match describe_target {
                    DescribeTarget::Statement(statement_name) => prepared
                        .get_for_current_route_map(&statement_name)
                        .map(|statement| statement),
                    _ => None,
                })
        {
            plans.push(RequestPlan::from_prepared(statement));
        }
    }

    Ok(plans)
}

fn safe_request_to_replay(
    frames: &[FrontendFrame],
    plans: &[RequestPlan<'_>],
    session: &VirtualSession,
) -> bool {
    !frames.is_empty()
        && frames
            .iter()
            .all(|frame| frame.tag == u8::from(FrontendTag::Query))
        && plans.len() == 1
        && plans[0].analysis().query_class().routes_to_replica()
        && session.pin_reason().is_none()
        && !session.has_replayable_settings()
}

fn mirror_sql_command_for_request_plan(request_plan: Option<&RequestPlan<'_>>) -> SqlCommand {
    request_plan
        .map(|plan| plan.command.clone())
        .unwrap_or(SqlCommand::Query)
}

fn route_checkout_freshness_outcome(
    target: &RoutingTarget,
    read_after_write_state: ReadAfterWriteState,
) -> Option<FreshnessStatus> {
    match target {
        RoutingTarget::Replica { .. } => Some(FreshnessStatus::Satisfied),
        RoutingTarget::Wait { reason } => match reason {
            RoutingReason::FallbackWait => Some(match read_after_write_state {
                ReadAfterWriteState::Unknown => FreshnessStatus::Unknown,
                ReadAfterWriteState::Required(_) => FreshnessStatus::Waiting,
                ReadAfterWriteState::Disabled => FreshnessStatus::Unavailable,
            }),
            _ => None,
        },
        RoutingTarget::Reject { reason } => Some(match reason {
            RoutingReason::ReplicaStale => FreshnessStatus::Stale,
            RoutingReason::FreshnessRequired => FreshnessStatus::Unknown,
            RoutingReason::PolicyDenied => FreshnessStatus::Unavailable,
            RoutingReason::ReplicaUnavailable
            | RoutingReason::FallbackReject
            | RoutingReason::RequireReplicaMode => FreshnessStatus::Unavailable,
            _ => FreshnessStatus::Unknown,
        }),
        RoutingTarget::Primary { reason } => match reason {
            RoutingReason::FallbackPrimary => match read_after_write_state {
                ReadAfterWriteState::Disabled => None,
                ReadAfterWriteState::Required(_) => Some(FreshnessStatus::Stale),
                ReadAfterWriteState::Unknown => Some(FreshnessStatus::Unknown),
            },
            _ => None,
        },
    }
}

fn checkout_mode_label(mode: CheckoutMode) -> &'static str {
    match mode {
        CheckoutMode::AllowConnect => "allow_connect",
        CheckoutMode::PreferConnect => "prefer_connect",
    }
}

async fn wait_for_checkout_target(
    route: &RouteKey,
    selection: &ReadRoutingSelection<'_>,
    wait_timeout: Duration,
) -> RoutingTarget {
    let started = Instant::now();
    let mut checkout_target = select_checkout_target(selection);
    let mut waited = false;

    while matches!(
        checkout_target,
        RoutingTarget::Wait {
            reason: RoutingReason::FallbackWait,
        }
    ) && started.elapsed() < wait_timeout
    {
        waited = true;
        let remaining = wait_timeout.saturating_sub(started.elapsed());
        let sleep_for = remaining.min(Duration::from_millis(25));
        if sleep_for.is_zero() {
            break;
        }

        tokio::time::sleep(sleep_for).await;
        checkout_target = select_checkout_target(selection);
    }

    let timed_out = matches!(
        checkout_target,
        RoutingTarget::Wait {
            reason: RoutingReason::FallbackWait,
        }
    );

    if waited {
        metrics::record_read_after_write_wait(
            route,
            started.elapsed().as_secs_f64() * 1_000.0,
            if timed_out {
                FreshnessStatus::Unavailable
            } else {
                FreshnessStatus::Waiting
            },
        );
    }

    if timed_out {
        fallback_target(selection.read_routing_mode, FallbackPolicy::Primary)
    } else {
        checkout_target
    }
}

fn build_route_pools(
    config: &Config,
    route_config: &RouteConfig,
    snapshot_store: SnapshotStore,
) -> RoutePools {
    let mut lifecycle = config.pool_lifecycle.clone();
    lifecycle.max_size = lifecycle.max_size.min(config.capacity.max_backends);
    let pool_args = (
        config.tls.clone(),
        config.socket.clone(),
        config.capacity.max_checkout_waiters,
        config.qos.max_route_in_flight,
        config.qos.max_route_waiters,
        config.performance.checkout_timeout(),
        config.performance.backend_reset_query.clone(),
        lifecycle,
    );

    let primary_pool = BackendPool::new_with_socket_and_lifecycle(
        route_config.primary.address,
        pool_args.0.clone(),
        pool_args.1.clone(),
        pool_args.2,
        pool_args.3,
        pool_args.4,
        pool_args.5,
        pool_args.6.clone(),
        pool_args.7.clone(),
    );
    let primary = BackendPoolRef::primary(primary_pool);
    primary.attach_snapshot_store(snapshot_store.clone());

    let replicas = route_config
        .replicas
        .iter()
        .enumerate()
        .map(|(index, replica)| {
            let pool = BackendPool::new_with_socket_and_lifecycle(
                replica.address,
                pool_args.0.clone(),
                pool_args.1.clone(),
                pool_args.2,
                pool_args.3,
                pool_args.4,
                pool_args.5,
                pool_args.6.clone(),
                pool_args.7.clone(),
            );
            BackendPoolRef::replica(index as u64 + 1, replica.weight as usize, pool)
        })
        .collect();

    RoutePools::new(
        primary,
        replicas,
        ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
    )
}

#[derive(Debug)]
struct ClientSnapshotGuard {
    handle: ClientSnapshotHandle,
    client_id: u64,
    session_id: u64,
    started: Instant,
    client_addr: SocketAddr,
    debug_sampler: DebugSampler,
}

impl ClientSnapshotGuard {
    fn new(
        handle: ClientSnapshotHandle,
        client_id: u64,
        client_addr: SocketAddr,
        started: Instant,
        debug_sampler: DebugSampler,
    ) -> Self {
        Self {
            handle,
            client_id,
            session_id: client_id,
            started,
            client_addr,
            debug_sampler,
        }
    }
}

impl Drop for ClientSnapshotGuard {
    fn drop(&mut self) {
        let _ = self.handle.remove(self.client_id);
        telemetry::emit_debug_sample_with(&self.debug_sampler, self.session_id, || {
            DebugSample::client_close(self.session_id, self.client_addr, self.started.elapsed())
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn update_client_snapshot(
    handle: &ClientSnapshotHandle,
    client_id: u64,
    database: String,
    user: String,
    application_name: Option<String>,
    route_key: RouteKey,
    state: &'static str,
    connected_duration: Duration,
) {
    handle.upsert(ClientSnapshot {
        client_id,
        user: Some(user),
        database: Some(database),
        application_name,
        route_key: Some(route_key),
        state: state.to_string(),
        connected_duration,
    });
}

fn record_pinning_snapshot(
    snapshot_store: &SnapshotStore,
    client_id: u64,
    backend_id: u64,
    reason: PinReason,
    route_key: RouteKey,
    duration: Duration,
) {
    snapshot_store.set_pinning_snapshot(PinningSnapshot::new(
        client_id,
        Some(backend_id),
        Some(route_key),
        snapshot_pin_reason(reason),
        duration,
    ));
}

fn snapshot_pin_reason(reason: PinReason) -> SessionPinReason {
    match reason {
        PinReason::OpenTransaction => SessionPinReason::OpenTransaction,
        PinReason::FailedTransaction => SessionPinReason::FailedTransaction,
        PinReason::SessionState => SessionPinReason::SessionState,
        PinReason::TempTable | PinReason::AdvisoryLock => SessionPinReason::SessionState,
        PinReason::Copy => SessionPinReason::Copy,
        PinReason::ListenNotify => SessionPinReason::ListenNotify,
        PinReason::UnknownProtocolState => SessionPinReason::UnknownProtocolState,
    }
}

fn clear_pinned_backend(
    pinned_backend: &mut PinnedBackend,
    snapshot_store: &SnapshotStore,
    client_id: u64,
) {
    pinned_backend.clear();
    let _ = snapshot_store.remove_pinning_snapshot(client_id);
}

async fn next_client_cycle(
    client: &mut ClientConnection,
    client_buffer: &mut BytesMut,
    idle_timeout: Option<Duration>,
    idle_timeout_kind: IdleTimeoutKind,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<Option<ClientCycle>> {
    let first = loop {
        if let Some(frame) = parse_frontend_frame(client_buffer)? {
            break frame;
        }

        if client_buffer.len() >= max_client_buffer_bytes {
            return Ok(Some(ClientCycle::BufferLimitExceeded));
        }

        match idle_timeout {
            Some(duration) => match timeout(duration, client.read_buf(client_buffer)).await {
                Ok(Ok(0)) => return Ok(Some(ClientCycle::Terminate)),
                Ok(Ok(_)) => {
                    if client_buffer.len() > max_client_buffer_bytes {
                        return Ok(Some(ClientCycle::BufferLimitExceeded));
                    }
                    continue;
                }
                Ok(Err(error)) => return Err(error).context("read client"),
                Err(_) => return Ok(Some(ClientCycle::IdleTimeout(idle_timeout_kind))),
            },
            None => {
                if client
                    .read_buf(client_buffer)
                    .await
                    .context("read client")?
                    == 0
                {
                    return Ok(Some(ClientCycle::Terminate));
                }

                if client_buffer.len() > max_client_buffer_bytes {
                    return Ok(Some(ClientCycle::BufferLimitExceeded));
                }
            }
        }
    };

    if first.tag == u8::from(FrontendTag::Terminate) {
        return Ok(Some(ClientCycle::Terminate));
    }

    if first.tag == u8::from(FrontendTag::Query) {
        return Ok(Some(ClientCycle::Frames(vec![first])));
    }

    let mut frames = vec![first];
    while !frames
        .iter()
        .any(|frame| frame.tag == u8::from(FrontendTag::Sync))
    {
        if let Some(frame) = parse_frontend_frame(client_buffer)? {
            frames.push(frame);
            continue;
        }

        if client_buffer.len() >= max_client_buffer_bytes {
            return Ok(Some(ClientCycle::BufferLimitExceeded));
        }

        match idle_timeout {
            Some(duration) => match timeout(duration, client.read_buf(client_buffer)).await {
                Ok(Ok(0)) => return Ok(Some(ClientCycle::Terminate)),
                Ok(Ok(_)) => {
                    if client_buffer.len() > max_client_buffer_bytes {
                        return Ok(Some(ClientCycle::BufferLimitExceeded));
                    }
                    continue;
                }
                Ok(Err(error)) => return Err(error).context("read extended query frame"),
                Err(_) => return Ok(Some(ClientCycle::IdleTimeout(idle_timeout_kind))),
            },
            None => {
                if client
                    .read_buf(client_buffer)
                    .await
                    .context("read extended query frame")?
                    == 0
                {
                    return Ok(Some(ClientCycle::Terminate));
                }

                if client_buffer.len() > max_client_buffer_bytes {
                    return Ok(Some(ClientCycle::BufferLimitExceeded));
                }
            }
        }
    }

    Ok(Some(ClientCycle::Frames(frames)))
}

#[derive(Debug)]
enum ClientCycle {
    Frames(Vec<FrontendFrame>),
    Terminate,
    IdleTimeout(IdleTimeoutKind),
    BufferLimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdleTimeoutKind {
    Client,
    Transaction,
}

#[derive(Default)]
struct QueryProgress {
    response_started: bool,
}

pub(crate) async fn read_startup_packet(
    client: &mut ClientConnection,
    client_tls_mode: crate::config::ClientTlsMode,
    client_tls_server_config: Option<&Arc<ServerConfig>>,
    idle_timeout: Duration,
    max_client_buffer_bytes: usize,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<StartupRead> {
    let mut buffer = BytesMut::with_capacity(8192);
    read_startup_packet_with_buffer(
        client,
        client_tls_mode,
        client_tls_server_config,
        idle_timeout,
        max_client_buffer_bytes,
        &mut buffer,
        phase_recorder,
    )
    .await
}

async fn read_startup_packet_with_buffer(
    client: &mut ClientConnection,
    client_tls_mode: crate::config::ClientTlsMode,
    client_tls_server_config: Option<&Arc<ServerConfig>>,
    idle_timeout: Duration,
    max_client_buffer_bytes: usize,
    buffer: &mut BytesMut,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<StartupRead> {
    let client_tls_required = matches!(
        client_tls_mode,
        crate::config::ClientTlsMode::Require | crate::config::ClientTlsMode::VerifyClient
    );
    loop {
        while let Some(packet) = next_startup_packet(buffer)? {
            match parse_startup_packet(&packet) {
                Ok(StartupPacket::SslRequest) => {
                    match client_tls_mode {
                        crate::config::ClientTlsMode::Disable => {
                            reject_startup_encryption_request(client).await?;
                        }
                        crate::config::ClientTlsMode::Allow
                        | crate::config::ClientTlsMode::Require
                        | crate::config::ClientTlsMode::VerifyClient => {
                            client
                                .write_all(b"S")
                                .await
                                .context("accept startup encryption request")?;
                            let server_config = client_tls_server_config
                                .context("client TLS server config is unavailable")?;
                            let tls_timer =
                                PhaseTimer::start(ProtocolPhase::TlsHandshake, phase_recorder);
                            let tls_result = client.start_tls(server_config).await;
                            let tls_outcome = match &tls_result {
                                Ok(())
                                    if matches!(
                                        client_tls_mode,
                                        crate::config::ClientTlsMode::VerifyClient
                                    ) && !client.has_peer_certificates() =>
                                {
                                    MetricOutcome::Rejected
                                }
                                Ok(()) => MetricOutcome::Ok,
                                Err(_) => MetricOutcome::Error,
                            };
                            tls_timer.finish(tls_outcome);
                            tls_result?;
                            if matches!(client_tls_mode, crate::config::ClientTlsMode::VerifyClient)
                                && !client.has_peer_certificates()
                            {
                                anyhow::bail!("client certificate is required");
                            }
                            buffer.clear();
                        }
                    }
                    continue;
                }
                Ok(StartupPacket::GssEncRequest) => {
                    reject_startup_encryption_request(client).await?;
                    continue;
                }
                Ok(StartupPacket::Startup { .. }) if client_tls_required && !client.is_tls() => {
                    anyhow::bail!("client TLS is required");
                }
                Ok(StartupPacket::CancelRequest { .. }) => {
                    anyhow::bail!("cancel requests are not supported during startup");
                }
                Ok(StartupPacket::Startup { .. }) => return Ok(StartupRead::Packet(packet)),
                Err(error) => return Err(error).context("parse startup packet"),
            }
        }

        if buffer.len() >= max_client_buffer_bytes {
            return Ok(StartupRead::BufferLimitExceeded);
        }

        match timeout(idle_timeout, client.read_buf(buffer)).await {
            Ok(Ok(0)) => return Ok(StartupRead::ClientClosed),
            Ok(Ok(_)) => {
                if buffer.len() > max_client_buffer_bytes {
                    return Ok(StartupRead::BufferLimitExceeded);
                }
                continue;
            }
            Ok(Err(error)) => return Err(error).context("read startup"),
            Err(_) => return Ok(StartupRead::TimedOut),
        }
    }
}

fn next_startup_packet(buffer: &mut BytesMut) -> anyhow::Result<Option<BytesMut>> {
    if buffer.len() < 4 {
        return Ok(None);
    }

    let len = i32::from_be_bytes(
        buffer[..4]
            .try_into()
            .expect("four startup length bytes are present"),
    );
    if len < 8 {
        return Err(WireError::InvalidStartupLength(len)).context("parse startup packet");
    }

    let len = len as usize;
    if buffer.len() < len {
        return Ok(None);
    }

    Ok(Some(buffer.split_to(len)))
}

async fn reject_startup_encryption_request(client: &mut ClientConnection) -> anyhow::Result<()> {
    client
        .write_all(b"N")
        .await
        .context("reject startup encryption request")
}

#[derive(Debug)]
pub(crate) enum StartupRead {
    Packet(BytesMut),
    ClientClosed,
    TimedOut,
    BufferLimitExceeded,
}

#[allow(clippy::too_many_arguments)]
async fn proxy_startup(
    client: &mut ClientConnection,
    backend: &mut PooledBackend,
    startup_packet: &[u8],
    max_client_buffer_bytes: usize,
    max_backend_buffer_bytes: usize,
    forward_backend_auth_requests_to_client: bool,
    emit_auth_ok_when_backend_requires_no_startup: bool,
    backend_credentials: Option<&auth::BackendCredentials>,
    buffers: &mut SessionBufferSet,
    _phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<()> {
    if !backend.requires_startup() {
        let startup_response = if emit_auth_ok_when_backend_requires_no_startup {
            synthetic_startup_ready()
        } else {
            ready_for_query_idle()
        };
        client
            .write_all(&startup_response)
            .await
            .context("write synthetic startup response")?;
        return Ok(());
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(startup_packet)
        .await
        .context("forward startup")?;

    buffers.client_read_mut().clear();
    buffers.backend_read_mut().clear();
    let mut backend_auth = backend_credentials
        .cloned()
        .map(auth::BackendAuthSession::new)
        .transpose()?;
    loop {
        if buffers.backend_read_mut().len() >= max_backend_buffer_bytes {
            return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
        }

        backend
            .backend_mut()
            .stream_mut()
            .read_buf(buffers.backend_read_mut())
            .await
            .context("read startup response")?;
        buffers.observe_backend_read();
        if buffers.backend_read_mut().len() > max_backend_buffer_bytes {
            return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
        }

        while let Some(frame) = parse_backend_frame(buffers.backend_read_mut())? {
            if frame.tag == u8::from(BackendTag::Authentication) {
                let code = auth_request_code(&frame.payload)?;
                if let Some(backend_auth) = backend_auth.as_mut() {
                    if let Some(response) =
                        backend_auth.respond(&frame.payload, backend.backend_mut().is_tls())?
                    {
                        backend
                            .backend_mut()
                            .stream_mut()
                            .write_all(&response)
                            .await
                            .context("respond to backend authentication request")?;
                    }
                    continue;
                }
                if code == 0 {
                    if forward_backend_auth_requests_to_client {
                        client
                            .write_all(&encode_backend_frame(&frame))
                            .await
                            .context("forward startup response")?;
                    }
                    continue;
                }

                if forward_backend_auth_requests_to_client {
                    client
                        .write_all(&encode_backend_frame(&frame))
                        .await
                        .context("forward startup response")?;

                    if auth_request_expects_client_response(&frame.payload)? {
                        if buffers.client_read_mut().len() >= max_client_buffer_bytes {
                            return Err(buffer_limit_exceeded(BufferBudgetKind::Client));
                        }

                        buffers.client_read_mut().clear();
                        let read = client
                            .read_buf(buffers.client_read_mut())
                            .await
                            .context("read startup auth response")?;
                        anyhow::ensure!(read > 0, "client disconnected during startup auth");
                        buffers.observe_client_read();
                        if buffers.client_read_mut().len() > max_client_buffer_bytes {
                            return Err(buffer_limit_exceeded(BufferBudgetKind::Client));
                        }
                        backend
                            .backend_mut()
                            .stream_mut()
                            .write_all(buffers.client_read_mut())
                            .await
                            .context("forward startup auth response")?;
                        buffers.client_read_mut().clear();
                    }
                } else {
                    anyhow::bail!(
                        "backend authentication exchange is not supported after local auth"
                    );
                }
            } else {
                client
                    .write_all(&encode_backend_frame(&frame))
                    .await
                    .context("forward startup response")?;
            }

            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return Ok(());
            }
        }
    }
}

async fn bootstrap_backend(
    backend: &mut PooledBackend,
    startup_packet: &[u8],
    backend_credentials: Option<&auth::BackendCredentials>,
) -> anyhow::Result<()> {
    if !backend.requires_startup() {
        return Ok(());
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(startup_packet)
        .await
        .context("forward backend startup")?;

    let mut backend_buffer = BytesMut::with_capacity(8192);
    let mut backend_auth = backend_credentials
        .cloned()
        .map(auth::BackendAuthSession::new)
        .transpose()?;
    loop {
        backend
            .backend_mut()
            .stream_mut()
            .read_buf(&mut backend_buffer)
            .await
            .context("read backend startup response")?;

        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            if frame.tag == u8::from(BackendTag::Authentication) {
                let code = auth_request_code(&frame.payload)?;
                if let Some(backend_auth) = backend_auth.as_mut() {
                    if let Some(response) =
                        backend_auth.respond(&frame.payload, backend.backend_mut().is_tls())?
                    {
                        backend
                            .backend_mut()
                            .stream_mut()
                            .write_all(&response)
                            .await
                            .context("respond to backend bootstrap authentication request")?;
                    }
                } else if code != 0 && auth_request_expects_client_response(&frame.payload)? {
                    anyhow::bail!("backend authentication exchange requires client response");
                }
            }

            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return Ok(());
            }
        }
    }
}

fn auth_request_code(payload: &[u8]) -> anyhow::Result<i32> {
    anyhow::ensure!(payload.len() >= 4, "authentication request missing code");
    Ok(i32::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

fn encode_backend_frame(frame: &BackendFrame) -> BytesMut {
    let mut encoded = BytesMut::with_capacity(frame.payload.len() + 5);
    encoded.put_u8(frame.tag);
    encoded.put_i32((frame.payload.len() + 4) as i32);
    encoded.extend_from_slice(&frame.payload);
    encoded
}

fn synthetic_startup_ready() -> BytesMut {
    let ready = ready_for_query_idle();
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::Authentication));
    bytes.put_i32(8);
    bytes.put_i32(0);
    bytes.extend_from_slice(&ready);
    bytes
}

fn ready_for_query_idle() -> BytesMut {
    ready_for_query(ReadyStatus::Idle)
}

fn ready_for_query(status: ReadyStatus) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(match status {
        ReadyStatus::Idle => u8::from(ReadyStatusByte::Idle),
        ReadyStatus::InTransaction => u8::from(ReadyStatusByte::InTransaction),
        ReadyStatus::FailedTransaction => u8::from(ReadyStatusByte::FailedTransaction),
    });
    bytes
}

fn auth_request_expects_client_response(payload: &[u8]) -> anyhow::Result<bool> {
    let code = auth_request_code(payload)?;
    Ok(matches!(code, 3 | 5 | 6 | 7 | 8 | 9 | 10 | 11))
}

struct ForwardCycleState<'a> {
    session: &'a mut VirtualSession,
    prepared: &'a mut PreparedCatalog,
    prepared_snapshot_handle: PreparedSnapshotHandle,
    route_application_name: &'a mut Option<String>,
    progress: &'a mut QueryProgress,
}

async fn forward_message_cycle(
    client: &mut ClientConnection,
    backend: &mut PooledBackend,
    state: &mut ForwardCycleState<'_>,
    frames: &[FrontendFrame],
    simple_query_commands: &[SqlCommand],
    max_backend_buffer_bytes: usize,
    buffers: &mut SessionBufferSet,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<ForwardOutcome> {
    let needs_sync = should_sync_for_frames(&frames);
    let execute_timer = PhaseTimer::start(ProtocolPhase::Execute, phase_recorder);
    let mut simple_query_commands = simple_query_commands.iter();
    buffers.clear_backend_write();
    for frame in frames {
        let simple_query_command = if frame.tag == u8::from(FrontendTag::Query) {
            Some(
                simple_query_commands
                    .next()
                    .context("missing request plan for simple query")?,
            )
        } else {
            None
        };
        let plan = prepare_frame_for_backend(
            backend.backend_id(),
            state.prepared,
            &state.prepared_snapshot_handle,
            frame.clone(),
            phase_recorder,
        )?;
        update_virtual_session_from_frame(
            state.session,
            &plan.frame,
            state.route_application_name,
            simple_query_command,
        )?;

        for prelude in &plan.prelude {
            buffers.append_frontend_frame(prelude.tag, &prelude.payload);
        }
        buffers.append_frontend_frame(plan.frame.tag, &plan.frame.payload);
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(buffers.backend_write())
        .await
        .map_err(|error| {
            backend_failure(
                BackendFailureKind::Write,
                false,
                anyhow::Error::new(error).context("write frontend cycle to backend"),
            )
        })?;
    execute_timer.finish(MetricOutcome::Ok);
    buffers.clear_backend_write();

    let rows_timer = PhaseTimer::start(ProtocolPhase::Rows, phase_recorder);
    buffers.backend_read_mut().clear();
    loop {
        if buffers.backend_read_mut().len() >= max_backend_buffer_bytes {
            record_buffer_limit(BufferBudgetKind::Backend);
            rows_timer.finish(MetricOutcome::Discarded);
            return Ok(ForwardOutcome::BufferLimitExceeded);
        }

        let read = backend
            .backend_mut()
            .stream_mut()
            .read_buf(buffers.backend_read_mut())
            .await
            .map_err(|error| {
                backend_failure(
                    BackendFailureKind::Read,
                    state.progress.response_started,
                    anyhow::Error::new(error).context("read backend frame"),
                )
            })?;
        if read == 0 {
            return Err(backend_failure(
                BackendFailureKind::Read,
                state.progress.response_started,
                anyhow::anyhow!("backend disconnected during response cycle"),
            ));
        }

        buffers.observe_backend_read();
        if buffers.backend_read_mut().len() > max_backend_buffer_bytes {
            record_buffer_limit(BufferBudgetKind::Backend);
            return Ok(ForwardOutcome::BufferLimitExceeded);
        }

        buffers.clear_client_write();
        let mut ready = None;
        while let Some(frame) = parse_backend_frame(buffers.backend_read_mut())? {
            state.progress.response_started = true;
            if let Some(sqlstate) = frame.sqlstate() {
                metrics::increment_sqlstate(sqlstate);
                let scope = state
                    .prepared
                    .invalidate_for_sqlstate(sqlstate, backend.backend_id());
                if scope != InvalidationScope::None {
                    metrics::increment_prepared_event(PreparedEvent::Invalidate);
                    publish_prepared_snapshot(state.prepared, &state.prepared_snapshot_handle);
                }
            }

            if frame.tag == u8::from(BackendTag::ErrorResponse)
                && matches!(state.session.pin_reason(), Some(PinReason::OpenTransaction))
            {
                state.session.mark_failed_transaction();
            }

            buffers.append_backend_frame(frame.tag, &frame.payload);
            if let Some(status) = frame.ready_status() {
                ready = Some(status);
            }
        }

        if !buffers.client_write().is_empty()
            && client.write_all(buffers.client_write()).await.is_err()
        {
            buffers.clear_client_write();
            buffers.trim_empty_buffers();
            if let Some(status) = ready {
                rows_timer.finish(MetricOutcome::Canceled);
                return Ok(ForwardOutcome::ClientDisconnectedAfterReady(status));
            }

            rows_timer.finish(MetricOutcome::Canceled);
            return Ok(ForwardOutcome::AbandonedResponse { needs_sync });
        }

        if let Some(status) = ready {
            buffers.clear_client_write();
            buffers.trim_empty_buffers();
            rows_timer.finish(MetricOutcome::Ok);
            return Ok(ForwardOutcome::Ready(status));
        }
    }
}

fn prepare_frame_for_backend(
    backend_id: u64,
    prepared: &mut PreparedCatalog,
    prepared_snapshot_handle: &PreparedSnapshotHandle,
    frame: FrontendFrame,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<PreparedForwardPlan> {
    if let Some(parse) = parse_parse_message(&frame)? {
        let timer = PhaseTimer::start(ProtocolPhase::Parse, phase_recorder);
        let statement = prepared
            .upsert(parse.statement_name, parse.query, parse.parameter_type_oids)
            .clone();
        metrics::increment_prepared_event(PreparedEvent::Parse);
        prepared_snapshot_handle.increment_statement_count();
        prepared_snapshot_handle.increment_cache_miss();
        prepared.mark_materialized(backend_id, &statement);
        publish_prepared_snapshot(prepared, prepared_snapshot_handle);
        timer.finish(MetricOutcome::Ok);
        return Ok(PreparedForwardPlan::single(rewrite_parse_statement_name(
            &frame,
            &statement.backend_name,
        )?));
    }

    if let Some(statement_name) = parse_bind_statement_name(&frame)? {
        let timer = PhaseTimer::start(ProtocolPhase::Bind, phase_recorder);
        if let Some(statement) = prepared.get_for_current_route_map(&statement_name).cloned() {
            metrics::increment_prepared_event(PreparedEvent::Bind);
            prepared_snapshot_handle.increment_cache_hit();
            let mut prelude = Vec::new();
            if !prepared.is_materialized(backend_id, &statement) {
                prelude.push(build_parse_frame(
                    &statement.backend_name,
                    &statement.query,
                    &statement.parameter_type_oids,
                ));
                prepared.mark_materialized(backend_id, &statement);
                metrics::increment_prepared_event(PreparedEvent::Materialize);
                prepared_snapshot_handle.increment_materialization_count();
                publish_prepared_snapshot(prepared, prepared_snapshot_handle);
            }

            timer.finish(MetricOutcome::Ok);
            return Ok(PreparedForwardPlan {
                prelude,
                frame: rewrite_bind_statement_name(&frame, &statement.backend_name)?,
            });
        }
        timer.finish(MetricOutcome::Rejected);
    }

    if let Some(DescribeTarget::Statement(statement_name)) = parse_describe_target(&frame)? {
        let timer = PhaseTimer::start(ProtocolPhase::Bind, phase_recorder);
        if let Some(statement) = prepared.get_for_current_route_map(&statement_name).cloned() {
            prepared_snapshot_handle.increment_cache_hit();
            let mut prelude = Vec::new();
            if !prepared.is_materialized(backend_id, &statement) {
                prelude.push(build_parse_frame(
                    &statement.backend_name,
                    &statement.query,
                    &statement.parameter_type_oids,
                ));
                prepared.mark_materialized(backend_id, &statement);
                metrics::increment_prepared_event(PreparedEvent::Materialize);
                prepared_snapshot_handle.increment_materialization_count();
                publish_prepared_snapshot(prepared, prepared_snapshot_handle);
            }

            timer.finish(MetricOutcome::Ok);
            return Ok(PreparedForwardPlan {
                prelude,
                frame: rewrite_describe_statement_name(&frame, &statement.backend_name)?,
            });
        }
        timer.finish(MetricOutcome::Rejected);
    }

    if let Some(CloseTarget::Statement(statement_name)) = parse_close_target(&frame)? {
        let timer = PhaseTimer::start(ProtocolPhase::Close, phase_recorder);
        if let Some(statement) = prepared.remove(&statement_name) {
            metrics::increment_prepared_event(PreparedEvent::Close);
            publish_prepared_snapshot(prepared, prepared_snapshot_handle);
            timer.finish(MetricOutcome::Ok);
            return Ok(PreparedForwardPlan::single(rewrite_close_statement_name(
                &frame,
                &statement.backend_name,
            )?));
        }
        timer.finish(MetricOutcome::Rejected);
    }

    if frame.tag == u8::from(FrontendTag::Execute) {
        let timer = PhaseTimer::start(ProtocolPhase::Execute, phase_recorder);
        timer.finish(MetricOutcome::Ok);
    }

    Ok(PreparedForwardPlan::single(frame))
}

fn publish_prepared_snapshot(
    prepared: &PreparedCatalog,
    prepared_snapshot_handle: &PreparedSnapshotHandle,
) {
    prepared_snapshot_handle.set_statements(prepared.snapshot());
}

#[derive(Debug)]
struct PreparedForwardPlan {
    prelude: Vec<FrontendFrame>,
    frame: FrontendFrame,
}

impl PreparedForwardPlan {
    fn single(frame: FrontendFrame) -> Self {
        Self {
            prelude: Vec::new(),
            frame,
        }
    }
}

#[derive(Debug)]
enum ForwardOutcome {
    Ready(ReadyStatus),
    ClientDisconnectedAfterReady(ReadyStatus),
    AbandonedResponse { needs_sync: bool },
    BufferLimitExceeded,
}

fn should_sync_for_frames(frames: &[FrontendFrame]) -> bool {
    frames
        .iter()
        .any(|frame| frame.tag != u8::from(FrontendTag::Query))
}

fn simple_query_frame(sql: &str) -> FrontendFrame {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.put_u8(0);
    FrontendFrame {
        tag: u8::from(FrontendTag::Query),
        payload: payload.freeze(),
    }
}

fn replay_frames(session: &VirtualSession) -> Vec<FrontendFrame> {
    session
        .replay_sql()
        .into_iter()
        .map(|sql| simple_query_frame(&sql))
        .collect()
}

fn sync_frame() -> FrontendFrame {
    FrontendFrame {
        tag: u8::from(FrontendTag::Sync),
        payload: BytesMut::new().freeze(),
    }
}

fn update_transaction_state_from_request_plans(
    session: &mut VirtualSession,
    request_plans: &[RequestPlan<'_>],
    track_routing_state: bool,
) -> anyhow::Result<bool> {
    let mut committed_write_transaction = false;
    for request_plan in request_plans {
        if request_plan.updates_transaction_state {
            committed_write_transaction |= update_transaction_state_from_request_plan(
                session,
                request_plan,
                track_routing_state,
            );
        }
    }

    Ok(committed_write_transaction)
}

fn update_transaction_state_from_request_plan(
    session: &mut VirtualSession,
    request_plan: &RequestPlan<'_>,
    track_routing_state: bool,
) -> bool {
    let committed_write_transaction =
        session.apply_transaction_sql_with_routing(request_plan.sql.as_ref(), track_routing_state);
    if track_routing_state {
        update_transaction_shard_state_from_sql(session, request_plan.sql.as_ref());
    }
    committed_write_transaction
}

fn update_transaction_shard_state_from_sql(session: &mut VirtualSession, sql: &str) {
    if session.read_routing_transaction_state().is_none() {
        return;
    }

    let Some(shard_id) = transaction_shard_id_from_sql(sql) else {
        if session.transaction_shard_state().is_some() {
            session.mark_transaction_cross_shard_violation();
        }
        return;
    };

    let route_reason = session
        .current_transaction_route_reason()
        .unwrap_or(CoreRoutingReason::UnknownQuery);
    let decision = session.apply_transaction_shard_affinity(
        Some(shard_id),
        route_reason,
        MultiShardPolicy::Reject,
    );
    if matches!(
        decision,
        pg_kinetic_core::session::TransactionShardDecision::Rejected
    ) {
        session.mark_transaction_cross_shard_violation();
    }
}

fn transaction_shard_id_from_sql(sql: &str) -> Option<ShardId> {
    match extract_shard_hint(sql) {
        ShardHint::Shard(value) | ShardHint::Tenant(value) | ShardHint::Route(value) => {
            ShardId::new(value.as_ref()).ok()
        }
        ShardHint::None | ShardHint::Unknown => None,
    }
}

fn update_virtual_session_from_frame(
    session: &mut VirtualSession,
    frame: &FrontendFrame,
    route_application_name: &mut Option<String>,
    simple_query_command: Option<&SqlCommand>,
) -> anyhow::Result<()> {
    if let Some(command) = simple_query_command {
        match command {
            SqlCommand::Set {
                scope: SetScope::Session,
                key,
                value,
            } if key == "application_name" => {
                *route_application_name = Some(value.clone());
            }
            SqlCommand::Reset { key } if key == "application_name" => {
                *route_application_name = None;
            }
            SqlCommand::DiscardAll => {
                *route_application_name = None;
            }
            _ => {}
        }

        if !matches!(
            command,
            SqlCommand::Begin { .. }
                | SqlCommand::Commit
                | SqlCommand::Rollback
                | SqlCommand::SetTransaction { .. }
        ) {
            session.apply_sql(command.clone());
        }
    } else if [
        FrontendTag::Parse,
        FrontendTag::Bind,
        FrontendTag::Describe,
        FrontendTag::Execute,
        FrontendTag::Close,
        FrontendTag::Sync,
    ]
    .iter()
    .any(|tag| frame.tag == u8::from(*tag))
    {
        return Ok(());
    }

    Ok(())
}

async fn execute_backend_batch(
    backend: &mut PooledBackend,
    frames: &[FrontendFrame],
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<ReadyStatus> {
    let mut outbound = BytesMut::new();
    for frame in frames {
        outbound.extend_from_slice(&encode_frontend_frame(frame));
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(&outbound)
        .await
        .context("write backend batch")?;
    await_ready_status(backend, max_backend_buffer_bytes).await
}

async fn execute_simple_query(
    backend: &mut PooledBackend,
    sql: &str,
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<()> {
    let status = execute_backend_batch(
        backend,
        &[simple_query_frame(sql)],
        max_backend_buffer_bytes,
    )
    .await
    .with_context(|| format!("execute backend query {sql}"))?;
    anyhow::ensure!(
        status == ReadyStatus::Idle,
        "unexpected backend status after {sql}: {status:?}"
    );
    Ok(())
}

async fn probe_read_after_write_requirement(
    backend: &mut PooledBackend,
    probe_timeout: Duration,
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<PgLsn> {
    match timeout(probe_timeout, async {
        let frame = simple_query_frame("SELECT pg_current_wal_lsn()");
        backend
            .backend_mut()
            .stream_mut()
            .write_all(&encode_frontend_frame(&frame))
            .await
            .context("write read-after-write probe")?;

        let mut backend_buffer = BytesMut::with_capacity(16 * 1024);
        let mut probe_lsn = None;

        loop {
            if backend_buffer.len() >= max_backend_buffer_bytes {
                return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
            }

            let read = backend
                .backend_mut()
                .stream_mut()
                .read_buf(&mut backend_buffer)
                .await
                .context("read read-after-write probe response")?;
            if read == 0 {
                anyhow::bail!("backend disconnected during read-after-write probe");
            }

            if backend_buffer.len() > max_backend_buffer_bytes {
                return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
            }

            while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
                match frame.tag {
                    tag if tag == u8::from(BackendTag::DataRow) && probe_lsn.is_none() => {
                        probe_lsn = parse_read_after_write_lsn(&frame.payload)?;
                    }
                    tag if tag == u8::from(BackendTag::ErrorResponse) => {
                        anyhow::bail!("backend returned error during read-after-write probe");
                    }
                    tag if tag == u8::from(BackendTag::ReadyForQuery) => {
                        return probe_lsn.context("read-after-write probe returned no LSN");
                    }
                    _ => {}
                }
            }
        }
    })
    .await
    {
        Ok(result) => result,
        Err(_) => anyhow::bail!("read-after-write probe timed out"),
    }
}

fn parse_read_after_write_lsn(payload: &[u8]) -> anyhow::Result<Option<PgLsn>> {
    let mut payload = payload;
    if payload.remaining() < 2 {
        return Ok(None);
    }

    let columns = payload.get_i16();
    if columns <= 0 {
        return Ok(None);
    }

    if payload.remaining() < 4 {
        return Ok(None);
    }

    let length = payload.get_i32();
    if length < 0 {
        return Ok(None);
    }

    let length = length as usize;
    if payload.remaining() < length {
        return Ok(None);
    }

    let mut value = vec![0_u8; length];
    payload.copy_to_slice(&mut value);
    let value = std::str::from_utf8(&value).context("read-after-write probe row is not utf8")?;
    value
        .parse::<PgLsn>()
        .map(Some)
        .context("parse read-after-write LSN")
}

async fn await_ready_status(
    backend: &mut PooledBackend,
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<ReadyStatus> {
    let mut backend_buffer = BytesMut::with_capacity(16 * 1024);
    loop {
        if backend_buffer.len() >= max_backend_buffer_bytes {
            return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
        }

        let read = backend
            .backend_mut()
            .stream_mut()
            .read_buf(&mut backend_buffer)
            .await
            .context("read backend response")?;
        if read == 0 {
            anyhow::bail!("backend disconnected during response drain");
        }

        if backend_buffer.len() > max_backend_buffer_bytes {
            return Err(buffer_limit_exceeded(BufferBudgetKind::Backend));
        }

        let mut ready = None;
        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            if let Some(status) = frame.ready_status() {
                ready = Some(status);
            }
        }

        if let Some(status) = ready {
            return Ok(status);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn recover_backend(
    backend: &mut PooledBackend,
    route_key: RouteKey,
    session_id: u64,
    debug_sampler: DebugSampler,
    trigger: RecoveryTrigger,
    performance: &crate::config::PerformanceConfig,
    needs_sync: bool,
    session: &mut VirtualSession,
    max_backend_buffer_bytes: usize,
    recovery_snapshot_handle: &RecoverySnapshotHandle,
) -> anyhow::Result<bool> {
    let action = recovery_action(trigger, performance.recovery_mode);
    let recovered = timeout(performance.recovery_timeout(), async {
        match action {
            RecoveryAction::None => Ok(true),
            RecoveryAction::Rollback => {
                execute_simple_query(backend, "ROLLBACK", max_backend_buffer_bytes).await?;
                session.apply_sql(classify("rollback"));
                Ok(true)
            }
            RecoveryAction::DrainAndSync => {
                let status = if needs_sync {
                    execute_backend_batch(backend, &[sync_frame()], max_backend_buffer_bytes)
                        .await?
                } else {
                    await_ready_status(backend, max_backend_buffer_bytes).await?
                };
                anyhow::ensure!(
                    status == ReadyStatus::Idle,
                    "unexpected recovery status: {status:?}"
                );
                Ok(true)
            }
            RecoveryAction::RollbackAndDrain => {
                let status = if needs_sync {
                    execute_backend_batch(backend, &[sync_frame()], max_backend_buffer_bytes)
                        .await?
                } else {
                    await_ready_status(backend, max_backend_buffer_bytes).await?
                };
                anyhow::ensure!(
                    status == ReadyStatus::Idle,
                    "unexpected recovery status: {status:?}"
                );
                execute_simple_query(backend, "ROLLBACK", max_backend_buffer_bytes).await?;
                session.apply_sql(classify("rollback"));
                Ok(true)
            }
            RecoveryAction::Discard => Ok(false),
        }
    })
    .await;

    match recovered {
        Ok(Ok(reuse)) => {
            metrics::increment_recovery(trigger, action, "ok");
            recovery_snapshot_handle.record(trigger, action, MetricOutcome::Ok);
            telemetry::emit_debug_sample_with(&debug_sampler, session_id, || {
                DebugSample::recovery(session_id, route_key, trigger, action, MetricOutcome::Ok)
            });
            Ok(reuse)
        }
        Ok(Err(error)) => {
            if buffer_limit_kind(&error).is_some() {
                metrics::increment_recovery(trigger, action, "buffer_limit");
                recovery_snapshot_handle.record(trigger, action, MetricOutcome::Discarded);
                telemetry::emit_debug_sample_with(&debug_sampler, session_id, || {
                    DebugSample::recovery(
                        session_id,
                        route_key,
                        trigger,
                        action,
                        MetricOutcome::Discarded,
                    )
                });
                return Ok(false);
            }
            metrics::increment_recovery(trigger, action, "error");
            recovery_snapshot_handle.record(trigger, action, MetricOutcome::Error);
            recovery_snapshot_handle.set_last_error(
                trigger,
                action,
                MetricOutcome::Error,
                error.to_string(),
            );
            telemetry::emit_debug_sample_with(&debug_sampler, session_id, || {
                DebugSample::recovery(session_id, route_key, trigger, action, MetricOutcome::Error)
            });
            Err(error)
        }
        Err(_) => {
            metrics::increment_recovery(trigger, action, "timeout");
            recovery_snapshot_handle.record(trigger, action, MetricOutcome::Timeout);
            telemetry::emit_debug_sample_with(&debug_sampler, session_id, || {
                DebugSample::recovery(
                    session_id,
                    route_key,
                    trigger,
                    action,
                    MetricOutcome::Timeout,
                )
            });
            Ok(false)
        }
    }
}

async fn error_response_and_ready(
    client: &mut ClientConnection,
    qos: &crate::config::QosConfig,
    message: &'static str,
) -> anyhow::Result<()> {
    error_response_and_ready_with_state(
        client,
        &qos.overload_error_code,
        message,
        ReadyStatus::Idle,
    )
    .await
}

async fn error_response_and_ready_with_state(
    client: &mut ClientConnection,
    sqlstate: &str,
    message: &str,
    ready_status: ReadyStatus,
) -> anyhow::Result<()> {
    let error = build_error_response(sqlstate, message);
    let mut response = BytesMut::with_capacity(error.len() + 6);
    response.extend_from_slice(&error);
    response.extend_from_slice(&ready_for_query(ready_status));
    client
        .write_all(&response)
        .await
        .context("write error response and ready")
}

async fn error_response_only(
    client: &mut ClientConnection,
    sqlstate: &str,
    message: &str,
) -> anyhow::Result<()> {
    let error = build_error_response(sqlstate, message);
    client
        .write_all(&error)
        .await
        .context("write timeout error response")
}

async fn reject_client_during_drain(
    client: &mut ClientConnection,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<()> {
    let drain_timer = PhaseTimer::start(ProtocolPhase::Drain, phase_recorder);
    error_response_only(
        client,
        SqlState::OperatorIntervention.as_str(),
        "proxy is draining",
    )
    .await?;
    drain_timer.finish(MetricOutcome::Rejected);
    client.shutdown().await.context("shutdown draining client")
}

#[allow(clippy::too_many_arguments)]
async fn handle_query_timeout(
    client: &mut ClientConnection,
    performance: &crate::config::PerformanceConfig,
    mut backend: PooledBackend,
    session: &mut VirtualSession,
    pinned_backend: &mut PinnedBackend,
    snapshot_store: &SnapshotStore,
    session_id: u64,
    route_key: RouteKey,
    recovery_snapshot_handle: &RecoverySnapshotHandle,
    progress: QueryProgress,
    max_backend_buffer_bytes: usize,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
    debug_sampler: DebugSampler,
) -> anyhow::Result<bool> {
    metrics_crate::counter!(
        MetricName::TimeoutTotal.as_str(),
        "kind" => "query"
    )
    .increment(1);

    let recovery_trigger = match session.pin_reason() {
        Some(PinReason::OpenTransaction) | Some(PinReason::FailedTransaction) => {
            RecoveryTrigger::AbandonedTransaction
        }
        _ => RecoveryTrigger::AbandonedResponse,
    };

    if !progress.response_started {
        error_response_and_ready_with_state(
            client,
            SqlState::QueryCanceled.as_str(),
            "query timed out",
            ReadyStatus::Idle,
        )
        .await?;
    }

    let cancel_timer = PhaseTimer::start(ProtocolPhase::Cancel, phase_recorder);
    let reused = recover_backend(
        &mut backend,
        route_key,
        session_id,
        debug_sampler,
        recovery_trigger,
        performance,
        false,
        session,
        max_backend_buffer_bytes,
        recovery_snapshot_handle,
    )
    .await
    .unwrap_or(false);
    clear_pinned_backend(pinned_backend, snapshot_store, session_id);
    if reused {
        backend.release().await;
    } else {
        backend.discard();
    }
    cancel_timer.finish(MetricOutcome::Timeout);

    Ok(!progress.response_started)
}

async fn handle_idle_timeout(
    client: &mut ClientConnection,
    kind: IdleTimeoutKind,
) -> anyhow::Result<()> {
    metrics_crate::counter!(
        MetricName::TimeoutTotal.as_str(),
        "kind" => match kind {
            IdleTimeoutKind::Client => "idle_client",
            IdleTimeoutKind::Transaction => "idle_transaction",
        }
    )
    .increment(1);

    match kind {
        IdleTimeoutKind::Client => {
            error_response_and_ready_with_state(
                client,
                SqlState::OperatorIntervention.as_str(),
                "idle client timed out",
                ReadyStatus::Idle,
            )
            .await
        }
        IdleTimeoutKind::Transaction => {
            error_response_only(
                client,
                SqlState::OperatorIntervention.as_str(),
                "idle transaction timed out",
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finalize_backend_on_disconnect(
    mut backend: PooledBackend,
    route_pools: &Arc<RoutePools>,
    performance: &crate::config::PerformanceConfig,
    session: &mut VirtualSession,
    pinned_backend: &mut PinnedBackend,
    snapshot_store: &SnapshotStore,
    session_id: u64,
    route_key: RouteKey,
    recovery_snapshot_handle: &RecoverySnapshotHandle,
    qos: &crate::config::QosConfig,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
    debug_sampler: DebugSampler,
) -> anyhow::Result<()> {
    let close_timer = PhaseTimer::start(ProtocolPhase::Close, phase_recorder);
    match session.pin_reason() {
        Some(PinReason::OpenTransaction) | Some(PinReason::FailedTransaction) => {
            let reused = recover_backend(
                &mut backend,
                route_key.clone(),
                session_id,
                debug_sampler,
                RecoveryTrigger::AbandonedTransaction,
                performance,
                false,
                session,
                qos.max_backend_buffer_bytes,
                recovery_snapshot_handle,
            )
            .await
            .context("recover abandoned transaction")?;
            clear_pinned_backend(pinned_backend, snapshot_store, session_id);
            if reused {
                backend.release().await;
            } else {
                backend.discard();
            }
        }
        Some(PinReason::UnknownProtocolState) => {
            clear_pinned_backend(pinned_backend, snapshot_store, session_id);
            backend.discard();
        }
        Some(PinReason::Copy)
        | Some(PinReason::TempTable)
        | Some(PinReason::AdvisoryLock)
        | Some(PinReason::ListenNotify)
        | Some(PinReason::SessionState) => {
            clear_pinned_backend(pinned_backend, snapshot_store, session_id);
            backend.discard();
        }
        None => {
            if session.has_replayable_settings() {
                let reset_timer = PhaseTimer::start(ProtocolPhase::Reset, phase_recorder);
                execute_simple_query(
                    &mut backend,
                    route_pools.primary().reset_query(),
                    qos.max_backend_buffer_bytes,
                )
                .await
                .context("reset backend during disconnect cleanup")?;
                reset_timer.finish(MetricOutcome::Ok);
            }
            clear_pinned_backend(pinned_backend, snapshot_store, session_id);
            backend.release().await;
        }
    }

    close_timer.finish(MetricOutcome::Ok);
    Ok(())
}

fn should_replay_session(
    session: &VirtualSession,
    pinned_backend: &PinnedBackend,
    backend_id: u64,
) -> bool {
    session.has_replayable_settings() && pinned_backend.backend_id() != Some(backend_id)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BufferBudgetKind {
    Client,
    Backend,
}

impl BufferBudgetKind {
    #[must_use]
    const fn metric_label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Backend => "backend",
        }
    }
}

impl std::fmt::Display for BufferBudgetKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.metric_label())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{kind} buffer limit exceeded")]
struct BufferLimitExceeded {
    kind: BufferBudgetKind,
}

fn buffer_limit_exceeded(kind: BufferBudgetKind) -> anyhow::Error {
    record_buffer_limit(kind);
    BufferLimitExceeded { kind }.into()
}

fn buffer_limit_kind(error: &anyhow::Error) -> Option<BufferBudgetKind> {
    error
        .downcast_ref::<BufferLimitExceeded>()
        .map(|error| error.kind)
}

fn record_buffer_limit(kind: BufferBudgetKind) {
    metrics_crate::counter!(
        MetricName::BufferLimitTotal.as_str(),
        "kind" => kind.metric_label()
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::auth_request_expects_client_response;

    fn auth_payload(code: i32) -> [u8; 4] {
        code.to_be_bytes()
    }

    #[test]
    fn sasl_start_and_continue_expect_client_responses() {
        assert!(auth_request_expects_client_response(&auth_payload(10)).unwrap());
        assert!(auth_request_expects_client_response(&auth_payload(11)).unwrap());
    }

    #[test]
    fn sasl_final_and_ok_do_not_expect_client_responses() {
        assert!(!auth_request_expects_client_response(&auth_payload(12)).unwrap());
        assert!(!auth_request_expects_client_response(&auth_payload(0)).unwrap());
    }
}
