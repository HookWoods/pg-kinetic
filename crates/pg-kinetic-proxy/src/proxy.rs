use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    io::IoSlice,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, RwLock, Semaphore},
    task::{JoinHandle, JoinSet},
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
    backend_query::AuthQueryService,
    buffers::{ProxyBufferPool, SessionBufferSet},
    cancel,
    config::{Config, PoolConfig, RouteConfig},
    drain::DrainController,
    health,
    lifecycle::{
        wait_for_shutdown_signal, LifecycleController, ShutdownCoordinator, ShutdownOutcome,
    },
    metrics,
    mirror::{MirrorDispatcher, MirrorOutcomeRecorder, MirrorTask},
    pause::PauseController,
    pool::{
        BackendPool, BackendPoolRef, CheckoutMode as PoolCheckoutMode, PooledBackend,
        ReplicaSelectionStrategy, ReplicaSelector, RoutePoolRegistry, RoutePoolRetirementTargets,
        RoutePools,
    },
    reload,
    snapshot::{
        ClientSnapshot, ClientSnapshotHandle, LimitsSnapshot, PinningSnapshot,
        PreparedSnapshotHandle, RecoverySnapshotHandle, RouteCheckoutSnapshot,
        RuntimeShardSnapshot, SettingsSnapshot, SnapshotStore,
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
    runtime::{RuntimeLifecycleState, ShutdownReason},
    session::PinReason as SessionPinReason,
    session::TransactionState,
    shard_extract::{extract_shard_hint, ShardHint},
    sharding::{MultiShardPolicy, ShardId},
    sql::{classify, SetScope, SqlCommand},
    sql_classify::{analyze_sql, SqlAnalysis},
    virtual_session::{PinReason, ReadAfterWriteState, VirtualSession},
};
use pg_kinetic_wire::{
    backend::{
        build_error_response, encode_backend_key_data, encode_parameter_status,
        parse_backend_frame, parse_parameter_status, BackendFrame, ReadyStatus,
    },
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

mod backend_startup;
mod buffer_limit;
mod checkout;
mod client_io;
mod client_session;
mod connection;
mod forwarding;
mod recovery;
mod request_plan;
mod session_snapshot;

use backend_startup::*;
use buffer_limit::*;
use checkout::*;
pub use checkout::{
    apply_policy_action_to_routing_target_with_mode, apply_policy_after_routing_target,
    apply_policy_before_checkout_target, apply_policy_before_routing_target, checkout_debug_fields,
    checkout_postgres_error_for_target, policy_audit_event_from_decision,
    route_checkout_snapshot_for_target,
};
use client_io::{
    bind_cancel_target, discard_backend_with_cancel_unbind, next_client_cycle,
    read_startup_packet_with_buffer, release_backend_with_cancel_unbind, CancelSessionGuard,
    ClientCycle, IdleTimeoutKind, QueryProgress,
};
pub(crate) use client_io::{read_startup_packet, StartupRead};
use client_session::{handle_client, ClientSessionContext};
pub(crate) use connection::ClientConnection;
use connection::{backend_failure, BackendFailure};
pub use connection::{retry_disposition, BackendFailureKind, RetryDisposition};
use forwarding::*;
use recovery::*;
use request_plan::*;
use session_snapshot::*;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
const CANNOT_CONNECT_NOW_SQLSTATE: &str = "57P03";
const CONNECTION_FAILURE_SQLSTATE: &str = "08006";
const INVALID_CATALOG_NAME_SQLSTATE: &str = "3D000";
const REPLICA_UNAVAILABLE_MESSAGE: &str = "no healthy replica available";
const POOL_WARMUP_MIN_IDLE_BACKENDS: usize = 2;
const SQL_PLAN_CACHE_CAPACITY: usize = 4096;

#[derive(Debug)]
pub struct Proxy {
    config: Config,
    buffer_pool: ProxyBufferPool,
    client_slots: Arc<Semaphore>,
    backend_slots: Arc<Semaphore>,
    lifecycle: LifecycleController,
    snapshot_store: SnapshotStore,
    cancel_registry: Arc<cancel::CancelRegistry>,
    pause: Arc<PauseController>,
}

struct ControlPlaneHandles {
    _health_handle: Option<JoinHandle<()>>,
    _admin_handle: Option<JoinHandle<()>>,
    _reload_handle: Option<JoinHandle<()>>,
    _adaptive_handle: Option<JoinHandle<()>>,
}

pub(crate) struct ShardContext {
    pub shard_id: usize,
    pub listener: TcpListener,
    pub route_pool_selector: RoutePoolSelector,
    pub active_config: Arc<RwLock<Config>>,
    pub backend_credentials: reload::BackendCredentialCache,
    pub snapshot_store: SnapshotStore,
    pub client_slots: Arc<Semaphore>,
    pub lifecycle: LifecycleController,
    pub buffer_pool: ProxyBufferPool,
    pub mirror_dispatcher: Arc<MirrorDispatcher>,
    pub routing_planner: ReadRoutingPlanner,
    pub cancel_registry: Arc<cancel::CancelRegistry>,
    pub pause: Arc<PauseController>,
    pub auth_query_service: Arc<AuthQueryService>,
    pub reject_phase_recorder: Arc<dyn telemetry::PhaseTimingRecorder>,
    pub debug_sampler: DebugSampler,
    pub phase_metrics_enabled: bool,
    pub phase_timing_sample_rate: f64,
    pub shutdown_completion: watch::Receiver<Option<ShutdownOutcome>>,
    pub runtime_shard_core_id: Option<usize>,
    pub runtime_shard_observability: bool,
}

struct ProxyRuntimeState {
    effective_config: Config,
    phase_metrics_enabled: bool,
    phase_timing_sample_rate: f64,
    reject_phase_recorder: Arc<dyn telemetry::PhaseTimingRecorder>,
    debug_sampler: DebugSampler,
    active_config: Arc<RwLock<Config>>,
    backend_credentials: reload::BackendCredentialCache,
    default_route_config: RouteConfig,
    route_pool_retirement_targets: RoutePoolRetirementTargets,
    control_route_config: RouteConfig,
    mirror_outcome_recorder: MirrorOutcomeRecorder,
    mirror_dispatcher: Arc<MirrorDispatcher>,
    route_pool_selector: RoutePoolSelector,
    control_route_pools: Arc<RoutePools>,
    routing_planner: ReadRoutingPlanner,
    auth_query_service: Arc<AuthQueryService>,
}

#[derive(Clone, Debug)]
pub(crate) struct RoutePoolSelector {
    default_pools: Option<Arc<RoutePools>>,
    registry: Arc<RoutePoolRegistry>,
}

impl RoutePoolSelector {
    #[must_use]
    fn default(default_pools: Arc<RoutePools>) -> Self {
        Self {
            default_pools: Some(default_pools),
            registry: Arc::new(RoutePoolRegistry::new()),
        }
    }

    #[must_use]
    fn configured(registry: Arc<RoutePoolRegistry>) -> Self {
        Self {
            default_pools: None,
            registry,
        }
    }

    #[must_use]
    fn resolve(&self, route: &RouteKey) -> Option<Arc<RoutePools>> {
        self.registry
            .route_pools(route)
            .map(Arc::new)
            .or_else(|| self.default_pools.as_ref().map(Arc::clone))
    }

    #[must_use]
    pub(crate) fn registry(&self) -> &Arc<RoutePoolRegistry> {
        &self.registry
    }

    fn register_retirement_target(&self, targets: &RoutePoolRetirementTargets) {
        if let Some(default_pools) = &self.default_pools {
            targets.register_pools(Arc::clone(default_pools));
        } else {
            targets.register_registry(Arc::clone(&self.registry));
        }
    }
}

impl Proxy {
    #[must_use]
    pub fn new(config: Config) -> Self {
        let client_slots = Arc::new(Semaphore::new(config.capacity.max_clients));
        let backend_slots = Arc::new(Semaphore::new(config.capacity.max_backends));
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
            backend_slots,
            lifecycle,
            snapshot_store,
            cancel_registry: Arc::new(cancel::CancelRegistry::default()),
            pause: Arc::new(PauseController::default()),
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
        let state = self.initialize_runtime_state().await?;

        let listener = TcpListener::bind(state.effective_config.connection.listen_addr)
            .await
            .with_context(|| {
                format!(
                    "bind listener {}",
                    state.effective_config.connection.listen_addr
                )
            })?;

        let drain = self.lifecycle.drain_controller();
        let _control_plane_handles = run_control_plane(
            &state.effective_config,
            self.config.clone(),
            Arc::clone(&state.active_config),
            state.backend_credentials.clone(),
            state.route_pool_retirement_targets.clone(),
            Arc::clone(&state.control_route_pools),
            Arc::clone(state.route_pool_selector.registry()),
            &state.control_route_config,
            Arc::clone(&drain),
            Arc::clone(&self.pause),
            self.lifecycle.clone(),
            self.snapshot_store.clone(),
            state.mirror_outcome_recorder.clone(),
        )
        .await?;

        tracing::info!(listen_addr = %state.effective_config.connection.listen_addr, "listening");

        let (shutdown_completion_tx, shutdown_completion_rx) = watch::channel(None);
        let shard_context = ShardContext {
            shard_id: 0,
            listener,
            route_pool_selector: state.route_pool_selector.clone(),
            active_config: Arc::clone(&state.active_config),
            backend_credentials: state.backend_credentials.clone(),
            snapshot_store: self.snapshot_store.clone(),
            client_slots: Arc::clone(&self.client_slots),
            lifecycle: self.lifecycle.clone(),
            buffer_pool: self.buffer_pool.clone(),
            mirror_dispatcher: Arc::clone(&state.mirror_dispatcher),
            routing_planner: state.routing_planner,
            cancel_registry: Arc::clone(&self.cancel_registry),
            pause: Arc::clone(&self.pause),
            auth_query_service: Arc::clone(&state.auth_query_service),
            reject_phase_recorder: Arc::clone(&state.reject_phase_recorder),
            debug_sampler: state.debug_sampler,
            phase_metrics_enabled: state.phase_metrics_enabled,
            phase_timing_sample_rate: state.phase_timing_sample_rate,
            shutdown_completion: shutdown_completion_rx,
            runtime_shard_core_id: None,
            runtime_shard_observability: false,
        };
        let mut shard = Box::pin(run_shard(shard_context));
        let mut shutdown = Box::pin(wait_for_shutdown_signal());
        let mut drain_start_wait = Box::pin(drain.wait_for_drain_start());
        let mut shutdown_coordinator = None;
        let mut shutdown_completion: Pin<Box<dyn Future<Output = ShutdownOutcome> + Send>> =
            Box::pin(std::future::pending());
        let mut shutdown_started = false;

        loop {
            tokio::select! {
                biased;
                result = &mut shard => return result,
                result = &mut shutdown, if !shutdown_started => {
                    let reason = result.context("wait for shutdown signal")?;
                    if self.lifecycle.begin_drain(reason) {
                        tracing::info!("received shutdown signal; beginning drain");
                    }
                    start_shutdown_coordinator(
                        self.lifecycle.clone(),
                        &mut shutdown_coordinator,
                        &mut shutdown_completion,
                        &mut shutdown_started,
                    );
                }
                _ = &mut drain_start_wait, if !shutdown_started => {
                    self.lifecycle.begin_drain(ShutdownReason::AdminRequest);
                    start_shutdown_coordinator(
                        self.lifecycle.clone(),
                        &mut shutdown_coordinator,
                        &mut shutdown_completion,
                        &mut shutdown_started,
                    );
                }
                outcome = &mut shutdown_completion, if shutdown_started => {
                    let _ = shutdown_completion_tx.send(Some(outcome));
                    if let Some(coordinator) = shutdown_coordinator.take() {
                        coordinator.complete();
                    }
                    return shard.await;
                }
            }
        }
    }

    pub fn run_thread_per_core(self) -> anyhow::Result<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build thread-per-core control runtime")?
            .block_on(self.run_thread_per_core_inner())
    }

    async fn run_thread_per_core_inner(self) -> anyhow::Result<()> {
        let state = self.initialize_runtime_state().await?;
        let listen_addr =
            resolve_runtime_listen_addr(state.effective_config.connection.listen_addr)?;
        let shard_count = resolve_runtime_shard_count(&state.effective_config)?;
        let core_assignments = runtime_shard_core_assignments(shard_count);
        let (shutdown_completion_tx, shutdown_completion_rx) = watch::channel(None);
        let (startup_tx, startup_rx) = std::sync::mpsc::channel();
        let (shard_result_tx, mut shard_result_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut shard_threads = Vec::with_capacity(shard_count);

        for (shard_id, core_id) in core_assignments.into_iter().enumerate() {
            let startup_tx = startup_tx.clone();
            let shard_result_tx = shard_result_tx.clone();
            let shutdown_completion = shutdown_completion_rx.clone();
            let (route_pool_selector, _shard_control_route_pools) = build_route_pool_selector(
                &state.effective_config,
                &state.default_route_config,
                self.snapshot_store.clone(),
                Arc::clone(&self.backend_slots),
            );
            route_pool_selector.register_retirement_target(&state.route_pool_retirement_targets);
            let active_config = Arc::clone(&state.active_config);
            let backend_credentials = state.backend_credentials.clone();
            let snapshot_store = self.snapshot_store.clone();
            let client_slots = Arc::clone(&self.client_slots);
            let lifecycle = self.lifecycle.clone();
            let buffer_pool = self.buffer_pool.clone();
            let mirror_dispatcher = Arc::clone(&state.mirror_dispatcher);
            let routing_planner = state.routing_planner;
            let cancel_registry = Arc::clone(&self.cancel_registry);
            let pause = Arc::clone(&self.pause);
            let auth_query_service = Arc::clone(&state.auth_query_service);
            let reject_phase_recorder = Arc::clone(&state.reject_phase_recorder);
            let debug_sampler = state.debug_sampler;
            let phase_metrics_enabled = state.phase_metrics_enabled;
            let phase_timing_sample_rate = state.phase_timing_sample_rate;
            let core_label = core_id.map(|core| core.id);
            let thread_name = match core_label {
                Some(core_id) => format!("pg-kinetic-shard-{shard_id}-core-{core_id}"),
                None => format!("pg-kinetic-shard-{shard_id}"),
            };

            let thread = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || {
                    if let Some(core_id) = core_id {
                        if !core_affinity::set_for_current(core_id) {
                            tracing::warn!(
                                shard_id,
                                core_id = core_id.id,
                                "failed to pin runtime shard; continuing unpinned"
                            );
                        }
                    } else {
                        tracing::warn!(shard_id, "no CPU affinity target for runtime shard");
                    }

                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let message = format!(
                                "build current-thread runtime for shard {shard_id}: {error}"
                            );
                            let _ = startup_tx.send(Err(message.clone()));
                            let _ = shard_result_tx.send((shard_id, Err(anyhow::anyhow!(message))));
                            return;
                        }
                    };

                    let startup_sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let startup_sent_by_future = Arc::clone(&startup_sent);
                    let startup_tx_by_future = startup_tx.clone();
                    let result = runtime.block_on(async move {
                        let listener = socket::bind_reuseport_listener(listen_addr, 1024)
                            .with_context(|| {
                                format!("bind reuseport listener for shard {shard_id}")
                            })?;
                        snapshot_store.set_runtime_shard_snapshot(RuntimeShardSnapshot::new(
                            shard_id,
                            core_label,
                            RuntimeLifecycleState::Ready,
                            0,
                            0,
                        ));
                        startup_sent_by_future.store(true, Ordering::Release);
                        let _ = startup_tx_by_future.send(Ok(shard_id));
                        let shard_context = ShardContext {
                            shard_id,
                            listener,
                            route_pool_selector,
                            active_config,
                            backend_credentials,
                            snapshot_store,
                            client_slots,
                            lifecycle,
                            buffer_pool,
                            mirror_dispatcher,
                            routing_planner,
                            cancel_registry,
                            pause,
                            auth_query_service,
                            reject_phase_recorder,
                            debug_sampler,
                            phase_metrics_enabled,
                            phase_timing_sample_rate,
                            shutdown_completion,
                            runtime_shard_core_id: core_label,
                            runtime_shard_observability: true,
                        };
                        run_shard(shard_context).await
                    });

                    if !startup_sent.load(Ordering::Acquire) {
                        let _ = startup_tx.send(Err(format!(
                            "runtime shard {shard_id} failed before startup completed"
                        )));
                    }
                    let _ = shard_result_tx.send((shard_id, result));
                })
                .with_context(|| format!("spawn runtime shard thread {shard_id}"))?;
            shard_threads.push(thread);
        }

        drop(startup_tx);
        drop(shard_result_tx);

        for _ in 0..shard_count {
            match startup_rx
                .recv_timeout(Duration::from_secs(5))
                .context("wait for runtime shard startup")?
            {
                Ok(shard_id) => {
                    tracing::debug!(shard_id, "runtime shard listener started");
                }
                Err(message) => {
                    self.lifecycle.begin_drain(ShutdownReason::StartupFailure);
                    let coordinator = ShutdownCoordinator::new(self.lifecycle.clone());
                    let outcome = coordinator.coordinate().await;
                    let _ = shutdown_completion_tx.send(Some(outcome));
                    coordinator.complete();
                    join_runtime_shards(shard_threads)?;
                    anyhow::bail!(message);
                }
            }
        }

        let drain = self.lifecycle.drain_controller();
        let _control_plane_handles = run_control_plane(
            &state.effective_config,
            self.config.clone(),
            Arc::clone(&state.active_config),
            state.backend_credentials.clone(),
            state.route_pool_retirement_targets.clone(),
            Arc::clone(&state.control_route_pools),
            Arc::clone(state.route_pool_selector.registry()),
            &state.control_route_config,
            Arc::clone(&drain),
            Arc::clone(&self.pause),
            self.lifecycle.clone(),
            self.snapshot_store.clone(),
            state.mirror_outcome_recorder.clone(),
        )
        .await?;

        tracing::info!(
            listen_addr = %listen_addr,
            shards = shard_count,
            "thread-per-core runtime listening"
        );

        let mut shutdown = Box::pin(wait_for_shutdown_signal());
        let mut drain_start_wait = Box::pin(drain.wait_for_drain_start());
        let mut shutdown_coordinator = None;
        let mut shutdown_completion: Pin<Box<dyn Future<Output = ShutdownOutcome> + Send>> =
            Box::pin(std::future::pending());
        let mut shutdown_started = false;
        let mut shard_failure = None;

        loop {
            tokio::select! {
                biased;
                result = shard_result_rx.recv() => {
                    if let Some((shard_id, result)) = result {
                        match result {
                            Ok(()) => {
                                if !shutdown_started {
                                    shard_failure = Some(anyhow::anyhow!("runtime shard {shard_id} exited before shutdown"));
                                    self.lifecycle.begin_drain(ShutdownReason::RuntimeFailure);
                                    start_shutdown_coordinator(
                                        self.lifecycle.clone(),
                                        &mut shutdown_coordinator,
                                        &mut shutdown_completion,
                                        &mut shutdown_started,
                                    );
                                }
                            }
                            Err(error) => {
                                if !shutdown_started {
                                    shard_failure = Some(error.context(format!("runtime shard {shard_id} failed")));
                                    self.lifecycle.begin_drain(ShutdownReason::RuntimeFailure);
                                    start_shutdown_coordinator(
                                        self.lifecycle.clone(),
                                        &mut shutdown_coordinator,
                                        &mut shutdown_completion,
                                        &mut shutdown_started,
                                    );
                                }
                            }
                        }
                    }
                }
                result = &mut shutdown, if !shutdown_started => {
                    let reason = result.context("wait for shutdown signal")?;
                    if self.lifecycle.begin_drain(reason) {
                        tracing::info!("received shutdown signal; beginning drain");
                    }
                    start_shutdown_coordinator(
                        self.lifecycle.clone(),
                        &mut shutdown_coordinator,
                        &mut shutdown_completion,
                        &mut shutdown_started,
                    );
                }
                _ = &mut drain_start_wait, if !shutdown_started => {
                    self.lifecycle.begin_drain(ShutdownReason::AdminRequest);
                    start_shutdown_coordinator(
                        self.lifecycle.clone(),
                        &mut shutdown_coordinator,
                        &mut shutdown_completion,
                        &mut shutdown_started,
                    );
                }
                outcome = &mut shutdown_completion, if shutdown_started => {
                    let _ = shutdown_completion_tx.send(Some(outcome));
                    if let Some(coordinator) = shutdown_coordinator.take() {
                        coordinator.complete();
                    }
                    join_runtime_shards(shard_threads)?;
                    if let Some(error) = shard_failure {
                        return Err(error);
                    }
                    return Ok(());
                }
            }
        }
    }

    async fn initialize_runtime_state(&self) -> anyhow::Result<ProxyRuntimeState> {
        let effective_config = reload::load_effective_config(&self.config)?;
        effective_config.validate().map_err(anyhow::Error::msg)?;
        reload::validate_runtime_assets(&effective_config)?;
        self.lifecycle.configure(
            effective_config.drain.drain_timeout(),
            effective_config.runtime.lifecycle.shutdown_grace(),
            effective_config
                .runtime
                .lifecycle
                .readiness_fail_during_drain,
        );
        let phase_metrics_enabled = effective_config.observability.metrics_addr.is_some();
        let phase_timing_sample_rate = effective_config.observability.phase_timing_sample_rate();
        let reject_phase_recorder = telemetry::sampled_phase_timing_recorder(
            phase_metrics_enabled,
            phase_timing_sample_rate,
            0,
        );
        let debug_sampler =
            DebugSampler::new(effective_config.observability.trace_sampling_ratio());
        self.snapshot_store
            .set_settings_snapshot(SettingsSnapshot::from_config(&effective_config));
        self.snapshot_store
            .set_limits_snapshot(LimitsSnapshot::from_config(&effective_config));
        let active_config = Arc::new(RwLock::new(effective_config.clone()));
        let backend_credentials = reload::BackendCredentialCache::from_config(&effective_config)?;
        let auth_query_service = Arc::new(AuthQueryService::new(
            effective_config.connection.backend_addr,
            effective_config.tls.clone(),
            effective_config.socket.clone(),
            backend_credentials.clone(),
        ));
        let route_config = effective_config
            .effective_routes()
            .into_iter()
            .next()
            .context("missing effective route config")?;
        let control_route_config = control_route_config(&effective_config, &route_config);
        let mirror_outcome_recorder = MirrorOutcomeRecorder::default();
        let mirror_dispatcher = Arc::new(MirrorDispatcher::disabled(
            control_route_config.primary.address,
            effective_config.tls.clone(),
            effective_config.socket.clone(),
            mirror_outcome_recorder.clone(),
        ));
        let (route_pool_selector, control_route_pools) = build_route_pool_selector(
            &effective_config,
            &route_config,
            self.snapshot_store.clone(),
            Arc::clone(&self.backend_slots),
        );
        let route_pool_retirement_targets = RoutePoolRetirementTargets::new();
        route_pool_selector.register_retirement_target(&route_pool_retirement_targets);
        self.lifecycle.mark_backend_pools_initialized();
        let routing_planner = ReadRoutingPlanner::new(
            route_config.read_routing.read_routing_mode,
            route_config.read_routing.fallback_policy,
            route_config.freshness.freshness_policy,
            route_config.freshness.max_replica_lag_ms,
        );

        Ok(ProxyRuntimeState {
            effective_config,
            phase_metrics_enabled,
            phase_timing_sample_rate,
            reject_phase_recorder,
            debug_sampler,
            active_config,
            backend_credentials,
            default_route_config: route_config,
            route_pool_retirement_targets,
            control_route_config,
            mirror_outcome_recorder,
            mirror_dispatcher,
            route_pool_selector,
            control_route_pools,
            routing_planner,
            auth_query_service,
        })
    }
}

fn resolve_runtime_listen_addr(addr: SocketAddr) -> anyhow::Result<SocketAddr> {
    if addr.port() != 0 {
        return Ok(addr);
    }

    let listener = std::net::TcpListener::bind(addr)
        .with_context(|| format!("reserve runtime listener port for {addr}"))?;
    listener.local_addr().context("read reserved listener addr")
}

fn resolve_runtime_shard_count(config: &Config) -> anyhow::Result<usize> {
    let shard_count = match config.runtime.engine.runtime_shards {
        Some(shards) => shards,
        None => core_affinity::get_core_ids()
            .map(|cores| cores.len())
            .filter(|cores| *cores > 0)
            .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
            .unwrap_or(1),
    };

    if shard_count == 0 {
        anyhow::bail!("runtime_shards must be greater than zero");
    }

    Ok(shard_count)
}

fn runtime_shard_core_assignments(shard_count: usize) -> Vec<Option<core_affinity::CoreId>> {
    let Some(cores) = core_affinity::get_core_ids().filter(|cores| !cores.is_empty()) else {
        return vec![None; shard_count];
    };

    (0..shard_count)
        .map(|shard_id| cores.get(shard_id % cores.len()).copied())
        .collect()
}

fn join_runtime_shards(shards: Vec<std::thread::JoinHandle<()>>) -> anyhow::Result<()> {
    for shard in shards {
        shard
            .join()
            .map_err(|panic| anyhow::anyhow!("runtime shard thread panicked: {panic:?}"))?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_control_plane(
    effective_config: &Config,
    base_config: Config,
    active_config: Arc<RwLock<Config>>,
    backend_credentials: reload::BackendCredentialCache,
    route_pool_retirement_targets: RoutePoolRetirementTargets,
    route_pools: Arc<RoutePools>,
    route_pool_registry: Arc<RoutePoolRegistry>,
    route_config: &RouteConfig,
    drain: Arc<DrainController>,
    pause: Arc<PauseController>,
    lifecycle: LifecycleController,
    snapshot_store: SnapshotStore,
    mirror_outcome_recorder: MirrorOutcomeRecorder,
) -> anyhow::Result<ControlPlaneHandles> {
    let health_handle = if let Some(health_addr) = effective_config.health.health_addr {
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
    let admin_handle = if let Some(admin_addr) = effective_config.admin.admin_addr {
        Some(
            admin::spawn(
                admin_addr,
                effective_config.clone(),
                base_config.clone(),
                Arc::clone(&active_config),
                Arc::clone(&route_pools),
                Arc::clone(&route_pool_registry),
                backend_credentials.clone(),
                route_pool_retirement_targets.clone(),
                Arc::clone(&drain),
                Arc::clone(&pause),
                snapshot_store.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    lifecycle.mark_listeners_initialized();

    let reload_handle = if effective_config.reload.reload_enabled
        && effective_config.reload.config_file.is_some()
    {
        let reload_config = effective_config.reload.clone();
        let active_config = Arc::clone(&active_config);
        Some(tokio::spawn(async move {
            reload::spawn_reload_loop(
                base_config,
                reload_config,
                active_config,
                route_pools,
                route_pool_registry,
                backend_credentials,
                route_pool_retirement_targets,
            )
            .await;
        }))
    } else {
        None
    };

    let adaptive_handle = if effective_config.runtime.production.adaptive_enabled {
        let controller = AdaptiveController::new(
            snapshot_store,
            mirror_outcome_recorder,
            Arc::clone(&active_config),
        );
        Some(tokio::spawn(async move {
            controller.run().await;
        }))
    } else {
        None
    };

    Ok(ControlPlaneHandles {
        _health_handle: health_handle,
        _admin_handle: admin_handle,
        _reload_handle: reload_handle,
        _adaptive_handle: adaptive_handle,
    })
}

fn start_shutdown_coordinator(
    lifecycle: LifecycleController,
    shutdown_coordinator: &mut Option<ShutdownCoordinator>,
    shutdown_completion: &mut Pin<Box<dyn Future<Output = ShutdownOutcome> + Send>>,
    shutdown_started: &mut bool,
) {
    if *shutdown_started {
        return;
    }

    let coordinator = ShutdownCoordinator::new(lifecycle);
    let task_coordinator = coordinator.clone();
    *shutdown_completion = Box::pin(async move { task_coordinator.coordinate().await });
    *shutdown_coordinator = Some(coordinator);
    *shutdown_started = true;
}

async fn run_shard(mut ctx: ShardContext) -> anyhow::Result<()> {
    let drain = ctx.lifecycle.drain_controller();
    let mut drain_start_wait = Box::pin(drain.wait_for_drain_start());
    let mut client_tasks = JoinSet::new();
    let mut draining = false;
    let mut accepted_connections = 0_u64;

    tracing::debug!(shard_id = ctx.shard_id, "runtime shard started");
    record_runtime_shard_snapshot(
        &ctx,
        RuntimeLifecycleState::Ready,
        accepted_connections,
        client_tasks.len(),
    );

    loop {
        if draining {
            loop {
                tokio::select! {
                    biased;
                    changed = ctx.shutdown_completion.changed() => {
                        changed.context("watch shutdown completion")?;
                        let Some(outcome) = *ctx.shutdown_completion.borrow() else {
                            continue;
                        };
                        if outcome.forced_sessions() > 0 {
                            tracing::warn!(
                                shard_id = ctx.shard_id,
                                active_clients = outcome.forced_sessions(),
                                "shutdown grace expired; force-closing client sessions"
                            );
                            client_tasks.abort_all();
                            while let Some(result) = client_tasks.join_next().await {
                                if let Err(error) = result {
                                    if !error.is_cancelled() {
                                        tracing::warn!(shard_id = ctx.shard_id, error = %error, "client task failed during shutdown");
                                    }
                                }
                            }
                            record_runtime_shard_snapshot(
                                &ctx,
                                RuntimeLifecycleState::Stopped,
                                accepted_connections,
                                client_tasks.len(),
                            );
                        } else {
                            tracing::info!(
                                shard_id = ctx.shard_id,
                                active_clients = drain.active_clients(),
                                "drain completed"
                            );
                            record_runtime_shard_snapshot(
                                &ctx,
                                RuntimeLifecycleState::Stopped,
                                accepted_connections,
                                client_tasks.len(),
                            );
                        }
                        return Ok(());
                    }
                    joined = client_tasks.join_next(), if !client_tasks.is_empty() => {
                        if let Some(Err(error)) = joined {
                            tracing::warn!(shard_id = ctx.shard_id, error = %error, "client task failed");
                        }
                        record_runtime_shard_snapshot(
                            &ctx,
                            RuntimeLifecycleState::Draining,
                            accepted_connections,
                            client_tasks.len(),
                        );
                    }
                    accept = ctx.listener.accept() => {
                        let (client, client_addr) = accept.context("accept draining client")?;
                        accepted_connections = accepted_connections.saturating_add(1);
                        record_runtime_shard_snapshot(
                            &ctx,
                            RuntimeLifecycleState::Draining,
                            accepted_connections,
                            client_tasks.len(),
                        );
                        let config_snapshot = ctx.active_config.read().await.clone();
                        let socket_options = socket::SocketOptions::from(&config_snapshot.socket);
                        socket::apply_socket_options(&client, &socket_options, "client")
                            .context("apply draining client socket options")?;
                        let mut client = ClientConnection::new(client);
                        metrics::increment_client_connections();
                        reject_client_during_drain(&mut client, ctx.reject_phase_recorder.as_ref())
                            .await?;
                        tracing::info!(shard_id = ctx.shard_id, %client_addr, "rejected client during drain");
                    }
                }
            }
        }

        tokio::select! {
            biased;
            _ = &mut drain_start_wait => {
                ctx.lifecycle.begin_drain(ShutdownReason::AdminRequest);
                draining = true;
                record_runtime_shard_snapshot(
                    &ctx,
                    RuntimeLifecycleState::Draining,
                    accepted_connections,
                    client_tasks.len(),
                );
            }
            joined = client_tasks.join_next(), if !client_tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::warn!(shard_id = ctx.shard_id, error = %error, "client task failed");
                }
                record_runtime_shard_snapshot(
                    &ctx,
                    RuntimeLifecycleState::Ready,
                    accepted_connections,
                    client_tasks.len(),
                );
            }
            accept = ctx.listener.accept() => {
                let (client, client_addr) = accept.context("accept client")?;
                accepted_connections = accepted_connections.saturating_add(1);
                let config_snapshot = ctx.active_config.read().await.clone();
                let socket_options = socket::SocketOptions::from(&config_snapshot.socket);
                socket::apply_socket_options(&client, &socket_options, "client")
                    .context("apply client socket options")?;
                let client = ClientConnection::new(client);
                metrics::increment_client_connections();

                let Some(client_guard) = ctx.lifecycle.drain_token().try_enter() else {
                    let mut client = client;
                    reject_client_during_drain(&mut client, ctx.reject_phase_recorder.as_ref())
                        .await?;
                    tracing::info!(shard_id = ctx.shard_id, %client_addr, "rejected client during drain");
                    continue;
                };

                let permit = ctx.client_slots.clone().acquire_owned().await?;
                let route_pool_selector = ctx.route_pool_selector.clone();
                let snapshot_store = ctx.snapshot_store.clone();
                let client_snapshot_handle = snapshot_store.client_handle();
                let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
                client_snapshot_handle.register(session_id);
                telemetry::emit_debug_sample_with(
                    &ctx.debug_sampler,
                    session_id,
                    || DebugSample::client_accepted(
                        session_id,
                        client_addr,
                        config_snapshot.tls.client_tls_mode.as_str(),
                        client.has_peer_certificates(),
                    ),
                );
                let phase_recorder = telemetry::sampled_phase_timing_recorder(
                    ctx.phase_metrics_enabled,
                    ctx.phase_timing_sample_rate,
                    session_id,
                );

                let mirror_dispatcher = Arc::clone(&ctx.mirror_dispatcher);
                let buffer_pool = ctx.buffer_pool.clone();
                let backend_credentials = ctx.backend_credentials.load();
                let routing_planner = ctx.routing_planner;
                let debug_sampler = ctx.debug_sampler;
                let cancel_registry = Arc::clone(&ctx.cancel_registry);
                let pause = Arc::clone(&ctx.pause);
                let auth_query_service = Arc::clone(&ctx.auth_query_service);

                let session_context = ClientSessionContext {
                    route_pool_selector,
                    config: config_snapshot,
                    routing_planner,
                    session_id,
                    snapshot_store,
                    client_snapshot_handle,
                    phase_recorder,
                    debug_sampler,
                    mirror_dispatcher,
                    buffer_pool,
                    backend_credentials,
                    cancel_registry,
                    pause,
                    auth_query_service,
                };

                client_tasks.spawn(async move {
                    let _client_guard = client_guard;
                    let result = handle_client(client, client_addr, session_context).await;
                    drop(permit);

                    if let Err(error) = result {
                        let error_chain = format!("{error:#}");
                        tracing::warn!(%client_addr, error = %error_chain, "client connection closed with error");
                    }
                });
                record_runtime_shard_snapshot(
                    &ctx,
                    RuntimeLifecycleState::Ready,
                    accepted_connections,
                    client_tasks.len(),
                );
            }
        }
    }
}

fn record_runtime_shard_snapshot(
    ctx: &ShardContext,
    lifecycle_state: RuntimeLifecycleState,
    accepted_connections: u64,
    active_clients: usize,
) {
    if !ctx.runtime_shard_observability {
        return;
    }

    ctx.snapshot_store
        .set_runtime_shard_snapshot(RuntimeShardSnapshot::new(
            ctx.shard_id,
            ctx.runtime_shard_core_id,
            lifecycle_state,
            accepted_connections,
            active_clients,
        ));
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
    global_backend_slots: Option<Arc<Semaphore>>,
    global_backend_available: Option<Arc<tokio::sync::Notify>>,
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

    let primary_pool = BackendPool::new_with_socket_lifecycle_and_global_limit_and_notify(
        route_config.primary.address,
        pool_args.0.clone(),
        pool_args.1.clone(),
        pool_args.2,
        pool_args.3,
        pool_args.4,
        pool_args.5,
        pool_args.6.clone(),
        pool_args.7.clone(),
        global_backend_slots.clone(),
        global_backend_available.clone(),
    );
    let primary = BackendPoolRef::primary(primary_pool);
    primary.attach_snapshot_store(snapshot_store.clone());

    let replicas = route_config
        .replicas
        .iter()
        .enumerate()
        .map(|(index, replica)| {
            let pool = BackendPool::new_with_socket_lifecycle_and_global_limit_and_notify(
                replica.address,
                pool_args.0.clone(),
                pool_args.1.clone(),
                pool_args.2,
                pool_args.3,
                pool_args.4,
                pool_args.5,
                pool_args.6.clone(),
                pool_args.7.clone(),
                global_backend_slots.clone(),
                global_backend_available.clone(),
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

fn control_route_config(config: &Config, default_route_config: &RouteConfig) -> RouteConfig {
    config.pools.first().map_or_else(
        || default_route_config.clone(),
        |pool| RouteConfig::from_backend_addr(pool.backend_addr),
    )
}

fn build_route_pool_selector(
    config: &Config,
    default_route_config: &RouteConfig,
    snapshot_store: SnapshotStore,
    global_backend_slots: Arc<Semaphore>,
) -> (RoutePoolSelector, Arc<RoutePools>) {
    if config.pools.is_empty() {
        let default_pools = Arc::new(build_route_pools(
            config,
            default_route_config,
            snapshot_store,
            Some(global_backend_slots),
            None,
        ));
        return (
            RoutePoolSelector::default(Arc::clone(&default_pools)),
            default_pools,
        );
    }

    let registry = Arc::new(RoutePoolRegistry::new());
    let global_backend_available = Arc::new(tokio::sync::Notify::new());
    let mut control_route_pools = None;
    for pool_config in &config.pools {
        let route = RouteKey::new(
            pool_config.database.as_str(),
            pool_config.user.as_str(),
            None,
            None,
            QueryClass::Default,
        );
        let pools = build_route_pools_for_pool(
            config,
            pool_config,
            snapshot_store.clone(),
            Arc::clone(&global_backend_slots),
            Some(Arc::clone(&global_backend_available)),
        );
        if control_route_pools.is_none() {
            control_route_pools = Some(Arc::new(pools.clone()));
        }
        registry.insert(route, pools);
    }

    (
        RoutePoolSelector::configured(registry),
        control_route_pools.expect("non-empty pools has a control pool"),
    )
}

fn build_route_pools_for_pool(
    config: &Config,
    pool_config: &PoolConfig,
    snapshot_store: SnapshotStore,
    global_backend_slots: Arc<Semaphore>,
    global_backend_available: Option<Arc<tokio::sync::Notify>>,
) -> RoutePools {
    let mut scoped_config = config.clone();
    if let Some(max_backends) = pool_config.max_backends {
        scoped_config.pool_lifecycle.max_size =
            scoped_config.pool_lifecycle.max_size.min(max_backends);
    }
    let route_config = RouteConfig::from_backend_addr(pool_config.backend_addr);
    build_route_pools(
        &scoped_config,
        &route_config,
        snapshot_store,
        Some(global_backend_slots),
        global_backend_available,
    )
}

#[cfg(test)]
mod tests {
    use std::io::IoSlice;

    use super::{auth_request_expects_client_response, connection::skip_empty_vectored_slices};

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

    #[test]
    fn vectored_write_progress_skips_empty_payload_slices() {
        let header = [b'1', 0, 0, 0, 4];
        let payload = [];
        let next_header = [b'Z', 0, 0, 0, 5];
        let slices = [
            IoSlice::new(&header),
            IoSlice::new(&payload),
            IoSlice::new(&next_header),
        ];
        let mut slice_index = 1;
        let mut slice_offset = 0;

        skip_empty_vectored_slices(&slices, &mut slice_index, &mut slice_offset);

        assert_eq!(slice_index, 2);
        assert_eq!(slice_offset, 0);
    }
}
