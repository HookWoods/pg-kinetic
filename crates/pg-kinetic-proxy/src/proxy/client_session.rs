use super::*;

pub(super) struct ClientSessionContext {
    pub(super) route_pool_selector: RoutePoolSelector,
    pub(super) config: Config,
    pub(super) routing_planner: ReadRoutingPlanner,
    pub(super) session_id: u64,
    pub(super) snapshot_store: SnapshotStore,
    pub(super) client_snapshot_handle: ClientSnapshotHandle,
    pub(super) phase_recorder: Arc<dyn telemetry::PhaseTimingRecorder>,
    pub(super) debug_sampler: DebugSampler,
    pub(super) mirror_dispatcher: Arc<MirrorDispatcher>,
    pub(super) buffer_pool: ProxyBufferPool,
    pub(super) backend_credentials: Option<Arc<auth::BackendCredentials>>,
    pub(super) cancel_registry: Arc<cancel::CancelRegistry>,
    pub(super) pause: Arc<PauseController>,
    pub(super) auth_query_service: Arc<AuthQueryService>,
}

pub(super) async fn handle_client(
    mut client: ClientConnection,
    client_addr: SocketAddr,
    context: ClientSessionContext,
) -> anyhow::Result<()> {
    let ClientSessionContext {
        route_pool_selector,
        config,
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
    } = context;

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
    let startup = match complete_client_startup(SessionStartupRequest {
        client: &mut client,
        client_addr,
        route_pool_selector: &route_pool_selector,
        config: &config,
        session_id,
        snapshot_store: &snapshot_store,
        client_snapshot_handle: &client_snapshot_handle,
        phase_recorder: &phase_recorder,
        debug_sampler,
        backend_credentials: backend_credentials.clone(),
        cancel_registry: &cancel_registry,
        pause: pause.as_ref(),
        auth_query_service: auth_query_service.as_ref(),
        session_buffers,
        session_started,
    })
    .await?
    {
        SessionStartupOutcome::Ready(startup) => startup,
        SessionStartupOutcome::Finished => return Ok(()),
    };
    let SessionStartupState {
        _cancel_session,
        client_key,
        performance,
        qos,
        route_read_routing_mode,
        route_fallback_policy,
        read_after_write_timeout,
        read_after_write_protection_enabled,
        prepared_snapshot_handle,
        recovery_snapshot_handle,
        mut route_application_name,
        mut session_route,
        route_pools,
        backend_startup_packet,
    } = startup;

    let mut session = VirtualSession::default();
    let mut pinned_backend = PinnedBackend::default();
    let mut prepared = PreparedCatalog::new(session_id);
    let mut sql_plan_cache = SqlPlanCache::new(SQL_PLAN_CACHE_CAPACITY);
    let mut held_backend: Option<PooledBackend> = None;
    let mut wait_for_client_activity_after_timeout = false;
    let mut mirror_query_id = 0_u64;

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
                match handle_frame_cycle(FrameCycleRequest {
                    frames,
                    client: &mut client,
                    client_addr,
                    routing_planner: &routing_planner,
                    session_id,
                    snapshot_store: &snapshot_store,
                    phase_recorder: phase_recorder.as_ref(),
                    debug_sampler,
                    mirror_dispatcher: &mirror_dispatcher,
                    backend_credentials: backend_credentials.as_deref(),
                    cancel_registry: cancel_registry.as_ref(),
                    pause: pause.as_ref(),
                    session_buffers,
                    client_key,
                    performance: &performance,
                    qos: &qos,
                    route_read_routing_mode,
                    route_fallback_policy,
                    read_after_write_timeout,
                    read_after_write_protection_enabled,
                    prepared_snapshot_handle: &prepared_snapshot_handle,
                    recovery_snapshot_handle: &recovery_snapshot_handle,
                    route_application_name: &mut route_application_name,
                    session_route: &mut session_route,
                    route_pools: &route_pools,
                    backend_startup_packet: &backend_startup_packet,
                    session: &mut session,
                    pinned_backend: &mut pinned_backend,
                    prepared: &mut prepared,
                    sql_plan_cache: &mut sql_plan_cache,
                    held_backend: &mut held_backend,
                    wait_for_client_activity_after_timeout:
                        &mut wait_for_client_activity_after_timeout,
                    mirror_query_id: &mut mirror_query_id,
                    session_started,
                })
                .await?
                {
                    FrameCycleOutcome::Continue => {}
                    FrameCycleOutcome::Finish => return Ok(()),
                }
            }
            ClientCycle::Terminate => {
                finalize_held_backend_on_disconnect(FinalizeHeldBackendRequest {
                    held_backend: &mut held_backend,
                    route_pools: &route_pools,
                    performance: &performance,
                    session: &mut session,
                    pinned_backend: &mut pinned_backend,
                    snapshot_store: &snapshot_store,
                    session_id,
                    session_route: session_route.clone(),
                    recovery_snapshot_handle: &recovery_snapshot_handle,
                    qos: &qos,
                    phase_recorder: phase_recorder.as_ref(),
                    debug_sampler,
                    cancel_registry: cancel_registry.as_ref(),
                    client_key,
                })
                .await?;
                return Ok(());
            }
            ClientCycle::IdleTimeout(kind) => {
                finalize_held_backend_on_disconnect(FinalizeHeldBackendRequest {
                    held_backend: &mut held_backend,
                    route_pools: &route_pools,
                    performance: &performance,
                    session: &mut session,
                    pinned_backend: &mut pinned_backend,
                    snapshot_store: &snapshot_store,
                    session_id,
                    session_route: session_route.clone(),
                    recovery_snapshot_handle: &recovery_snapshot_handle,
                    qos: &qos,
                    phase_recorder: phase_recorder.as_ref(),
                    debug_sampler,
                    cancel_registry: cancel_registry.as_ref(),
                    client_key,
                })
                .await?;

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
                finalize_held_backend_on_disconnect(FinalizeHeldBackendRequest {
                    held_backend: &mut held_backend,
                    route_pools: &route_pools,
                    performance: &performance,
                    session: &mut session,
                    pinned_backend: &mut pinned_backend,
                    snapshot_store: &snapshot_store,
                    session_id,
                    session_route: session_route.clone(),
                    recovery_snapshot_handle: &recovery_snapshot_handle,
                    qos: &qos,
                    phase_recorder: phase_recorder.as_ref(),
                    debug_sampler,
                    cancel_registry: cancel_registry.as_ref(),
                    client_key,
                })
                .await?;

                return Ok(());
            }
        }
    }
}

struct FrameCycleRequest<'a> {
    frames: Vec<FrontendFrame>,
    client: &'a mut ClientConnection,
    client_addr: SocketAddr,
    routing_planner: &'a ReadRoutingPlanner,
    session_id: u64,
    snapshot_store: &'a SnapshotStore,
    phase_recorder: &'a dyn telemetry::PhaseTimingRecorder,
    debug_sampler: DebugSampler,
    mirror_dispatcher: &'a MirrorDispatcher,
    backend_credentials: Option<&'a auth::BackendCredentials>,
    cancel_registry: &'a cancel::CancelRegistry,
    pause: &'a PauseController,
    session_buffers: &'a mut SessionBufferSet,
    client_key: (i32, i32),
    performance: &'a crate::config::PerformanceConfig,
    qos: &'a crate::config::QosConfig,
    route_read_routing_mode: ReadRoutingMode,
    route_fallback_policy: FallbackPolicy,
    read_after_write_timeout: Duration,
    read_after_write_protection_enabled: bool,
    prepared_snapshot_handle: &'a PreparedSnapshotHandle,
    recovery_snapshot_handle: &'a RecoverySnapshotHandle,
    route_application_name: &'a mut Option<String>,
    session_route: &'a mut RouteKey,
    route_pools: &'a Arc<RoutePools>,
    backend_startup_packet: &'a BytesMut,
    session: &'a mut VirtualSession,
    pinned_backend: &'a mut PinnedBackend,
    prepared: &'a mut PreparedCatalog,
    sql_plan_cache: &'a mut SqlPlanCache,
    held_backend: &'a mut Option<PooledBackend>,
    wait_for_client_activity_after_timeout: &'a mut bool,
    mirror_query_id: &'a mut u64,
    session_started: Instant,
}

enum FrameCycleOutcome {
    Continue,
    Finish,
}

async fn handle_frame_cycle(request: FrameCycleRequest<'_>) -> anyhow::Result<FrameCycleOutcome> {
    let FrameCycleRequest {
        frames,
        client,
        client_addr,
        routing_planner,
        session_id,
        snapshot_store,
        phase_recorder,
        debug_sampler,
        mirror_dispatcher,
        backend_credentials,
        cancel_registry,
        pause,
        session_buffers,
        client_key,
        performance,
        qos,
        route_read_routing_mode,
        route_fallback_policy,
        read_after_write_timeout,
        read_after_write_protection_enabled,
        prepared_snapshot_handle,
        recovery_snapshot_handle,
        route_application_name,
        session_route,
        route_pools,
        backend_startup_packet,
        session,
        pinned_backend,
        prepared,
        sql_plan_cache,
        held_backend,
        wait_for_client_activity_after_timeout,
        mirror_query_id,
        session_started,
    } = request;
    let current_query_id = *mirror_query_id;
    *mirror_query_id = mirror_query_id.wrapping_add(1);
    let full_routing_analysis = route_read_routing_mode != ReadRoutingMode::Off;
    let request_plans =
        request_plans_for_frames(&prepared, &frames, full_routing_analysis, sql_plan_cache)
            .context("build request plan before backend checkout")?;
    let committed_write_transaction =
        update_transaction_state_from_request_plans(session, &request_plans, full_routing_analysis)
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
        pause.wait_if_paused().await;
        match checkout_backend(CheckoutBackendRequest {
            route_pools: &route_pools,
            route: route.clone(),
            target: checkout_target,
            context: "checkout backend for cycle",
            mode: CheckoutMode::AllowConnect,
            session_id,
            debug_sampler,
            phase_recorder,
            snapshot_store,
            startup_packet: backend_startup_packet,
            backend_credentials,
            read_after_write_state: session.read_after_write_state(),
            record_snapshot: true,
            bootstrap_backend: true,
        })
        .await
        {
            Ok(backend) => backend,
            Err(CheckoutFailure::Overload(message)) => {
                error_response_and_ready(client, qos, message).await?;
                return Ok(FrameCycleOutcome::Finish);
            }
            Err(CheckoutFailure::Postgres { sqlstate, message }) => {
                error_response_and_ready_with_state(client, sqlstate, message, ReadyStatus::Idle)
                    .await?;
                return Ok(FrameCycleOutcome::Finish);
            }
            Err(CheckoutFailure::Close) => {
                return Ok(FrameCycleOutcome::Finish);
            }
            Err(CheckoutFailure::Fatal(error)) => return Err(error),
        }
    };
    bind_cancel_target(cancel_registry, client_key, &backend);

    let replay = if should_replay_session(&session, &pinned_backend, backend.backend_id()) {
        Some(replay_frames(&session))
    } else {
        None
    };

    if let Some(replay_frames) = replay.as_ref() {
        let status =
            execute_backend_batch(&mut backend, replay_frames, qos.max_backend_buffer_bytes)
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
    let mut retry_attempted = false;
    let (result, progress) = loop {
        let mut progress = QueryProgress::default();
        let mut state = ForwardCycleState {
            session,
            prepared,
            prepared_snapshot_handle: prepared_snapshot_handle.clone(),
            route_application_name,
            progress: &mut progress,
        };
        let result = timeout(
            qos.query_timeout(),
            forward_message_cycle(
                client,
                &mut backend,
                &mut state,
                &frames,
                &simple_query_commands,
                qos.max_backend_buffer_bytes,
                session_buffers,
                phase_recorder,
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
                            safe_request_to_replay(&frames, &request_plans, &session),
                        ) == RetryDisposition::RetryBeforeResponse
                })
                .unwrap_or(false),
            _ => false,
        };
        if !retry {
            break (result, progress);
        }

        backend.mark_failed();
        discard_backend_with_cancel_unbind(cancel_registry, client_key, backend).await;
        pause.wait_if_paused().await;
        let Ok(replacement) = checkout_backend(CheckoutBackendRequest {
            route_pools: &route_pools,
            route: route.clone(),
            target: retry_target.clone(),
            context: "checkout backend for failure retry",
            mode: CheckoutMode::AllowConnect,
            session_id,
            debug_sampler,
            phase_recorder,
            snapshot_store,
            startup_packet: backend_startup_packet,
            backend_credentials,
            read_after_write_state: session.read_after_write_state(),
            record_snapshot: false,
            bootstrap_backend: true,
        })
        .await
        else {
            return Ok(FrameCycleOutcome::Finish);
        };
        backend = replacement;
        bind_cancel_target(cancel_registry, client_key, &backend);
        retry_attempted = true;
    };

    let client_disconnected_after_ready = matches!(
        &result,
        Ok(Ok(ForwardOutcome::ClientDisconnectedAfterReady(_)))
    );
    let outcome = handle_forward_result(ForwardResultRequest {
        backend,
        result,
        progress,
        client_disconnected_after_ready,
        client,
        client_addr,
        committed_write_transaction,
        read_after_write_protection_enabled,
        read_after_write_timeout,
        qos,
        route_application_name,
        session_route,
        session,
        prepared,
        performance,
        debug_sampler,
        session_id,
        route_pools,
        snapshot_store,
        pinned_backend,
        held_backend,
        cancel_registry,
        client_key,
        phase_recorder,
        session_started,
        recovery_snapshot_handle,
        wait_for_client_activity_after_timeout,
    })
    .await?;
    drop(request_plans);
    Ok(outcome)
}

struct ForwardResultRequest<'a> {
    backend: PooledBackend,
    result: Result<anyhow::Result<ForwardOutcome>, tokio::time::error::Elapsed>,
    progress: QueryProgress,
    client_disconnected_after_ready: bool,
    client: &'a mut ClientConnection,
    client_addr: SocketAddr,
    committed_write_transaction: bool,
    read_after_write_protection_enabled: bool,
    read_after_write_timeout: Duration,
    qos: &'a crate::config::QosConfig,
    route_application_name: &'a Option<String>,
    session_route: &'a mut RouteKey,
    session: &'a mut VirtualSession,
    prepared: &'a PreparedCatalog,
    performance: &'a crate::config::PerformanceConfig,
    debug_sampler: DebugSampler,
    session_id: u64,
    route_pools: &'a Arc<RoutePools>,
    snapshot_store: &'a SnapshotStore,
    pinned_backend: &'a mut PinnedBackend,
    held_backend: &'a mut Option<PooledBackend>,
    cancel_registry: &'a cancel::CancelRegistry,
    client_key: (i32, i32),
    phase_recorder: &'a dyn telemetry::PhaseTimingRecorder,
    session_started: Instant,
    recovery_snapshot_handle: &'a RecoverySnapshotHandle,
    wait_for_client_activity_after_timeout: &'a mut bool,
}

async fn handle_forward_result(
    request: ForwardResultRequest<'_>,
) -> anyhow::Result<FrameCycleOutcome> {
    let ForwardResultRequest {
        mut backend,
        result,
        progress,
        client_disconnected_after_ready,
        client,
        client_addr,
        committed_write_transaction,
        read_after_write_protection_enabled,
        read_after_write_timeout,
        qos,
        route_application_name,
        session_route,
        session,
        prepared,
        performance,
        debug_sampler,
        session_id,
        route_pools,
        snapshot_store,
        pinned_backend,
        held_backend,
        cancel_registry,
        client_key,
        phase_recorder,
        session_started,
        recovery_snapshot_handle,
        wait_for_client_activity_after_timeout,
    } = request;
    match result {
        Ok(Ok(ForwardOutcome::Ready(status)))
        | Ok(Ok(ForwardOutcome::ClientDisconnectedAfterReady(status))) => {
            finish_ready_backend(ReadyBackendRequest {
                backend,
                status,
                committed_write_transaction,
                read_after_write_protection_enabled,
                read_after_write_timeout,
                qos,
                route_application_name,
                session_route,
                session,
                prepared,
                performance,
                debug_sampler,
                session_id,
                route_pools,
                snapshot_store,
                pinned_backend,
                held_backend,
                cancel_registry,
                client_key,
                phase_recorder,
                session_started,
            })
            .await?;

            if client_disconnected_after_ready {
                finalize_held_backend_on_disconnect(FinalizeHeldBackendRequest {
                    held_backend,
                    route_pools,
                    performance,
                    session,
                    pinned_backend,
                    snapshot_store,
                    session_id,
                    session_route: session_route.clone(),
                    recovery_snapshot_handle,
                    qos,
                    phase_recorder,
                    debug_sampler,
                    cancel_registry,
                    client_key,
                })
                .await?;
                return Ok(FrameCycleOutcome::Finish);
            }
        }
        Ok(Ok(ForwardOutcome::AbandonedResponse { needs_sync })) => {
            let reused = recover_backend(
                &mut backend,
                session_route.clone(),
                session_id,
                debug_sampler,
                RecoveryTrigger::AbandonedResponse,
                performance,
                needs_sync,
                session,
                qos.max_backend_buffer_bytes,
                recovery_snapshot_handle,
            )
            .await
            .context("recover abandoned response")?;
            clear_pinned_backend(pinned_backend, snapshot_store, session_id);
            if reused {
                release_backend_with_cancel_unbind(cancel_registry, client_key, backend).await;
            } else {
                discard_backend_with_cancel_unbind(cancel_registry, client_key, backend).await;
            }
            return Ok(FrameCycleOutcome::Finish);
        }
        Ok(Ok(ForwardOutcome::BufferLimitExceeded)) => {
            clear_pinned_backend(pinned_backend, snapshot_store, session_id);
            discard_backend_with_cancel_unbind(cancel_registry, client_key, backend).await;
            return Ok(FrameCycleOutcome::Finish);
        }
        Ok(Err(error)) => {
            if let Some(kind) = buffer_limit_kind(&error) {
                record_buffer_limit(kind);
                clear_pinned_backend(pinned_backend, snapshot_store, session_id);
                discard_backend_with_cancel_unbind(cancel_registry, client_key, backend).await;
                return Ok(FrameCycleOutcome::Finish);
            }

            clear_pinned_backend(pinned_backend, snapshot_store, session_id);
            if let Some(failure) = error.downcast_ref::<BackendFailure>() {
                backend.mark_failed();
                if !failure.response_started {
                    error_response_and_ready_with_state(
                        client,
                        CONNECTION_FAILURE_SQLSTATE,
                        "backend connection failed before response",
                        ReadyStatus::Idle,
                    )
                    .await?;
                }
            }
            discard_backend_with_cancel_unbind(cancel_registry, client_key, backend).await;
            if error.downcast_ref::<BackendFailure>().is_some() {
                return Ok(FrameCycleOutcome::Finish);
            }
            return Err(error).with_context(|| format!("proxy client {client_addr}"));
        }
        Err(_) => {
            cancel_registry.unbind(client_key).await;
            let continue_client = handle_query_timeout(
                client,
                performance,
                backend,
                held_backend,
                cancel_registry,
                client_key,
                session,
                pinned_backend,
                snapshot_store,
                session_id,
                session_route.clone(),
                recovery_snapshot_handle,
                progress,
                qos.max_backend_buffer_bytes,
                phase_recorder,
                debug_sampler,
            )
            .await?;

            if !continue_client {
                return Ok(FrameCycleOutcome::Finish);
            }

            *wait_for_client_activity_after_timeout = true;
        }
    }

    Ok(FrameCycleOutcome::Continue)
}

struct ReadyBackendRequest<'a> {
    backend: PooledBackend,
    status: ReadyStatus,
    committed_write_transaction: bool,
    read_after_write_protection_enabled: bool,
    read_after_write_timeout: Duration,
    qos: &'a crate::config::QosConfig,
    route_application_name: &'a Option<String>,
    session_route: &'a mut RouteKey,
    session: &'a mut VirtualSession,
    prepared: &'a PreparedCatalog,
    performance: &'a crate::config::PerformanceConfig,
    debug_sampler: DebugSampler,
    session_id: u64,
    route_pools: &'a Arc<RoutePools>,
    snapshot_store: &'a SnapshotStore,
    pinned_backend: &'a mut PinnedBackend,
    held_backend: &'a mut Option<PooledBackend>,
    cancel_registry: &'a cancel::CancelRegistry,
    client_key: (i32, i32),
    phase_recorder: &'a dyn telemetry::PhaseTimingRecorder,
    session_started: Instant,
}

async fn finish_ready_backend(request: ReadyBackendRequest<'_>) -> anyhow::Result<()> {
    let mut backend = request.backend;
    *request.session_route = request
        .session_route
        .with_application_name(request.route_application_name.as_deref());
    if request.committed_write_transaction
        && request.read_after_write_protection_enabled
        && request.status == ReadyStatus::Idle
    {
        let freshness_outcome = probe_read_after_write_requirement(
            &mut backend,
            request.read_after_write_timeout,
            request.qos.max_backend_buffer_bytes,
        )
        .await;
        match freshness_outcome {
            Ok(lsn) => request.session.set_read_after_write_required(lsn),
            Err(_) => request.session.set_read_after_write_unknown(),
        }
    }

    telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
        DebugSample::query_complete(
            request.session_id,
            request.session_route.clone(),
            MetricOutcome::Ok,
            0,
            match request.status {
                ReadyStatus::Idle => "idle",
                ReadyStatus::InTransaction => "in_transaction",
                ReadyStatus::FailedTransaction => "failed_transaction",
            },
            None,
            &[],
        )
    });
    request.session.mark_ready_after_copy();
    let action = if request.status == ReadyStatus::Idle && request.prepared.has_named_statements() {
        CleanupAction::KeepPinned
    } else {
        cleanup_action(
            request.session,
            request.status,
            request.performance.pool_mode.into(),
        )
    };
    metrics::increment_cleanup(action);

    match action {
        CleanupAction::Reuse => {
            clear_pinned_backend(
                request.pinned_backend,
                request.snapshot_store,
                request.session_id,
            );
            release_backend_with_cancel_unbind(
                request.cancel_registry,
                request.client_key,
                backend,
            )
            .await;
        }
        CleanupAction::ResetThenReuse => {
            let reset_timer = PhaseTimer::start(ProtocolPhase::Reset, request.phase_recorder);
            execute_simple_query(
                &mut backend,
                request.route_pools.primary().reset_query(),
                request.qos.max_backend_buffer_bytes,
            )
            .await
            .context("reset backend before reuse")?;
            reset_timer.finish(MetricOutcome::Ok);
            clear_pinned_backend(
                request.pinned_backend,
                request.snapshot_store,
                request.session_id,
            );
            release_backend_with_cancel_unbind(
                request.cancel_registry,
                request.client_key,
                backend,
            )
            .await;
        }
        CleanupAction::KeepPinned => {
            if let Some(reason) = request.session.pin_reason() {
                metrics::increment_pin(reason);
                telemetry::emit_debug_sample_with(
                    &request.debug_sampler,
                    request.session_id,
                    || {
                        DebugSample::pinning(
                            request.session_id,
                            request.session_route.clone(),
                            reason,
                            backend.backend_id(),
                            request.session_started.elapsed(),
                        )
                    },
                );
                record_pinning_snapshot(
                    request.snapshot_store,
                    request.session_id,
                    backend.backend_id(),
                    reason,
                    request.session_route.clone(),
                    request.session_started.elapsed(),
                );
            }
            request.pinned_backend.mark_pinned(backend.backend_id());
            *request.held_backend = Some(backend);
        }
        CleanupAction::RollbackThenReuse => {
            execute_simple_query(
                &mut backend,
                "ROLLBACK",
                request.qos.max_backend_buffer_bytes,
            )
            .await
            .context("rollback failed transaction")?;
            request.session.apply_sql(classify("rollback"));
            clear_pinned_backend(
                request.pinned_backend,
                request.snapshot_store,
                request.session_id,
            );
            release_backend_with_cancel_unbind(
                request.cancel_registry,
                request.client_key,
                backend,
            )
            .await;
        }
        CleanupAction::RollbackThenKeepPinned => {
            execute_simple_query(
                &mut backend,
                "ROLLBACK",
                request.qos.max_backend_buffer_bytes,
            )
            .await
            .context("rollback failed transaction")?;
            request.session.apply_sql(classify("rollback"));
            request.pinned_backend.mark_pinned(backend.backend_id());
            *request.held_backend = Some(backend);
        }
        CleanupAction::Discard => {
            clear_pinned_backend(
                request.pinned_backend,
                request.snapshot_store,
                request.session_id,
            );
            discard_backend_with_cancel_unbind(
                request.cancel_registry,
                request.client_key,
                backend,
            )
            .await;
        }
    }

    Ok(())
}

pub(super) struct SessionStartupRequest<'a> {
    pub(super) client: &'a mut ClientConnection,
    pub(super) client_addr: SocketAddr,
    pub(super) route_pool_selector: &'a RoutePoolSelector,
    pub(super) config: &'a Config,
    pub(super) session_id: u64,
    pub(super) snapshot_store: &'a SnapshotStore,
    pub(super) client_snapshot_handle: &'a ClientSnapshotHandle,
    pub(super) phase_recorder: &'a Arc<dyn telemetry::PhaseTimingRecorder>,
    pub(super) debug_sampler: DebugSampler,
    pub(super) backend_credentials: Option<Arc<auth::BackendCredentials>>,
    pub(super) cancel_registry: &'a Arc<cancel::CancelRegistry>,
    pub(super) pause: &'a PauseController,
    pub(super) auth_query_service: &'a AuthQueryService,
    pub(super) session_buffers: &'a mut SessionBufferSet,
    pub(super) session_started: Instant,
}

pub(super) enum SessionStartupOutcome {
    Ready(SessionStartupState),
    Finished,
}

pub(super) struct SessionStartupState {
    pub(super) _cancel_session: CancelSessionGuard,
    pub(super) client_key: (i32, i32),
    pub(super) performance: crate::config::PerformanceConfig,
    pub(super) qos: crate::config::QosConfig,
    pub(super) route_read_routing_mode: ReadRoutingMode,
    pub(super) route_fallback_policy: FallbackPolicy,
    pub(super) read_after_write_timeout: Duration,
    pub(super) read_after_write_protection_enabled: bool,
    pub(super) prepared_snapshot_handle: PreparedSnapshotHandle,
    pub(super) recovery_snapshot_handle: RecoverySnapshotHandle,
    pub(super) route_application_name: Option<String>,
    pub(super) session_route: RouteKey,
    pub(super) route_pools: Arc<RoutePools>,
    pub(super) backend_startup_packet: BytesMut,
}

pub(super) async fn complete_client_startup(
    request: SessionStartupRequest<'_>,
) -> anyhow::Result<SessionStartupOutcome> {
    let startup_timer = PhaseTimer::start(ProtocolPhase::Startup, request.phase_recorder.as_ref());
    let client_tls_mode = request.config.tls.client_tls_mode;
    let client_tls_server_config = reload::load_client_tls_server_config(request.config)?;
    let auth_users = reload::load_auth_users(request.config)?;
    let auth = request.config.auth.clone();
    let performance = request.config.performance.clone();
    let qos = request.config.qos.clone();
    let route_config = request
        .config
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
    let prepared_snapshot_handle = request.snapshot_store.prepared_handle();
    let recovery_snapshot_handle = request.snapshot_store.recovery_handle();

    let startup_packet = match read_startup_packet_with_buffer(
        request.client,
        client_tls_mode,
        client_tls_server_config.as_ref(),
        qos.idle_client_timeout(),
        qos.max_client_buffer_bytes,
        request.session_buffers.client_read_mut(),
        request.phase_recorder.as_ref(),
    )
    .await
    .with_context(|| format!("proxy client {}", request.client_addr))
    {
        Ok(StartupRead::Packet(packet)) => packet,
        Ok(StartupRead::ClientClosed) => {
            startup_timer.finish(MetricOutcome::Canceled);
            return Ok(SessionStartupOutcome::Finished);
        }
        Ok(StartupRead::TimedOut) => {
            startup_timer.finish(MetricOutcome::Timeout);
            error_response_and_ready_with_state(
                request.client,
                SqlState::OperatorIntervention.as_str(),
                "startup timed out",
                ReadyStatus::Idle,
            )
            .await?;
            return Ok(SessionStartupOutcome::Finished);
        }
        Ok(StartupRead::BufferLimitExceeded) => {
            startup_timer.finish(MetricOutcome::Discarded);
            record_buffer_limit(BufferBudgetKind::Client);
            return Ok(SessionStartupOutcome::Finished);
        }
        Ok(StartupRead::Cancel {
            process_id,
            secret_key,
        }) => {
            startup_timer.finish(MetricOutcome::Canceled);
            if let Err(error) = request
                .cancel_registry
                .forward_cancel((process_id, secret_key))
                .await
            {
                tracing::debug!(%error, "cancel forwarding failed");
            }
            return Ok(SessionStartupOutcome::Finished);
        }
        Err(error) => {
            startup_timer.finish(MetricOutcome::Error);
            return Err(error);
        }
    };
    request.session_buffers.observe_client_read();
    let client_key = request.cancel_registry.issue_client_key()?;
    let cancel_session = CancelSessionGuard::new(Arc::clone(request.cancel_registry), client_key);

    let (route_database, route_user, route_application_name) = startup_route_key(&startup_packet)?;
    let session_route = route_key(
        &route_database,
        &route_user,
        route_application_name.as_deref(),
        request.client_addr,
    );
    let Some(route_pools) = request.route_pool_selector.resolve(&session_route) else {
        startup_timer.finish(MetricOutcome::Rejected);
        let message = format!(
            "database \"{route_database}\" for user \"{route_user}\" is not configured on this proxy"
        );
        error_response_and_ready_with_state(
            request.client,
            INVALID_CATALOG_NAME_SQLSTATE,
            &message,
            ReadyStatus::Idle,
        )
        .await?;
        return Ok(SessionStartupOutcome::Finished);
    };
    update_client_snapshot(
        request.client_snapshot_handle,
        request.session_id,
        route_database.clone(),
        route_user.clone(),
        route_application_name.clone(),
        session_route.clone(),
        "startup",
        request.session_started.elapsed(),
    );

    if !matches!(auth.auth_mode, crate::config::AuthMode::PassThrough) {
        let auth_timer = PhaseTimer::start(ProtocolPhase::Auth, request.phase_recorder.as_ref());
        let auth_users = match auth_users.as_deref().context("auth user store unavailable") {
            Ok(users) => users,
            Err(error) => {
                auth_timer.finish(MetricOutcome::Error);
                startup_timer.finish(MetricOutcome::Error);
                return Err(error);
            }
        };
        match auth::authenticate_client(
            request.client,
            &route_user,
            &auth,
            auth_users,
            Some(request.auth_query_service),
            qos.max_client_buffer_bytes,
            qos.max_backend_buffer_bytes,
        )
        .await
        .with_context(|| format!("authenticate client {}", request.client_addr))
        {
            Ok(auth::ClientAuthOutcome::PassThrough)
            | Ok(auth::ClientAuthOutcome::Authenticated) => {
                auth_timer.finish(MetricOutcome::Ok);
            }
            Ok(auth::ClientAuthOutcome::Rejected) => {
                auth_timer.finish(MetricOutcome::Rejected);
                startup_timer.finish(MetricOutcome::Rejected);
                return Ok(SessionStartupOutcome::Finished);
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
        request
            .backend_credentials
            .as_deref()
            .map(auth::BackendCredentials::username),
    )?;

    request.pause.wait_if_paused().await;
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
        session_id: request.session_id,
        debug_sampler: request.debug_sampler,
        phase_recorder: request.phase_recorder.as_ref(),
        snapshot_store: request.snapshot_store,
        startup_packet: &backend_startup_packet,
        backend_credentials: request.backend_credentials.as_deref(),
        read_after_write_state: ReadAfterWriteState::Disabled,
        record_snapshot: false,
        bootstrap_backend: false,
    })
    .await
    {
        Ok(backend) => backend,
        Err(CheckoutFailure::Overload(message)) => {
            startup_timer.finish(MetricOutcome::Rejected);
            error_response_and_ready(request.client, &qos, message).await?;
            return Ok(SessionStartupOutcome::Finished);
        }
        Err(CheckoutFailure::Postgres { sqlstate, message }) => {
            startup_timer.finish(MetricOutcome::Rejected);
            error_response_and_ready_with_state(
                request.client,
                sqlstate,
                message,
                ReadyStatus::Idle,
            )
            .await?;
            return Ok(SessionStartupOutcome::Finished);
        }
        Err(CheckoutFailure::Close) => {
            startup_timer.finish(MetricOutcome::Canceled);
            return Ok(SessionStartupOutcome::Finished);
        }
        Err(CheckoutFailure::Fatal(error)) => {
            startup_timer.finish(MetricOutcome::Error);
            return Err(error);
        }
    };
    if let Err(error) = proxy_startup(
        request.client,
        &mut backend,
        &backend_startup_packet,
        qos.max_client_buffer_bytes,
        qos.max_backend_buffer_bytes,
        matches!(auth.auth_mode, crate::config::AuthMode::PassThrough),
        matches!(auth.auth_mode, crate::config::AuthMode::PassThrough),
        request.backend_credentials.as_deref(),
        request.session_buffers,
        request.phase_recorder.as_ref(),
        client_key,
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
            return Ok(SessionStartupOutcome::Finished);
        }

        backend.discard();
        return Err(error).with_context(|| format!("proxy client {}", request.client_addr));
    }
    backend.release().await;
    schedule_service_backend_pool_warmup(
        Arc::clone(&route_pools),
        session_route.clone(),
        backend_startup_packet.to_vec(),
        request.backend_credentials.clone(),
        request.snapshot_store.clone(),
        Arc::clone(request.phase_recorder),
        request.debug_sampler,
        request.session_id,
    );
    startup_timer.finish(MetricOutcome::Ok);
    telemetry::emit_debug_sample_with(&request.debug_sampler, request.session_id, || {
        DebugSample::startup_complete(
            request.session_id,
            session_route.clone(),
            auth.auth_mode.as_str(),
            client_tls_mode.as_str(),
            MetricOutcome::Ok,
        )
    });
    update_client_snapshot(
        request.client_snapshot_handle,
        request.session_id,
        route_database.clone(),
        route_user.clone(),
        route_application_name.clone(),
        session_route.clone(),
        "active",
        request.session_started.elapsed(),
    );

    Ok(SessionStartupOutcome::Ready(SessionStartupState {
        _cancel_session: cancel_session,
        client_key,
        performance,
        qos,
        route_read_routing_mode,
        route_fallback_policy,
        read_after_write_timeout,
        read_after_write_protection_enabled,
        prepared_snapshot_handle,
        recovery_snapshot_handle,
        route_application_name,
        session_route,
        route_pools,
        backend_startup_packet,
    }))
}

pub(super) struct FinalizeHeldBackendRequest<'a> {
    pub(super) held_backend: &'a mut Option<PooledBackend>,
    pub(super) route_pools: &'a Arc<RoutePools>,
    pub(super) performance: &'a crate::config::PerformanceConfig,
    pub(super) session: &'a mut VirtualSession,
    pub(super) pinned_backend: &'a mut PinnedBackend,
    pub(super) snapshot_store: &'a SnapshotStore,
    pub(super) session_id: u64,
    pub(super) session_route: RouteKey,
    pub(super) recovery_snapshot_handle: &'a RecoverySnapshotHandle,
    pub(super) qos: &'a crate::config::QosConfig,
    pub(super) phase_recorder: &'a dyn telemetry::PhaseTimingRecorder,
    pub(super) debug_sampler: DebugSampler,
    pub(super) cancel_registry: &'a cancel::CancelRegistry,
    pub(super) client_key: (i32, i32),
}

pub(super) async fn finalize_held_backend_on_disconnect(
    request: FinalizeHeldBackendRequest<'_>,
) -> anyhow::Result<()> {
    let Some(backend) = request.held_backend.take() else {
        return Ok(());
    };

    request.cancel_registry.unbind(request.client_key).await;
    finalize_backend_on_disconnect(
        backend,
        request.route_pools,
        request.performance,
        request.session,
        request.pinned_backend,
        request.snapshot_store,
        request.session_id,
        request.session_route,
        request.recovery_snapshot_handle,
        request.qos,
        request.phase_recorder,
        request.debug_sampler,
    )
    .await
}
