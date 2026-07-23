use super::*;

pub(super) struct CheckoutBackendRequest<'a> {
    pub(super) route_pools: &'a Arc<RoutePools>,
    pub(super) route: RouteKey,
    pub(super) target: RoutingTarget,
    pub(super) context: &'static str,
    pub(super) mode: CheckoutMode,
    pub(super) session_id: u64,
    pub(super) debug_sampler: DebugSampler,
    pub(super) phase_recorder: &'a dyn telemetry::PhaseTimingRecorder,
    pub(super) snapshot_store: &'a SnapshotStore,
    pub(super) startup_packet: &'a [u8],
    pub(super) backend_credentials: Option<&'a auth::BackendCredentials>,
    pub(super) read_after_write_state: ReadAfterWriteState,
    pub(super) record_snapshot: bool,
    pub(super) bootstrap_backend: bool,
}

pub(super) async fn checkout_backend(
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

pub(super) fn schedule_service_backend_pool_warmup(
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
pub(super) enum CheckoutMode {
    AllowConnect,
    PreferConnect,
}

#[derive(Debug)]
pub(super) enum CheckoutFailure {
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

pub(super) fn startup_route_key(
    startup_packet: &[u8],
) -> anyhow::Result<(String, String, Option<String>)> {
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

pub(super) fn rewrite_backend_startup_user(
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

pub(super) fn startup_parameter<'a>(
    parameters: &'a [(String, String)],
    key: &str,
) -> Option<&'a str> {
    parameters
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

pub(super) fn route_key(
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

pub(super) struct ReadRoutingSelection<'a> {
    pub(super) planner: &'a ReadRoutingPlanner,
    pub(super) route_pools: &'a RoutePools,
    pub(super) snapshot_store: &'a SnapshotStore,
    pub(super) read_routing_mode: ReadRoutingMode,
    pub(super) fallback_policy: FallbackPolicy,
    pub(super) session: &'a VirtualSession,
    pub(super) request_plan: Option<&'a RequestPlan<'a>>,
}

pub(super) fn select_checkout_target(selection: &ReadRoutingSelection<'_>) -> RoutingTarget {
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

pub(super) fn fallback_target(
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

pub(super) fn routing_reason_from_core(reason: CoreRoutingReason) -> RoutingReason {
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

pub(super) fn build_route_health_snapshot(
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
