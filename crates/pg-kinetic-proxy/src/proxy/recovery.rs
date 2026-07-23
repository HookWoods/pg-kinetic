use super::*;

pub(super) async fn execute_backend_batch(
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

pub(super) async fn execute_simple_query(
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

pub(super) async fn probe_read_after_write_requirement(
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

pub(super) fn parse_read_after_write_lsn(payload: &[u8]) -> anyhow::Result<Option<PgLsn>> {
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

pub(super) async fn await_ready_status(
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
pub(super) async fn recover_backend(
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

pub(super) async fn error_response_and_ready(
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

pub(super) async fn error_response_and_ready_with_state(
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

pub(super) async fn error_response_only(
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

pub(super) async fn reject_client_during_drain(
    client: &mut ClientConnection,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<()> {
    let drain_timer = PhaseTimer::start(ProtocolPhase::Drain, phase_recorder);
    error_response_and_ready_with_state(
        client,
        SqlState::OperatorIntervention.as_str(),
        "proxy is draining",
        ReadyStatus::Idle,
    )
    .await?;
    drain_timer.finish(MetricOutcome::Rejected);
    client.shutdown().await.context("shutdown draining client")
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_query_timeout(
    client: &mut ClientConnection,
    performance: &crate::config::PerformanceConfig,
    mut backend: PooledBackend,
    held_backend: &mut Option<PooledBackend>,
    cancel_registry: &cancel::CancelRegistry,
    client_key: (i32, i32),
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
        if performance.pool_mode == crate::config::PoolMode::Session {
            bind_cancel_target(cancel_registry, client_key, &backend);
            *held_backend = Some(backend);
        } else {
            backend.release().await;
        }
    } else {
        backend.discard();
    }
    cancel_timer.finish(MetricOutcome::Timeout);

    Ok(!progress.response_started)
}

pub(super) async fn handle_idle_timeout(
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
pub(super) async fn finalize_backend_on_disconnect(
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

pub(super) fn should_replay_session(
    session: &VirtualSession,
    pinned_backend: &PinnedBackend,
    backend_id: u64,
) -> bool {
    session.has_replayable_settings() && pinned_backend.backend_id() != Some(backend_id)
}
