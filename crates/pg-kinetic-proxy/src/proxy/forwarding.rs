use super::*;

pub(super) struct ForwardCycleState<'a> {
    pub(super) session: &'a mut VirtualSession,
    pub(super) prepared: &'a mut PreparedCatalog,
    pub(super) prepared_snapshot_handle: PreparedSnapshotHandle,
    pub(super) route_application_name: &'a mut Option<String>,
    pub(super) progress: &'a mut QueryProgress,
}

pub(super) fn plan_frontend_cycle(
    backend_id: u64,
    state: &mut ForwardCycleState<'_>,
    frames: &[FrontendFrame],
    simple_query_commands: &[SqlCommand],
    buffers: &mut SessionBufferSet,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<crate::io_runtime::PlannedFrontendCycle> {
    let needs_sync = should_sync_for_frames(frames);
    let mut simple_query_commands = simple_query_commands.iter();
    let mut injected_parse_completes = 0_usize;
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
            backend_id,
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
            if prelude.tag == u8::from(FrontendTag::Parse) {
                injected_parse_completes += 1;
            }
            buffers.append_frontend_frame(prelude.tag, &prelude.payload);
        }
        buffers.append_frontend_frame(plan.frame.tag, &plan.frame.payload);
    }

    Ok(crate::io_runtime::PlannedFrontendCycle {
        backend_bytes: BytesMut::from(buffers.backend_write()),
        injected_parse_completes,
        needs_sync,
    })
}

pub(super) async fn forward_message_cycle(
    client: &mut ClientConnection,
    backend: &mut PooledBackend,
    state: &mut ForwardCycleState<'_>,
    frames: &[FrontendFrame],
    simple_query_commands: &[SqlCommand],
    max_backend_buffer_bytes: usize,
    buffers: &mut SessionBufferSet,
    phase_recorder: &dyn telemetry::PhaseTimingRecorder,
) -> anyhow::Result<ForwardOutcome> {
    let execute_timer = PhaseTimer::start(ProtocolPhase::Execute, phase_recorder);
    let planned = plan_frontend_cycle(
        backend.backend_id(),
        state,
        frames,
        simple_query_commands,
        buffers,
        phase_recorder,
    )?;
    let needs_sync = planned.needs_sync;
    let mut injected_parse_completes = planned.injected_parse_completes;

    backend
        .backend_mut()
        .stream_mut()
        .write_all(&planned.backend_bytes)
        .await
        .map_err(|error| {
            backend_failure(
                BackendFailureKind::Write,
                false,
                anyhow::Error::new(error).context("write frontend cycle to backend"),
            )
        })?;
    backend
        .backend_mut()
        .stream_mut()
        .flush()
        .await
        .map_err(|error| {
            backend_failure(
                BackendFailureKind::Write,
                false,
                anyhow::Error::new(error).context("flush frontend cycle to backend"),
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

        let mut backend_read = std::mem::take(buffers.backend_read_mut());
        let mut forwarded_frames = buffers.take_backend_frames();
        let ready = classify_backend_frames(
            backend.backend_id(),
            state,
            &mut backend_read,
            &mut injected_parse_completes,
            &mut forwarded_frames,
        )?;
        *buffers.backend_read_mut() = backend_read;

        if !forwarded_frames.is_empty() {
            let mut client_write = Vec::with_capacity(forwarded_frames.len() * 2);
            for (header, payload) in &forwarded_frames {
                client_write.push(IoSlice::new(header));
                client_write.push(IoSlice::new(payload.as_ref()));
            }

            if client.write_all_vectored(&client_write).await.is_err() {
                buffers.restore_backend_frames(forwarded_frames);
                buffers.trim_empty_buffers();
                if let Some(status) = ready {
                    rows_timer.finish(MetricOutcome::Canceled);
                    return Ok(ForwardOutcome::ClientDisconnectedAfterReady(status));
                }

                rows_timer.finish(MetricOutcome::Canceled);
                return Ok(ForwardOutcome::AbandonedResponse { needs_sync });
            }
        }
        buffers.restore_backend_frames(forwarded_frames);

        if let Some(status) = ready {
            buffers.trim_empty_buffers();
            rows_timer.finish(MetricOutcome::Ok);
            return Ok(ForwardOutcome::Ready(status));
        }
    }
}

pub(super) fn classify_backend_frames(
    backend_id: u64,
    state: &mut ForwardCycleState<'_>,
    backend_buffer: &mut BytesMut,
    injected_parse_completes: &mut usize,
    forwarded_frames: &mut Vec<([u8; 5], Bytes)>,
) -> anyhow::Result<Option<ReadyStatus>> {
    let mut ready = None;
    while let Some(frame) = parse_backend_frame(backend_buffer)? {
        if *injected_parse_completes > 0 && frame.tag == b'1' {
            *injected_parse_completes -= 1;
            continue;
        }
        state.progress.response_started = true;
        if let Some(sqlstate) = frame.sqlstate() {
            metrics::increment_sqlstate(sqlstate);
            let scope = state.prepared.invalidate_for_sqlstate(sqlstate, backend_id);
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

        if let Some(status) = frame.ready_status() {
            ready = Some(status);
        }
        let mut header = [0_u8; 5];
        header[0] = frame.tag;
        header[1..].copy_from_slice(&((frame.payload.len() + 4) as i32).to_be_bytes());
        forwarded_frames.push((header, frame.payload));
    }
    Ok(ready)
}

pub(super) fn prepare_frame_for_backend(
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

pub(super) fn publish_prepared_snapshot(
    prepared: &PreparedCatalog,
    prepared_snapshot_handle: &PreparedSnapshotHandle,
) {
    prepared_snapshot_handle.set_statements(prepared.snapshot());
}

#[derive(Debug)]
pub(super) struct PreparedForwardPlan {
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
pub(super) enum ForwardOutcome {
    Ready(ReadyStatus),
    ClientDisconnectedAfterReady(ReadyStatus),
    AbandonedResponse { needs_sync: bool },
    BufferLimitExceeded,
}

pub(super) fn should_sync_for_frames(frames: &[FrontendFrame]) -> bool {
    frames
        .iter()
        .any(|frame| frame.tag != u8::from(FrontendTag::Query))
}

pub(super) fn simple_query_frame(sql: &str) -> FrontendFrame {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.put_u8(0);
    FrontendFrame {
        tag: u8::from(FrontendTag::Query),
        payload: payload.freeze(),
    }
}

pub(super) fn replay_frames(session: &VirtualSession) -> Vec<FrontendFrame> {
    session
        .replay_sql()
        .into_iter()
        .map(|sql| simple_query_frame(&sql))
        .collect()
}

pub(super) fn sync_frame() -> FrontendFrame {
    FrontendFrame {
        tag: u8::from(FrontendTag::Sync),
        payload: BytesMut::new().freeze(),
    }
}

pub(super) fn update_transaction_state_from_request_plans(
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

pub(super) fn update_transaction_state_from_request_plan(
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

pub(super) fn update_transaction_shard_state_from_sql(session: &mut VirtualSession, sql: &str) {
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

pub(super) fn transaction_shard_id_from_sql(sql: &str) -> Option<ShardId> {
    match extract_shard_hint(sql) {
        ShardHint::Shard(value) | ShardHint::Tenant(value) | ShardHint::Route(value) => {
            ShardId::new(value.as_ref()).ok()
        }
        ShardHint::None | ShardHint::Unknown => None,
    }
}

pub(super) fn update_virtual_session_from_frame(
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
