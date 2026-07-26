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
) -> anyhow::Result<crate::engine::io_runtime::PlannedFrontendCycle> {
    let cycle_shape = crate::engine::io_runtime::FrontendCycleShape::from_frames(frames);
    let needs_sync = cycle_shape.needs_sync();
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

    Ok(crate::engine::io_runtime::PlannedFrontendCycle {
        backend_bytes: BytesMut::from(buffers.backend_write()),
        injected_parse_completes,
        needs_sync,
    })
}

enum CopyDuplexEvent {
    Backend(std::io::Result<usize>),
    Client(std::io::Result<usize>),
}

/// Backend messages that hand control of the exchange to the client.
///
/// `CopyInResponse` starts `COPY ... FROM STDIN`; `CopyBothResponse` starts the
/// replication handshake. `CopyOutResponse` is deliberately absent: the backend
/// keeps producing there, so the ordinary read loop already handles it.
fn is_copy_from_client_request(tag: u8) -> bool {
    tag == u8::from(BackendTag::CopyInResponse) || tag == u8::from(BackendTag::CopyBothResponse)
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
    let cycle_shape = crate::engine::io_runtime::FrontendCycleShape::from_frames(frames);
    let expected_ready_count = cycle_shape.expected_ready_count();
    let mut progress = crate::engine::io_runtime::BackendCycleProgress {
        injected_parse_completes: planned.injected_parse_completes,
        ..Default::default()
    };

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
    if cycle_shape.expects_no_response() {
        // Nothing to wait for: the cycle elicits no backend reply at all.
        buffers.trim_empty_buffers();
        rows_timer.finish(MetricOutcome::Ok);
        return Ok(ForwardOutcome::Flushed);
    }
    // The read buffer is deliberately not cleared here. A Flush-delimited cycle
    // can return with a partially received frame still buffered, and those bytes
    // belong to the response this cycle continues reading. Cross-session reuse is
    // safe because the buffer pool clears on recycle.
    // Set once the backend asks the client for data (COPY ... FROM STDIN, or the
    // replication CopyBoth handshake). From that point the client drives the
    // exchange, so reading only the backend would deadlock: the backend waits for
    // rows the proxy never collects.
    let mut client_drives_copy = false;
    let mut copy_from_client = BytesMut::new();

    loop {
        if buffers.backend_read_mut().len() >= max_backend_buffer_bytes {
            record_buffer_limit(BufferBudgetKind::Backend);
            rows_timer.finish(MetricOutcome::Discarded);
            return Ok(ForwardOutcome::BufferLimitExceeded);
        }

        let read = if client_drives_copy {
            copy_from_client.clear();
            let event = tokio::select! {
                // Bias to the backend so an ErrorResponse ending the copy is seen
                // promptly rather than after another round of client rows.
                biased;
                result = backend
                    .backend_mut()
                    .stream_mut()
                    .read_buf(buffers.backend_read_mut()) => CopyDuplexEvent::Backend(result),
                result = client.read_buf(&mut copy_from_client) => CopyDuplexEvent::Client(result),
            };

            match event {
                CopyDuplexEvent::Backend(result) => result.map_err(|error| {
                    backend_failure(
                        BackendFailureKind::Read,
                        state.progress.response_started,
                        anyhow::Error::new(error).context("read backend frame"),
                    )
                })?,
                CopyDuplexEvent::Client(result) => {
                    // CopyData/CopyDone/CopyFail need no rewriting, so the client's
                    // bytes go to the backend verbatim.
                    let client_read = result.context("read client copy data")?;
                    if client_read == 0 {
                        rows_timer.finish(MetricOutcome::Canceled);
                        return Ok(ForwardOutcome::AbandonedResponse { needs_sync });
                    }
                    let stream = backend.backend_mut().stream_mut();
                    stream.write_all(&copy_from_client).await.map_err(|error| {
                        backend_failure(
                            BackendFailureKind::Write,
                            state.progress.response_started,
                            anyhow::Error::new(error).context("write client copy data"),
                        )
                    })?;
                    stream.flush().await.map_err(|error| {
                        backend_failure(
                            BackendFailureKind::Write,
                            state.progress.response_started,
                            anyhow::Error::new(error).context("flush client copy data"),
                        )
                    })?;
                    continue;
                }
            }
        } else {
            backend
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
                })?
        };
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
            cycle_shape,
            &mut progress,
            &mut forwarded_frames,
        )?;
        *buffers.backend_read_mut() = backend_read;
        let has_forwarded_frames = !forwarded_frames.is_empty();
        if !client_drives_copy {
            client_drives_copy = forwarded_frames
                .iter()
                .any(|(header, _)| is_copy_from_client_request(header[0]));
        }

        if has_forwarded_frames {
            let mut client_write = Vec::with_capacity(forwarded_frames.len() * 2);
            for (header, payload) in &forwarded_frames {
                client_write.push(IoSlice::new(header));
                client_write.push(IoSlice::new(payload.as_ref()));
            }

            if client.write_all_vectored(&client_write).await.is_err() {
                buffers.restore_backend_frames(forwarded_frames);
                buffers.trim_empty_buffers();
                if let Some(status) = ready.filter(|_| progress.ready_count >= expected_ready_count)
                {
                    rows_timer.finish(MetricOutcome::Canceled);
                    return Ok(ForwardOutcome::ClientDisconnectedAfterReady(status));
                }

                rows_timer.finish(MetricOutcome::Canceled);
                return Ok(ForwardOutcome::AbandonedResponse { needs_sync });
            }
        }
        buffers.restore_backend_frames(forwarded_frames);

        if let Some(status) = ready.filter(|_| progress.ready_count >= expected_ready_count) {
            buffers.trim_empty_buffers();
            rows_timer.finish(MetricOutcome::Ok);
            return Ok(ForwardOutcome::Ready(status));
        }

        // Only stop once every pending request has been answered. Returning on the
        // first read that produced frames would strand the rest of a split response
        // and deadlock a client waiting on it.
        if !cycle_shape.expects_ready() && flush_cycle_complete(cycle_shape, progress) {
            buffers.trim_empty_buffers();
            rows_timer.finish(MetricOutcome::Ok);
            return Ok(ForwardOutcome::Flushed);
        }
    }
}

fn flush_cycle_complete(
    shape: crate::engine::io_runtime::FrontendCycleShape,
    progress: crate::engine::io_runtime::BackendCycleProgress,
) -> bool {
    progress.saw_error || progress.completion_count >= shape.expected_completion_count()
}

#[cfg(any(test, all(target_os = "linux", feature = "io-uring")))]
/// Returns the narrow two-variant outcome on purpose: the runtime path can only
/// finish on `ReadyForQuery` or a completed `Flush` cycle, and encoding that in
/// the type keeps callers from having to assert it at runtime.
pub(crate) async fn forward_runtime_cycle<C, B>(
    client: &mut C,
    backend: &mut B,
    cycle: &[u8],
    shape: crate::engine::io_runtime::FrontendCycleShape,
    injected_parse_completes: usize,
    backend_buffer: &mut BytesMut,
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<crate::engine::io_runtime::BackendForwardOutcome>
where
    C: crate::engine::io_runtime::RuntimeByteStream + ?Sized,
    B: crate::engine::io_runtime::RuntimeByteStream + ?Sized,
{
    crate::engine::io_runtime::write_all_to(backend, cycle)
        .await
        .map_err(|error| {
            backend_failure(
                BackendFailureKind::Write,
                false,
                anyhow::Error::new(error).context("write frontend cycle to backend"),
            )
        })?;
    let mut response_drain =
        crate::engine::io_runtime::BackendResponseDrain::for_cycle(shape, injected_parse_completes);
    forward_runtime_backend_until_cycle_complete(
        backend,
        client,
        backend_buffer,
        &mut response_drain,
        max_backend_buffer_bytes,
    )
    .await
}

#[cfg(any(test, all(target_os = "linux", feature = "io-uring")))]
async fn forward_runtime_backend_until_cycle_complete<B, C>(
    backend: &mut B,
    client: &mut C,
    backend_buffer: &mut BytesMut,
    drain: &mut crate::engine::io_runtime::BackendResponseDrain,
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<crate::engine::io_runtime::BackendForwardOutcome>
where
    B: crate::engine::io_runtime::RuntimeByteStream + ?Sized,
    C: crate::engine::io_runtime::RuntimeByteStream + ?Sized,
{
    if !drain.expects_ready() && drain.flush_cycle_complete() {
        return Ok(crate::engine::io_runtime::BackendForwardOutcome::Flushed);
    }

    loop {
        let read = crate::engine::io_runtime::read_from(backend, backend_buffer)
            .await
            .map_err(|error| {
                backend_failure(
                    BackendFailureKind::Read,
                    drain.response_started() || !backend_buffer.is_empty(),
                    anyhow::Error::new(error).context("read backend response"),
                )
            })?;
        if read == 0 {
            return Err(backend_failure(
                BackendFailureKind::Read,
                drain.response_started() || !backend_buffer.is_empty(),
                anyhow::anyhow!("backend closed during response"),
            ));
        }

        match crate::engine::io_runtime::drain_backend_response_bytes(
            backend_buffer,
            drain,
            max_backend_buffer_bytes,
        )? {
            crate::engine::io_runtime::BackendBytesDrainEvent::Bytes { bytes, ready } => {
                if !bytes.is_empty() {
                    crate::engine::io_runtime::write_all_to(client, &bytes)
                        .await
                        .context("write backend response")?;
                }
                if let Some(status) = ready {
                    return Ok(crate::engine::io_runtime::BackendForwardOutcome::Ready(
                        status,
                    ));
                }
                if !drain.expects_ready() && drain.flush_cycle_complete() {
                    return Ok(crate::engine::io_runtime::BackendForwardOutcome::Flushed);
                }
            }
            crate::engine::io_runtime::BackendBytesDrainEvent::BufferLimitExceeded => {
                anyhow::bail!("backend response exceeded configured buffer limit");
            }
            crate::engine::io_runtime::BackendBytesDrainEvent::NeedMoreBytes => {}
        }
    }
}

#[cfg(test)]
mod generic_tests {
    use super::*;
    use crate::pool::PoolBackendTransport;
    use bytes::BytesMut;
    use std::collections::VecDeque;

    #[derive(Debug)]
    struct MemoryBackendStream {
        id: u64,
        reads: VecDeque<BytesMut>,
        writes: Vec<BytesMut>,
    }

    impl crate::engine::io_runtime::RuntimeByteStream for MemoryBackendStream {
        async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
            let Some(next) = self.reads.pop_front() else {
                return Ok(0);
            };
            let read = next.len();
            dst.extend_from_slice(&next);
            Ok(read)
        }

        async fn write_all_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            self.writes.push(BytesMut::from(bytes));
            Ok(())
        }

        async fn shutdown_stream(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl crate::pool::PoolBackendTransport for MemoryBackendStream {
        fn id(&self) -> u64 {
            self.id
        }

        fn attach_snapshot_store(&mut self, _snapshot_store: SnapshotStore) {}

        fn mark_checked_out(&self, _route_key: Option<RouteKey>) {}

        fn mark_idle(&self, _route_key: Option<RouteKey>) {}

        fn mark_discarded(&self) {}
    }

    #[tokio::test]
    async fn forwarding_accepts_generic_pooled_backend_stream() {
        let backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        assert_eq!(backend.id(), 1);
    }

    fn cycle_shape(tags: &[FrontendTag]) -> crate::engine::io_runtime::FrontendCycleShape {
        let frames = tags
            .iter()
            .map(|tag| FrontendFrame {
                tag: u8::from(*tag),
                payload: Bytes::new(),
            })
            .collect::<Vec<_>>();
        crate::engine::io_runtime::FrontendCycleShape::from_frames(&frames)
    }

    /// Guards the stop condition the pooled (non-runtime) forwarding loop uses for
    /// Flush-delimited cycles, where there is no ReadyForQuery to wait on.
    #[test]
    fn flush_cycle_completes_only_once_every_pending_reply_arrived() {
        let shape = cycle_shape(&[
            FrontendTag::Parse,
            FrontendTag::Describe,
            FrontendTag::Flush,
        ]);
        assert_eq!(shape.expected_completion_count(), 2);
        assert!(!shape.expects_ready());

        let mut progress = crate::engine::io_runtime::BackendCycleProgress::default();
        assert!(!flush_cycle_complete(shape, progress));

        // ParseComplete only: the Describe reply is still outstanding.
        progress.completion_count = 1;
        assert!(!flush_cycle_complete(shape, progress));

        // RowDescription closes the Describe: the cycle is done.
        progress.completion_count = 2;
        assert!(flush_cycle_complete(shape, progress));

        // An ErrorResponse ends the cycle even with replies outstanding, because
        // the backend discards the rest of it until it sees a Sync.
        let errored = crate::engine::io_runtime::BackendCycleProgress {
            saw_error: true,
            ..Default::default()
        };
        assert!(flush_cycle_complete(shape, errored));
    }

    #[test]
    fn sync_terminated_cycles_do_not_use_the_completion_budget() {
        let shape = cycle_shape(&[
            FrontendTag::Parse,
            FrontendTag::Bind,
            FrontendTag::Execute,
            FrontendTag::Sync,
        ]);
        assert!(shape.expects_ready());
        assert_eq!(shape.expected_ready_count(), 1);
        assert_eq!(shape.expected_completion_count(), 0);
        assert!(!shape.expects_no_response());
    }

    #[test]
    fn mixed_cycles_expect_one_ready_per_simple_query_plus_the_sync() {
        let shape = cycle_shape(&[
            FrontendTag::Query,
            FrontendTag::Parse,
            FrontendTag::Bind,
            FrontendTag::Execute,
            FrontendTag::Sync,
        ]);
        assert_eq!(shape.expected_ready_count(), 2);
    }

    #[tokio::test]
    async fn runtime_forwarding_drains_injected_parse_before_ready() {
        let mut client = MemoryBackendStream {
            id: 2,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        let mut backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::from([
                BytesMut::from(&b"1\x00\x00\x00\x04"[..]),
                BytesMut::from(&b"Z\x00\x00\x00\x05I"[..]),
            ]),
            writes: Vec::new(),
        };

        forward_runtime_cycle(
            &mut client,
            &mut backend,
            b"Q",
            cycle_shape(&[FrontendTag::Query]),
            1,
            &mut BytesMut::new(),
            1024,
        )
        .await
        .expect("forwarding should ignore the injected ParseComplete");

        assert_eq!(backend.writes, vec![BytesMut::from(&b"Q"[..])]);
        assert_eq!(
            client.writes,
            vec![BytesMut::from(&b"Z\x00\x00\x00\x05I"[..])]
        );
    }

    #[tokio::test]
    async fn runtime_forwarding_waits_for_every_flush_completion_across_reads() {
        // Parse + Describe(statement) + Flush expects ParseComplete and then
        // ParameterDescription followed by RowDescription. Splitting them across
        // reads must not end the cycle early: a client blocked on RowDescription
        // before sending Bind would deadlock against a proxy that returned after
        // the first read.
        let mut client = MemoryBackendStream {
            id: 2,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        let mut backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::from([
                BytesMut::from(&b"1\x00\x00\x00\x04"[..]),
                BytesMut::from(&b"t\x00\x00\x00\x06\x00\x00"[..]),
                BytesMut::from(&b"T\x00\x00\x00\x06\x00\x00"[..]),
            ]),
            writes: Vec::new(),
        };

        let outcome = forward_runtime_cycle(
            &mut client,
            &mut backend,
            b"H\x00\x00\x00\x04",
            cycle_shape(&[
                FrontendTag::Parse,
                FrontendTag::Describe,
                FrontendTag::Flush,
            ]),
            0,
            &mut BytesMut::new(),
            1024,
        )
        .await
        .expect("flush forwarding completes once every pending reply arrived");

        assert!(matches!(
            outcome,
            crate::engine::io_runtime::BackendForwardOutcome::Flushed
        ));
        assert_eq!(
            backend.writes,
            vec![BytesMut::from(&b"H\x00\x00\x00\x04"[..])]
        );
        // Every frame reaches the client, including the ones after the first read.
        assert_eq!(
            client.writes,
            vec![
                BytesMut::from(&b"1\x00\x00\x00\x04"[..]),
                BytesMut::from(&b"t\x00\x00\x00\x06\x00\x00"[..]),
                BytesMut::from(&b"T\x00\x00\x00\x06\x00\x00"[..]),
            ]
        );
    }

    #[tokio::test]
    async fn runtime_forwarding_ends_flush_cycle_on_error_response() {
        // After an ErrorResponse the backend discards the rest of the cycle until
        // it sees a Sync, so the outstanding completions never arrive.
        let mut client = MemoryBackendStream {
            id: 2,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        let mut backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::from([BytesMut::from(&b"E\x00\x00\x00\x05\x00"[..])]),
            writes: Vec::new(),
        };

        let outcome = forward_runtime_cycle(
            &mut client,
            &mut backend,
            b"H\x00\x00\x00\x04",
            cycle_shape(&[
                FrontendTag::Parse,
                FrontendTag::Describe,
                FrontendTag::Flush,
            ]),
            0,
            &mut BytesMut::new(),
            1024,
        )
        .await
        .expect("an error ends the flush cycle without the remaining completions");

        assert!(matches!(
            outcome,
            crate::engine::io_runtime::BackendForwardOutcome::Flushed
        ));
    }

    #[tokio::test]
    async fn runtime_forwarding_returns_immediately_when_no_reply_is_expected() {
        // A lone Flush gets no response at all; waiting on the backend would hang.
        let mut client = MemoryBackendStream {
            id: 2,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        let mut backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };

        let outcome = forward_runtime_cycle(
            &mut client,
            &mut backend,
            b"H\x00\x00\x00\x04",
            cycle_shape(&[FrontendTag::Flush]),
            0,
            &mut BytesMut::new(),
            1024,
        )
        .await
        .expect("a cycle with no expected reply returns without reading");

        assert!(matches!(
            outcome,
            crate::engine::io_runtime::BackendForwardOutcome::Flushed
        ));
        assert!(client.writes.is_empty());
    }

    #[tokio::test]
    async fn runtime_forwarding_classifies_backend_close_before_response() {
        let mut client = MemoryBackendStream {
            id: 2,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        let mut backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };

        let error = forward_runtime_cycle(
            &mut client,
            &mut backend,
            b"Q",
            cycle_shape(&[FrontendTag::Query]),
            0,
            &mut BytesMut::new(),
            1024,
        )
        .await
        .expect_err("backend close before response is retry-classified");
        let failure = error
            .downcast_ref::<BackendFailure>()
            .expect("structured backend failure");

        assert_eq!(failure.kind, BackendFailureKind::Read);
        assert!(!failure.response_started);
    }

    #[tokio::test]
    async fn runtime_forwarding_classifies_backend_close_after_response_started() {
        let mut client = MemoryBackendStream {
            id: 2,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        let mut backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::from([BytesMut::from(&b"E\x00\x00\x00\x05\x00"[..])]),
            writes: Vec::new(),
        };

        let error = forward_runtime_cycle(
            &mut client,
            &mut backend,
            b"Q",
            cycle_shape(&[FrontendTag::Query]),
            0,
            &mut BytesMut::new(),
            1024,
        )
        .await
        .expect_err("backend close after response is not retry-safe");
        let failure = error
            .downcast_ref::<BackendFailure>()
            .expect("structured backend failure");

        assert_eq!(failure.kind, BackendFailureKind::Read);
        assert!(failure.response_started);
        assert_eq!(
            client.writes,
            vec![BytesMut::from(&b"E\x00\x00\x00\x05\x00"[..])]
        );
    }

    #[tokio::test]
    async fn runtime_forwarding_treats_partial_frame_as_response_started() {
        let mut client = MemoryBackendStream {
            id: 2,
            reads: VecDeque::new(),
            writes: Vec::new(),
        };
        let mut backend = MemoryBackendStream {
            id: 1,
            reads: VecDeque::from([BytesMut::from(&b"E\x00\x00"[..])]),
            writes: Vec::new(),
        };

        let error = forward_runtime_cycle(
            &mut client,
            &mut backend,
            b"Q",
            cycle_shape(&[FrontendTag::Query]),
            0,
            &mut BytesMut::new(),
            1024,
        )
        .await
        .expect_err("partial backend frame is not retry-safe");
        let failure = error
            .downcast_ref::<BackendFailure>()
            .expect("structured backend failure");

        assert_eq!(failure.kind, BackendFailureKind::Read);
        assert!(failure.response_started);
        assert!(client.writes.is_empty());
    }
}

pub(super) fn classify_backend_frames(
    backend_id: u64,
    state: &mut ForwardCycleState<'_>,
    backend_buffer: &mut BytesMut,
    shape: crate::engine::io_runtime::FrontendCycleShape,
    progress: &mut crate::engine::io_runtime::BackendCycleProgress,
    forwarded_frames: &mut Vec<([u8; 5], Bytes)>,
) -> anyhow::Result<Option<ReadyStatus>> {
    progress.response_started = state.progress.response_started;
    let mut drain = crate::engine::io_runtime::BackendResponseDrain::resume(shape, *progress);
    let event = drain.drain_with(backend_buffer, forwarded_frames, |frame| {
        state.progress.response_started = true;
        if frame.tag == u8::from(BackendTag::DataRow) {
            state.progress.rows = state.progress.rows.saturating_add(1);
        }
        if frame.tag == u8::from(BackendTag::ErrorResponse) {
            state.progress.error = true;
        }
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

        Ok(())
    })?;
    *progress = drain.progress();
    Ok(match event {
        crate::engine::io_runtime::ResponseDrainEvent::Frames { ready, .. } => ready,
        crate::engine::io_runtime::ResponseDrainEvent::BufferLimitExceeded
        | crate::engine::io_runtime::ResponseDrainEvent::NeedMoreBytes => None,
    })
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
    Flushed,
    AbandonedResponse { needs_sync: bool },
    BufferLimitExceeded,
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
        pg_kinetic_core::protocol::session::TransactionShardDecision::Rejected
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
        FrontendTag::Flush,
        FrontendTag::Sync,
    ]
    .iter()
    .any(|tag| frame.tag == u8::from(*tag))
    {
        return Ok(());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_shape_tracks_batched_simple_queries() {
        let simple_frames = vec![
            simple_query_frame("select 1"),
            simple_query_frame("select 2"),
            simple_query_frame("select 3"),
        ];
        let sync_frame = FrontendFrame {
            tag: u8::from(FrontendTag::Sync),
            payload: Bytes::new(),
        };

        let simple_shape =
            crate::engine::io_runtime::FrontendCycleShape::from_frames(&simple_frames);
        let sync_shape = crate::engine::io_runtime::FrontendCycleShape::from_frames(&[sync_frame]);

        assert_eq!(simple_shape.expected_ready_count(), 3);
        assert!(!simple_shape.needs_sync());
        assert_eq!(sync_shape.expected_ready_count(), 1);
        assert!(sync_shape.needs_sync());
    }
}
