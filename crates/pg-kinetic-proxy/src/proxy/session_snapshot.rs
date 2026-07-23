use super::*;

#[derive(Debug)]
pub(super) struct ClientSnapshotGuard {
    handle: ClientSnapshotHandle,
    client_id: u64,
    session_id: u64,
    started: Instant,
    client_addr: SocketAddr,
    debug_sampler: DebugSampler,
}

impl ClientSnapshotGuard {
    pub(super) fn new(
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
pub(super) fn update_client_snapshot(
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

pub(super) fn record_pinning_snapshot(
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

pub(super) fn snapshot_pin_reason(reason: PinReason) -> SessionPinReason {
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

pub(super) fn clear_pinned_backend(
    pinned_backend: &mut PinnedBackend,
    snapshot_store: &SnapshotStore,
    client_id: u64,
) {
    pinned_backend.clear();
    let _ = snapshot_store.remove_pinning_snapshot(client_id);
}
