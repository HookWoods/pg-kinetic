use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use pg_kinetic_core::runtime::{
    LifecycleTransition, ReadinessState, RuntimeLifecycleState, ShutdownReason,
};
use tokio::sync::watch;

use crate::drain::{DrainClientGuard, DrainController, DrainOutcome};

const LIFECYCLE_STATE_METRIC: &str = "pg_kinetic_runtime_lifecycle_state";
const READINESS_STATE_METRIC: &str = "pg_kinetic_runtime_readiness_state";
const SHUTDOWN_TOTAL_METRIC: &str = "pg_kinetic_runtime_shutdown_total";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleState {
    lifecycle: RuntimeLifecycleState,
    readiness: ReadinessState,
    listeners_initialized: bool,
    backend_pools_initialized: bool,
    active_sessions: usize,
    shutdown_reason: Option<ShutdownReason>,
    transition_count: u64,
    force_close_requested: bool,
}

impl LifecycleState {
    #[must_use]
    pub const fn lifecycle(&self) -> RuntimeLifecycleState {
        self.lifecycle
    }

    #[must_use]
    pub const fn readiness(&self) -> ReadinessState {
        self.readiness
    }

    #[must_use]
    pub const fn listeners_initialized(&self) -> bool {
        self.listeners_initialized
    }

    #[must_use]
    pub const fn backend_pools_initialized(&self) -> bool {
        self.backend_pools_initialized
    }

    #[must_use]
    pub const fn active_sessions(&self) -> usize {
        self.active_sessions
    }

    #[must_use]
    pub const fn shutdown_reason(&self) -> Option<ShutdownReason> {
        self.shutdown_reason
    }

    #[must_use]
    pub const fn transition_count(&self) -> u64 {
        self.transition_count
    }

    #[must_use]
    pub const fn force_close_requested(&self) -> bool {
        self.force_close_requested
    }
}

#[derive(Debug)]
pub struct ReadinessGate {
    readiness_fail_during_drain: AtomicBool,
}

impl ReadinessGate {
    #[must_use]
    pub const fn new(readiness_fail_during_drain: bool) -> Self {
        Self {
            readiness_fail_during_drain: AtomicBool::new(readiness_fail_during_drain),
        }
    }

    pub fn configure(&self, readiness_fail_during_drain: bool) {
        self.readiness_fail_during_drain
            .store(readiness_fail_during_drain, Ordering::Release);
    }

    #[must_use]
    pub fn evaluate(
        &self,
        lifecycle: RuntimeLifecycleState,
        listeners_initialized: bool,
        backend_pools_initialized: bool,
    ) -> ReadinessState {
        match lifecycle {
            RuntimeLifecycleState::Ready if listeners_initialized && backend_pools_initialized => {
                ReadinessState::Ready
            }
            RuntimeLifecycleState::Draining
                if !self.readiness_fail_during_drain.load(Ordering::Acquire) =>
            {
                ReadinessState::Draining
            }
            RuntimeLifecycleState::Starting
            | RuntimeLifecycleState::Ready
            | RuntimeLifecycleState::Draining
            | RuntimeLifecycleState::Stopping
            | RuntimeLifecycleState::Stopped => ReadinessState::NotReady,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LifecycleController {
    inner: Arc<LifecycleInner>,
}

#[derive(Debug)]
struct LifecycleInner {
    state: Mutex<LifecycleState>,
    readiness_gate: ReadinessGate,
    drain: Arc<DrainController>,
    drain_grace: Mutex<Duration>,
    shutdown_grace: Mutex<Duration>,
    updates: watch::Sender<LifecycleState>,
    force_close: watch::Sender<bool>,
}

impl LifecycleController {
    #[must_use]
    pub fn new(
        drain: Arc<DrainController>,
        drain_grace: Duration,
        shutdown_grace: Duration,
        readiness_fail_during_drain: bool,
    ) -> Self {
        let initial = LifecycleState {
            lifecycle: RuntimeLifecycleState::Starting,
            readiness: ReadinessState::NotReady,
            listeners_initialized: false,
            backend_pools_initialized: false,
            active_sessions: 0,
            shutdown_reason: None,
            transition_count: 0,
            force_close_requested: false,
        };
        let (updates, _updates_receiver) = watch::channel(initial.clone());
        let (force_close, _force_close_receiver) = watch::channel(false);
        let controller = Self {
            inner: Arc::new(LifecycleInner {
                state: Mutex::new(initial),
                readiness_gate: ReadinessGate::new(readiness_fail_during_drain),
                drain,
                drain_grace: Mutex::new(drain_grace),
                shutdown_grace: Mutex::new(shutdown_grace),
                updates,
                force_close,
            }),
        };
        controller.publish_current();
        controller
    }

    #[must_use]
    pub fn state(&self) -> LifecycleState {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("lifecycle state lock")
            .clone();
        state.active_sessions = self.inner.drain.active_clients();
        state
    }

    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<LifecycleState> {
        self.inner.updates.subscribe()
    }

    #[must_use]
    pub fn force_close_receiver(&self) -> watch::Receiver<bool> {
        self.inner.force_close.subscribe()
    }

    #[must_use]
    pub fn drain_controller(&self) -> Arc<DrainController> {
        Arc::clone(&self.inner.drain)
    }

    #[must_use]
    pub fn drain_token(&self) -> DrainToken {
        DrainToken {
            lifecycle: self.clone(),
        }
    }

    pub fn mark_listeners_initialized(&self) {
        self.update(|state| state.listeners_initialized = true);
        self.maybe_mark_ready();
    }

    pub fn mark_backend_pools_initialized(&self) {
        self.update(|state| state.backend_pools_initialized = true);
        self.maybe_mark_ready();
    }

    pub fn configure(
        &self,
        drain_grace: Duration,
        shutdown_grace: Duration,
        readiness_fail_during_drain: bool,
    ) {
        *self.inner.drain_grace.lock().expect("drain grace lock") = drain_grace;
        *self
            .inner
            .shutdown_grace
            .lock()
            .expect("shutdown grace lock") = shutdown_grace;
        self.inner
            .readiness_gate
            .configure(readiness_fail_during_drain);
        self.update(|_| {});
    }

    pub fn begin_drain(&self, reason: ShutdownReason) -> bool {
        let drain_grace = *self.inner.drain_grace.lock().expect("drain grace lock");
        let began_drain = self.inner.drain.begin_drain(drain_grace);
        let transitioned = self.update(|state| {
            if state.lifecycle == RuntimeLifecycleState::Ready {
                state.lifecycle = RuntimeLifecycleState::Draining;
                state.transition_count = state.transition_count.saturating_add(1);
            }
            if state.shutdown_reason.is_none() {
                state.shutdown_reason = Some(reason);
            }
        });

        if began_drain || transitioned {
            metrics_crate::counter!(
                SHUTDOWN_TOTAL_METRIC,
                "reason" => reason.as_str()
            )
            .increment(1);
        }
        began_drain || transitioned
    }

    fn maybe_mark_ready(&self) {
        self.update(|state| {
            if state.lifecycle == RuntimeLifecycleState::Starting
                && state.listeners_initialized
                && state.backend_pools_initialized
            {
                let transition = LifecycleTransition::new(
                    RuntimeLifecycleState::Starting,
                    RuntimeLifecycleState::Ready,
                )
                .expect("starting to ready transition");
                state.lifecycle = transition.to();
                state.transition_count = state.transition_count.saturating_add(1);
            }
        });
    }

    fn mark_stopping(&self) {
        self.update(|state| {
            if matches!(
                state.lifecycle,
                RuntimeLifecycleState::Starting
                    | RuntimeLifecycleState::Ready
                    | RuntimeLifecycleState::Draining
            ) {
                state.lifecycle = RuntimeLifecycleState::Stopping;
                state.transition_count = state.transition_count.saturating_add(1);
            }
        });
    }

    fn request_force_close(&self) {
        self.update(|state| state.force_close_requested = true);
        self.inner.force_close.send_replace(true);
    }

    fn mark_stopped(&self) {
        self.inner.drain.finish_drain();
        self.update(|state| {
            if state.lifecycle == RuntimeLifecycleState::Stopping {
                let transition = LifecycleTransition::new(
                    RuntimeLifecycleState::Stopping,
                    RuntimeLifecycleState::Stopped,
                )
                .expect("stopping to stopped transition");
                state.lifecycle = transition.to();
                state.transition_count = state.transition_count.saturating_add(1);
            }
        });
    }

    fn update(&self, update: impl FnOnce(&mut LifecycleState)) -> bool {
        let previous = self.state();
        {
            let mut state = self.inner.state.lock().expect("lifecycle state lock");
            update(&mut state);
            state.readiness = self.inner.readiness_gate.evaluate(
                state.lifecycle,
                state.listeners_initialized,
                state.backend_pools_initialized,
            );
            state.active_sessions = self.inner.drain.active_clients();
        }
        let current = self.state();
        if current != previous {
            self.publish(current);
            true
        } else {
            false
        }
    }

    fn publish_current(&self) {
        self.publish(self.state());
    }

    fn publish(&self, state: LifecycleState) {
        self.inner.updates.send_replace(state.clone());
        record_lifecycle_metrics(&state);
    }
}

#[derive(Clone, Debug)]
pub struct DrainToken {
    lifecycle: LifecycleController,
}

impl DrainToken {
    #[must_use]
    pub fn begin(&self, reason: ShutdownReason) -> bool {
        self.lifecycle.begin_drain(reason)
    }

    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.lifecycle.inner.drain.is_accepting()
            && self.lifecycle.state().lifecycle == RuntimeLifecycleState::Ready
    }

    #[must_use]
    pub fn try_enter(&self) -> Option<LifecycleSessionGuard> {
        if !self.is_accepting() {
            return None;
        }
        let guard = self.lifecycle.inner.drain.try_enter_client()?;
        self.lifecycle.publish_current();
        Some(LifecycleSessionGuard {
            guard: Some(guard),
            lifecycle: self.lifecycle.clone(),
        })
    }
}

#[derive(Debug)]
pub struct LifecycleSessionGuard {
    guard: Option<DrainClientGuard>,
    lifecycle: LifecycleController,
}

impl Drop for LifecycleSessionGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.lifecycle.publish_current();
    }
}

#[derive(Clone, Debug)]
pub struct ShutdownCoordinator {
    lifecycle: LifecycleController,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownOutcome {
    drain_outcome: DrainOutcome,
    forced_sessions: usize,
}

impl ShutdownOutcome {
    #[must_use]
    pub const fn drain_outcome(self) -> DrainOutcome {
        self.drain_outcome
    }

    #[must_use]
    pub const fn forced_sessions(self) -> usize {
        self.forced_sessions
    }
}

impl ShutdownCoordinator {
    #[must_use]
    pub const fn new(lifecycle: LifecycleController) -> Self {
        Self { lifecycle }
    }

    pub async fn coordinate(&self) -> ShutdownOutcome {
        let drain_outcome = self.lifecycle.inner.drain.wait_for_completion().await;
        self.lifecycle.mark_stopping();

        let forced_sessions = if drain_outcome == DrainOutcome::TimedOut {
            let shutdown_grace = *self
                .lifecycle
                .inner
                .shutdown_grace
                .lock()
                .expect("shutdown grace lock");
            tokio::time::sleep(shutdown_grace).await;
            let active_sessions = self.lifecycle.inner.drain.active_clients();
            if active_sessions > 0 {
                self.lifecycle.request_force_close();
            }
            active_sessions
        } else {
            0
        };

        ShutdownOutcome {
            drain_outcome,
            forced_sessions,
        }
    }

    pub fn complete(&self) {
        self.lifecycle.mark_stopped();
    }
}

#[cfg(unix)]
pub async fn wait_for_shutdown_signal() -> std::io::Result<ShutdownReason> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            Ok(ShutdownReason::Signal)
        }
        _ = terminate.recv() => Ok(ShutdownReason::Signal),
    }
}

#[cfg(not(unix))]
pub async fn wait_for_shutdown_signal() -> std::io::Result<ShutdownReason> {
    tokio::signal::ctrl_c().await?;
    Ok(ShutdownReason::Signal)
}

fn record_lifecycle_metrics(state: &LifecycleState) {
    for lifecycle in [
        RuntimeLifecycleState::Starting,
        RuntimeLifecycleState::Ready,
        RuntimeLifecycleState::Draining,
        RuntimeLifecycleState::Stopping,
        RuntimeLifecycleState::Stopped,
    ] {
        metrics_crate::gauge!(
            LIFECYCLE_STATE_METRIC,
            "state" => lifecycle.as_str()
        )
        .set(f64::from(lifecycle == state.lifecycle));
    }
    for readiness in [
        ReadinessState::Ready,
        ReadinessState::NotReady,
        ReadinessState::Draining,
    ] {
        metrics_crate::gauge!(
            READINESS_STATE_METRIC,
            "state" => readiness.as_str()
        )
        .set(f64::from(readiness == state.readiness));
    }
}
