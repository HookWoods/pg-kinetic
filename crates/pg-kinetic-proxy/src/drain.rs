use std::{
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use pg_kinetic_core::security::DrainState;
use tokio::sync::Notify;

#[derive(Debug)]
pub struct DrainController {
    state: AtomicU8,
    active_clients: AtomicUsize,
    drain_started_at: Mutex<Option<Instant>>,
    drain_deadline: Mutex<Option<Instant>>,
    notify: Notify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainOutcome {
    Completed,
    TimedOut,
}

#[derive(Debug)]
pub struct DrainClientGuard {
    controller: Arc<DrainController>,
}

impl DrainController {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(Self::state_to_u8(DrainState::Accepting)),
            active_clients: AtomicUsize::new(0),
            drain_started_at: Mutex::new(None),
            drain_deadline: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    #[must_use]
    pub fn state(&self) -> DrainState {
        Self::state_from_u8(self.state.load(Ordering::Acquire))
    }

    #[must_use]
    pub fn is_accepting(&self) -> bool {
        matches!(self.state(), DrainState::Accepting)
    }

    #[must_use]
    pub fn is_draining(&self) -> bool {
        matches!(self.state(), DrainState::Draining)
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.is_accepting()
    }

    #[must_use]
    pub fn active_clients(&self) -> usize {
        self.active_clients.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn drain_started_at(&self) -> Option<Instant> {
        self.drain_started_at
            .lock()
            .expect("drain started timestamp lock")
            .as_ref()
            .copied()
    }

    #[must_use]
    pub fn drain_deadline(&self) -> Option<Instant> {
        self.drain_deadline
            .lock()
            .expect("drain deadline lock")
            .as_ref()
            .copied()
    }

    pub fn begin_drain(self: &Arc<Self>, timeout: Duration) -> bool {
        let previous = self.state.compare_exchange(
            Self::state_to_u8(DrainState::Accepting),
            Self::state_to_u8(DrainState::Draining),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if previous.is_err() {
            return false;
        }

        let started_at = Instant::now();
        let deadline = started_at
            .checked_add(timeout)
            .unwrap_or_else(|| Instant::now() + timeout);
        *self
            .drain_started_at
            .lock()
            .expect("drain started timestamp lock") = Some(started_at);
        *self.drain_deadline.lock().expect("drain deadline lock") = Some(deadline);
        self.notify.notify_waiters();
        true
    }

    pub fn finish_drain(&self) {
        self.state
            .store(Self::state_to_u8(DrainState::Drained), Ordering::Release);
        self.notify.notify_waiters();
    }

    #[must_use]
    pub fn try_enter_client(self: &Arc<Self>) -> Option<DrainClientGuard> {
        if !self.is_accepting() {
            return None;
        }

        self.active_clients.fetch_add(1, Ordering::AcqRel);
        if self.is_accepting() {
            Some(DrainClientGuard {
                controller: Arc::clone(self),
            })
        } else {
            self.active_clients.fetch_sub(1, Ordering::AcqRel);
            None
        }
    }

    pub async fn wait_for_drain_start(&self) {
        loop {
            if !self.is_accepting() {
                return;
            }
            self.notify.notified().await;
        }
    }

    pub async fn wait_for_completion(&self) -> DrainOutcome {
        let deadline = self.drain_deadline().expect("drain deadline is set");
        let deadline = tokio::time::Instant::from_std(deadline);

        loop {
            if self.active_clients() == 0 {
                return DrainOutcome::Completed;
            }

            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return DrainOutcome::TimedOut,
            }
        }
    }

    const fn state_to_u8(state: DrainState) -> u8 {
        match state {
            DrainState::Accepting => 0,
            DrainState::Draining => 1,
            DrainState::Drained => 2,
        }
    }

    const fn state_from_u8(state: u8) -> DrainState {
        match state {
            0 => DrainState::Accepting,
            1 => DrainState::Draining,
            _ => DrainState::Drained,
        }
    }
}

impl Default for DrainController {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DrainClientGuard {
    fn drop(&mut self) {
        let remaining = self
            .controller
            .active_clients
            .fetch_sub(1, Ordering::AcqRel)
            .saturating_sub(1);
        if remaining == 0 && self.controller.is_draining() {
            self.controller.notify.notify_waiters();
        }
    }
}
