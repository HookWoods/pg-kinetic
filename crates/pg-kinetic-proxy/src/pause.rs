use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct PauseController {
    paused: AtomicBool,
    resumed: Notify,
}

impl PauseController {
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        self.resumed.notify_waiters();
    }

    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub async fn wait_if_paused(&self) {
        while self.is_paused() {
            let notified = self.resumed.notified();
            if !self.is_paused() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;

    #[tokio::test]
    async fn resume_releases_waiters() {
        let controller = Arc::new(PauseController::default());
        controller.pause();
        let waiter = {
            let controller = Arc::clone(&controller);
            tokio::spawn(async move {
                controller.wait_if_paused().await;
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        controller.resume();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("released")
            .expect("join");
    }
}
