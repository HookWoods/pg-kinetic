use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pg_kinetic_core::{protocol::session::PinReason, traffic::routing::QueryClass};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BreakerConfig {
    pub failure_threshold: usize,
    pub cooldown: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown: Duration::from_secs(5),
        }
    }
}

#[derive(Debug)]
struct BreakerData {
    config: BreakerConfig,
    state: BreakerState,
    failures: usize,
    opened_at: Option<Instant>,
    probe_in_flight: bool,
}

#[derive(Debug)]
pub struct BackendBreaker {
    data: Mutex<BreakerData>,
}

impl BackendBreaker {
    #[must_use]
    pub fn new(config: BreakerConfig) -> Self {
        Self {
            data: Mutex::new(BreakerData {
                config,
                state: BreakerState::Closed,
                failures: 0,
                opened_at: None,
                probe_in_flight: false,
            }),
        }
    }

    #[must_use]
    pub fn allow_request(&self, now: Instant) -> bool {
        let mut data = self.lock();
        if data.state == BreakerState::Open
            && data
                .opened_at
                .is_some_and(|opened_at| now.duration_since(opened_at) >= data.config.cooldown)
        {
            data.state = BreakerState::HalfOpen;
            data.probe_in_flight = false;
        }

        match data.state {
            BreakerState::Closed => true,
            BreakerState::Open => false,
            BreakerState::HalfOpen if !data.probe_in_flight => {
                data.probe_in_flight = true;
                true
            }
            BreakerState::HalfOpen => false,
        }
    }

    pub fn record_success(&self) {
        let mut data = self.lock();
        data.state = BreakerState::Closed;
        data.failures = 0;
        data.opened_at = None;
        data.probe_in_flight = false;
    }

    pub fn record_failure(&self, now: Instant) {
        let mut data = self.lock();
        data.probe_in_flight = false;
        match data.state {
            BreakerState::HalfOpen => {
                data.state = BreakerState::Open;
                data.opened_at = Some(now);
                data.failures = data.config.failure_threshold;
            }
            BreakerState::Closed => {
                data.failures = data.failures.saturating_add(1);
                if data.failures >= data.config.failure_threshold {
                    data.state = BreakerState::Open;
                    data.opened_at = Some(now);
                }
            }
            BreakerState::Open => {}
        }
    }

    #[must_use]
    pub fn state(&self, now: Instant) -> BreakerState {
        let mut data = self.lock();
        if data.state == BreakerState::Open
            && data
                .opened_at
                .is_some_and(|opened_at| now.duration_since(opened_at) >= data.config.cooldown)
        {
            data.state = BreakerState::HalfOpen;
            data.probe_in_flight = false;
        }
        data.state
    }

    #[must_use]
    pub fn failures(&self) -> usize {
        self.lock().failures
    }

    pub fn configure(&self, config: BreakerConfig) {
        self.lock().config = config;
    }

    fn lock(&self) -> MutexGuard<'_, BreakerData> {
        self.data.lock().expect("backend breaker poisoned")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HedgeEligibility {
    pub enabled: bool,
    pub query_class: QueryClass,
    pub freshness_proven: bool,
    pub transaction_active: bool,
    pub pinned: bool,
    pub routing_certain: bool,
    pub safely_replayable: bool,
}

impl HedgeEligibility {
    #[must_use]
    pub const fn is_eligible(self) -> bool {
        self.enabled
            && matches!(
                self.query_class,
                QueryClass::ReadOnly | QueryClass::ReadCandidate
            )
            && self.freshness_proven
            && !self.transaction_active
            && !self.pinned
            && self.routing_certain
            && self.safely_replayable
    }

    #[must_use]
    pub const fn with_pin_reason(mut self, pin_reason: Option<PinReason>) -> Self {
        self.pinned = pin_reason.is_some();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn breaker_transitions_closed_open_half_open_closed() {
        let start = Instant::now();
        let breaker = BackendBreaker::new(BreakerConfig {
            failure_threshold: 2,
            cooldown: Duration::from_secs(10),
        });

        assert!(breaker.allow_request(start));
        breaker.record_failure(start);
        assert_eq!(breaker.state(start), BreakerState::Closed);
        breaker.record_failure(start);
        assert_eq!(breaker.state(start), BreakerState::Open);
        assert!(!breaker.allow_request(start + Duration::from_secs(1)));
        assert!(breaker.allow_request(start + Duration::from_secs(10)));
        assert!(!breaker.allow_request(start + Duration::from_secs(10)));
        breaker.record_success();
        assert_eq!(
            breaker.state(start + Duration::from_secs(10)),
            BreakerState::Closed
        );
    }

    #[test]
    fn failed_half_open_probe_reopens_and_fast_rejects() {
        let start = Instant::now();
        let breaker = BackendBreaker::new(BreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_secs(1),
        });
        breaker.record_failure(start);
        assert_eq!(breaker.state(start), BreakerState::Open);
        assert!(breaker.allow_request(start + Duration::from_secs(1)));
        breaker.record_failure(start + Duration::from_secs(1));
        assert_eq!(
            breaker.state(start + Duration::from_secs(1)),
            BreakerState::Open
        );
        assert!(!breaker.allow_request(start + Duration::from_secs(1)));
    }

    fn eligible() -> HedgeEligibility {
        HedgeEligibility {
            enabled: true,
            query_class: QueryClass::ReadCandidate,
            freshness_proven: true,
            transaction_active: false,
            pinned: false,
            routing_certain: true,
            safely_replayable: true,
        }
    }

    #[test]
    fn hedging_requires_safe_read_and_freshness() {
        assert!(eligible().is_eligible());
        assert!(!HedgeEligibility {
            enabled: false,
            ..eligible()
        }
        .is_eligible());
        assert!(!HedgeEligibility {
            freshness_proven: false,
            ..eligible()
        }
        .is_eligible());
        assert!(!HedgeEligibility {
            query_class: QueryClass::Write,
            ..eligible()
        }
        .is_eligible());
        assert!(!HedgeEligibility {
            transaction_active: true,
            ..eligible()
        }
        .is_eligible());
        assert!(!HedgeEligibility {
            pinned: true,
            ..eligible()
        }
        .is_eligible());
        assert!(!HedgeEligibility {
            routing_certain: false,
            ..eligible()
        }
        .is_eligible());
        assert!(!HedgeEligibility {
            safely_replayable: false,
            ..eligible()
        }
        .is_eligible());
    }
}
