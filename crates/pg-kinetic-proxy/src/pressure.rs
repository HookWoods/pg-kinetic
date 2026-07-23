use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::{sync::RwLock, time::sleep};

use crate::{
    buffers::ProxyBufferPool,
    config::{Config, PressureConfig},
    limits::{self, Pressure},
    snapshot::{PressureSnapshot, SnapshotStore},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PressureAction {
    Hold,
    Reduce,
    Restore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PressureDecision {
    pub action: PressureAction,
    pub route_in_flight_limit: usize,
    pub pressured: bool,
}

#[must_use]
pub fn pressure_step(
    current_limit: usize,
    configured_limit: usize,
    pressure: Option<Pressure>,
    config: &PressureConfig,
) -> PressureDecision {
    let configured_limit = configured_limit.max(1);
    let current_limit = current_limit.max(1).min(configured_limit);
    if !config.enabled {
        return PressureDecision {
            action: if current_limit < configured_limit {
                PressureAction::Restore
            } else {
                PressureAction::Hold
            },
            route_in_flight_limit: configured_limit,
            pressured: false,
        };
    }

    let pressured = pressure.is_some_and(|pressure| {
        pressure.cpu_some_avg10 >= config.cpu_high_pct
            || pressure.mem_some_avg10 >= config.mem_high_pct
    });
    let floor = config.min_in_flight_floor.max(1).min(configured_limit);

    if pressured {
        let reduction = (current_limit / 4).max(1);
        let next = current_limit.saturating_sub(reduction).max(floor);
        return PressureDecision {
            action: if next < current_limit {
                PressureAction::Reduce
            } else {
                PressureAction::Hold
            },
            route_in_flight_limit: next,
            pressured: true,
        };
    }

    if current_limit < configured_limit {
        let restore = (configured_limit / 10).max(1);
        return PressureDecision {
            action: PressureAction::Restore,
            route_in_flight_limit: current_limit.saturating_add(restore).min(configured_limit),
            pressured: false,
        };
    }

    PressureDecision {
        action: PressureAction::Hold,
        route_in_flight_limit: configured_limit,
        pressured: false,
    }
}

#[derive(Clone, Debug)]
pub struct PressureController {
    active_config: Arc<RwLock<Config>>,
    snapshot_store: SnapshotStore,
    route_in_flight_limit: Arc<AtomicUsize>,
    buffer_pool: ProxyBufferPool,
}

impl PressureController {
    #[must_use]
    pub fn new(
        active_config: Arc<RwLock<Config>>,
        snapshot_store: SnapshotStore,
        route_in_flight_limit: Arc<AtomicUsize>,
        buffer_pool: ProxyBufferPool,
    ) -> Self {
        Self {
            active_config,
            snapshot_store,
            route_in_flight_limit,
            buffer_pool,
        }
    }

    pub async fn tick(&self) {
        let config = self.active_config.read().await.clone();
        let pressure = limits::read_pressure();
        let configured_limit = config.qos.max_route_in_flight.max(1);
        let current_limit = self.route_in_flight_limit.load(Ordering::Acquire);
        let pressure_config = &config.runtime.production.pressure;
        let decision = pressure_step(current_limit, configured_limit, pressure, pressure_config);
        if decision.route_in_flight_limit != current_limit {
            self.route_in_flight_limit
                .store(decision.route_in_flight_limit, Ordering::Release);
        }
        if decision.action == PressureAction::Reduce
            && pressure
                .is_some_and(|pressure| pressure.mem_some_avg10 >= pressure_config.mem_high_pct)
        {
            self.buffer_pool.trim_cached();
        }
        self.snapshot_store.set_pressure_snapshot(PressureSnapshot {
            enabled: pressure_config.enabled,
            cpu_some_avg10: pressure.map(|pressure| pressure.cpu_some_avg10),
            mem_some_avg10: pressure.map(|pressure| pressure.mem_some_avg10),
            cpu_high_pct: pressure_config.cpu_high_pct,
            mem_high_pct: pressure_config.mem_high_pct,
            pressured: decision.pressured,
            route_in_flight_limit: decision.route_in_flight_limit,
            configured_route_in_flight: configured_limit,
        });
    }

    pub async fn run(self) {
        loop {
            self.tick().await;
            let interval = {
                let config = self.active_config.read().await;
                config.runtime.production.pressure.window()
            };
            sleep(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_step_reduces_toward_floor() {
        let config = PressureConfig {
            enabled: true,
            cpu_high_pct: 20.0,
            mem_high_pct: 10.0,
            min_in_flight_floor: 4,
            window_ms: 5_000,
        };

        let decision = pressure_step(
            16,
            32,
            Some(Pressure {
                cpu_some_avg10: 25.0,
                mem_some_avg10: 0.0,
            }),
            &config,
        );

        assert_eq!(decision.action, PressureAction::Reduce);
        assert_eq!(decision.route_in_flight_limit, 12);
        assert!(decision.pressured);
    }

    #[test]
    fn pressure_step_restores_gradually() {
        let config = PressureConfig {
            enabled: true,
            cpu_high_pct: 20.0,
            mem_high_pct: 10.0,
            min_in_flight_floor: 1,
            window_ms: 5_000,
        };

        let decision = pressure_step(
            12,
            32,
            Some(Pressure {
                cpu_some_avg10: 0.0,
                mem_some_avg10: 0.0,
            }),
            &config,
        );

        assert_eq!(decision.action, PressureAction::Restore);
        assert_eq!(decision.route_in_flight_limit, 15);
        assert!(!decision.pressured);
    }
}
