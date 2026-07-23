use std::{os::fd::RawFd, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpliceConfig {
    pub enabled: bool,
    pub wait_poll_interval: Duration,
}

impl Default for SpliceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            wait_poll_interval: Duration::from_millis(1),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CycleEvent {
    pub cookie: u64,
    pub ready_status: u8,
    pub saw_error: u8,
    pub protocol_uncertain: u8,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpliceAvailability {
    Disabled,
    UnsupportedPlatform,
    FeatureNotEnabled,
    Available,
}

#[derive(Debug)]
pub struct SpliceManager;

#[derive(Debug)]
pub struct SpliceGuard;

impl SpliceManager {
    pub fn availability(config: SpliceConfig) -> SpliceAvailability {
        if !config.enabled {
            return SpliceAvailability::Disabled;
        }
        if !cfg!(target_os = "linux") {
            return SpliceAvailability::UnsupportedPlatform;
        }
        if !cfg!(feature = "sockmap-bypass") {
            return SpliceAvailability::FeatureNotEnabled;
        }
        SpliceAvailability::Available
    }

    pub fn try_load(config: SpliceConfig) -> anyhow::Result<Option<Self>> {
        match Self::availability(config) {
            SpliceAvailability::Available => {
                anyhow::bail!("sockmap bypass loader is not wired in this build step")
            }
            SpliceAvailability::Disabled
            | SpliceAvailability::UnsupportedPlatform
            | SpliceAvailability::FeatureNotEnabled => Ok(None),
        }
    }

    pub fn engage(
        &self,
        _backend_fd: RawFd,
        _client_fd: RawFd,
        _cookie: u64,
    ) -> anyhow::Result<SpliceGuard> {
        anyhow::bail!("sockmap bypass manager is unavailable")
    }

    pub async fn wait_cycle(&self, _cookie: u64, _timeout: Duration) -> anyhow::Result<CycleEvent> {
        anyhow::bail!("sockmap bypass manager is unavailable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_config_returns_no_manager() {
        let config = SpliceConfig {
            enabled: false,
            wait_poll_interval: Duration::from_millis(1),
        };

        assert_eq!(
            SpliceManager::availability(config),
            SpliceAvailability::Disabled
        );
        assert!(SpliceManager::try_load(config)
            .expect("load result")
            .is_none());
    }

    #[test]
    fn enabled_config_reports_platform_or_feature_gate() {
        let config = SpliceConfig {
            enabled: true,
            wait_poll_interval: Duration::from_millis(1),
        };

        let availability = SpliceManager::availability(config);
        if cfg!(target_os = "linux") && cfg!(feature = "sockmap-bypass") {
            assert_eq!(availability, SpliceAvailability::Available);
        } else if cfg!(target_os = "linux") {
            assert_eq!(availability, SpliceAvailability::FeatureNotEnabled);
        } else {
            assert_eq!(availability, SpliceAvailability::UnsupportedPlatform);
        }
    }
}
