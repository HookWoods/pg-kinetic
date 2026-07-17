use pg_kinetic_core::runtime::{RuntimeEngine, RuntimeEngineStatus};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeEngineExperiment {
    config_enabled: bool,
}

impl RuntimeEngineExperiment {
    #[must_use]
    pub const fn new(config_enabled: bool) -> Self {
        Self { config_enabled }
    }

    #[must_use]
    pub const fn config_enabled(self) -> bool {
        self.config_enabled
    }

    #[must_use]
    pub const fn feature_enabled(self) -> bool {
        cfg!(feature = "runtime-experiments")
    }

    #[must_use]
    pub const fn is_enabled(self) -> bool {
        self.config_enabled && self.feature_enabled()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeEngineSelector {
    engine: RuntimeEngine,
    experiment: RuntimeEngineExperiment,
}

impl Default for RuntimeEngineSelector {
    fn default() -> Self {
        Self::new(RuntimeEngine::default())
    }
}

impl RuntimeEngineSelector {
    #[must_use]
    pub const fn new(engine: RuntimeEngine) -> Self {
        Self {
            engine,
            experiment: RuntimeEngineExperiment::new(false),
        }
    }

    #[must_use]
    pub const fn with_experiment(self, experiment: RuntimeEngineExperiment) -> Self {
        Self { experiment, ..self }
    }

    #[must_use]
    pub const fn engine(self) -> RuntimeEngine {
        self.engine
    }

    #[must_use]
    pub const fn experiment(self) -> RuntimeEngineExperiment {
        self.experiment
    }

    #[must_use]
    pub const fn benchmark_label(self) -> &'static str {
        self.engine.as_str()
    }

    #[must_use]
    pub const fn capabilities(self) -> RuntimeEngineCapabilities {
        RuntimeEngineCapabilities::new(self.engine, self.experiment)
    }

    #[must_use]
    pub fn selection_snapshot(self) -> RuntimeEngineSelectionSnapshot {
        self.capabilities().selection_snapshot()
    }

    #[must_use]
    pub fn selection_metrics(self) -> RuntimeEngineSelectionMetrics {
        self.capabilities().selection_metrics()
    }

    pub fn validate(self) -> Result<(), RuntimeEngineSelectionError> {
        self.capabilities().validate()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeEngineCapabilities {
    engine: RuntimeEngine,
    experiment: RuntimeEngineExperiment,
}

impl RuntimeEngineCapabilities {
    #[must_use]
    pub const fn new(engine: RuntimeEngine, experiment: RuntimeEngineExperiment) -> Self {
        Self { engine, experiment }
    }

    #[must_use]
    pub const fn engine(self) -> RuntimeEngine {
        self.engine
    }

    #[must_use]
    pub const fn experiment(self) -> RuntimeEngineExperiment {
        self.experiment
    }

    #[must_use]
    pub const fn benchmark_label(self) -> &'static str {
        self.engine.as_str()
    }

    #[must_use]
    pub const fn platform_supported(self) -> bool {
        match self.engine {
            RuntimeEngine::ExperimentalIoUring => cfg!(target_os = "linux"),
            _ => true,
        }
    }

    #[must_use]
    pub const fn is_stable(self) -> bool {
        matches!(self.engine.status(), RuntimeEngineStatus::Stable)
    }

    #[must_use]
    pub const fn is_experimental(self) -> bool {
        self.engine.is_experimental()
    }

    #[must_use]
    pub const fn is_enabled(self) -> bool {
        if self.is_experimental() {
            self.experiment.is_enabled()
        } else {
            true
        }
    }

    #[must_use]
    pub const fn is_available(self) -> bool {
        self.platform_supported() && self.is_enabled()
    }

    #[must_use]
    pub const fn status(self) -> RuntimeEngineStatus {
        if self.platform_supported() {
            self.engine.status()
        } else {
            RuntimeEngineStatus::Unsupported
        }
    }

    #[must_use]
    pub fn selection_snapshot(self) -> RuntimeEngineSelectionSnapshot {
        RuntimeEngineSelectionSnapshot {
            runtime_engine: self.engine,
            status: self.status(),
            platform_supported: self.platform_supported(),
            experimental_enabled: self.experiment.is_enabled(),
            available: self.is_available(),
            benchmark_label: self.benchmark_label(),
            platform: current_platform_label(),
        }
    }

    #[must_use]
    pub fn selection_metrics(self) -> RuntimeEngineSelectionMetrics {
        RuntimeEngineSelectionMetrics {
            runtime_engine: self.engine.as_str(),
            status: self.status().as_str(),
            availability: if self.is_available() {
                "available"
            } else {
                "blocked"
            },
            platform: current_platform_label(),
            benchmark_label: self.benchmark_label(),
        }
    }

    pub fn validate(self) -> Result<(), RuntimeEngineSelectionError> {
        if !self.platform_supported() {
            return Err(RuntimeEngineSelectionError::UnsupportedPlatform {
                engine: self.engine,
                platform: current_platform_label(),
            });
        }

        if self.is_experimental() && !self.experiment.is_enabled() {
            return Err(RuntimeEngineSelectionError::ExperimentalDisabled {
                engine: self.engine,
            });
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeEngineSelectionSnapshot {
    pub runtime_engine: RuntimeEngine,
    pub status: RuntimeEngineStatus,
    pub platform_supported: bool,
    pub experimental_enabled: bool,
    pub available: bool,
    pub benchmark_label: &'static str,
    pub platform: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeEngineSelectionMetrics {
    pub runtime_engine: &'static str,
    pub status: &'static str,
    pub availability: &'static str,
    pub platform: &'static str,
    pub benchmark_label: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeEngineSelectionError {
    #[error("runtime engine '{engine}' is unsupported on {platform}")]
    UnsupportedPlatform {
        engine: RuntimeEngine,
        platform: &'static str,
    },
    #[error(
        "runtime engine '{engine}' requires the runtime-experiments feature and an explicit config gate"
    )]
    ExperimentalDisabled { engine: RuntimeEngine },
}

fn current_platform_label() -> &'static str {
    std::env::consts::OS
}
