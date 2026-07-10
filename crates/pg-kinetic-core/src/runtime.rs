use std::{fmt, str::FromStr, sync::Arc};

use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum RuntimeLifecycleState {
    #[default]
    Starting,
    Ready,
    Draining,
    Stopping,
    Stopped,
}

impl RuntimeLifecycleState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Starting, Self::Ready | Self::Stopping)
                | (Self::Ready, Self::Draining | Self::Stopping)
                | (Self::Draining, Self::Stopping)
                | (Self::Stopping, Self::Stopped)
        )
    }
}

impl fmt::Display for RuntimeLifecycleState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ReadinessState {
    Ready,
    #[default]
    NotReady,
    Draining,
}

impl ReadinessState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NotReady => "not_ready",
            Self::Draining => "draining",
        }
    }
}

impl fmt::Display for ReadinessState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ShutdownReason {
    Signal,
    AdminRequest,
    PreStopHook,
    StartupFailure,
    RuntimeFailure,
}

impl ShutdownReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Signal => "signal",
            Self::AdminRequest => "admin_request",
            Self::PreStopHook => "pre_stop_hook",
            Self::StartupFailure => "startup_failure",
            Self::RuntimeFailure => "runtime_failure",
        }
    }
}

impl fmt::Display for ShutdownReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LifecycleTransition {
    from: RuntimeLifecycleState,
    to: RuntimeLifecycleState,
}

impl LifecycleTransition {
    pub const fn new(
        from: RuntimeLifecycleState,
        to: RuntimeLifecycleState,
    ) -> Result<Self, LifecycleTransitionError> {
        if from.can_transition_to(to) {
            Ok(Self { from, to })
        } else {
            Err(LifecycleTransitionError::InvalidTransition { from, to })
        }
    }

    #[must_use]
    pub const fn from(self) -> RuntimeLifecycleState {
        self.from
    }

    #[must_use]
    pub const fn to(self) -> RuntimeLifecycleState {
        self.to
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LifecycleTransitionError {
    #[error("invalid lifecycle transition from {from} to {to}")]
    InvalidTransition {
        from: RuntimeLifecycleState,
        to: RuntimeLifecycleState,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NodeId(Arc<str>);

impl NodeId {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, NodeIdValidationError> {
        let value = value.into();
        if value.is_empty() {
            Err(NodeIdValidationError::EmptyNodeId)
        } else {
            Ok(Self(value))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for NodeId {
    type Err = NodeIdValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum NodeIdValidationError {
    #[error("node id cannot be empty")]
    EmptyNodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetadata {
    node_id: NodeId,
    hostname: Arc<str>,
    process_id: u32,
}

impl NodeMetadata {
    #[must_use]
    pub fn new(node_id: NodeId, hostname: impl Into<Arc<str>>, process_id: u32) -> Self {
        Self {
            node_id,
            hostname: hostname.into(),
            process_id,
        }
    }

    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    #[must_use]
    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    #[must_use]
    pub const fn process_id(&self) -> u32 {
        self.process_id
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum RuntimeEngine {
    #[default]
    TokioDefault,
    TokioCurrentThread,
    ExperimentalThreadPerCore,
    ExperimentalIoUring,
}

impl RuntimeEngine {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TokioDefault => "tokio_default",
            Self::TokioCurrentThread => "tokio_current_thread",
            Self::ExperimentalThreadPerCore => "experimental_thread_per_core",
            Self::ExperimentalIoUring => "experimental_io_uring",
        }
    }

    #[must_use]
    pub const fn status(self) -> RuntimeEngineStatus {
        match self {
            Self::TokioDefault | Self::TokioCurrentThread => RuntimeEngineStatus::Stable,
            Self::ExperimentalThreadPerCore | Self::ExperimentalIoUring => {
                RuntimeEngineStatus::Experimental
            }
        }
    }

    #[must_use]
    pub const fn is_experimental(self) -> bool {
        matches!(self.status(), RuntimeEngineStatus::Experimental)
    }
}

impl fmt::Display for RuntimeEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuntimeEngineStatus {
    Stable,
    Experimental,
    Unsupported,
}

impl RuntimeEngineStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Experimental => "experimental",
            Self::Unsupported => "unsupported",
        }
    }
}

impl fmt::Display for RuntimeEngineStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum RuntimePreflightStatus {
    #[default]
    Passed,
    Warning,
    Failed,
}

impl RuntimePreflightStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Warning => "warning",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for RuntimePreflightStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
