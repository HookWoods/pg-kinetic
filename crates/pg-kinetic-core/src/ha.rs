use crate::routing::BackendRole;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointHealth {
    Healthy,
    Degraded,
    Unhealthy,
    Unavailable,
}

impl EndpointHealth {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
            Self::Unavailable => "unavailable",
        }
    }

    #[must_use]
    pub const fn is_healthy(self) -> bool {
        matches!(self, Self::Healthy)
    }

    #[must_use]
    pub const fn is_available(self) -> bool {
        !matches!(self, Self::Unavailable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointRoleState {
    Primary,
    Replica,
    Warning,
    Unknown,
}

impl EndpointRoleState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Replica => "replica",
            Self::Warning => "warning",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaLagState {
    Unknown,
    Fresh,
    Lagging,
}

impl ReplicaLagState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Fresh => "fresh",
            Self::Lagging => "lagging",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SplitBrainWarning {
    pub expected_role: BackendRole,
    pub observed_role: BackendRole,
}

impl SplitBrainWarning {
    #[must_use]
    pub const fn new(expected_role: BackendRole, observed_role: BackendRole) -> Self {
        Self {
            expected_role,
            observed_role,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthProbeOutcome {
    pub state: EndpointHealth,
    pub recovered: bool,
    pub consecutive_failures: u32,
}

impl HealthProbeOutcome {
    #[must_use]
    pub const fn new(state: EndpointHealth, recovered: bool, consecutive_failures: u32) -> Self {
        Self {
            state,
            recovered,
            consecutive_failures,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoleProbeOutcome {
    pub state: EndpointRoleState,
    pub warning: Option<SplitBrainWarning>,
}

impl RoleProbeOutcome {
    #[must_use]
    pub const fn new(state: EndpointRoleState, warning: Option<SplitBrainWarning>) -> Self {
        Self { state, warning }
    }
}
