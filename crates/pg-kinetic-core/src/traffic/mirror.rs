use std::{fmt, net::SocketAddr};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MirrorMode {
    #[default]
    Off,
    ReadOnly,
    Explicit,
}

impl MirrorMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::ReadOnly => "read_only",
            Self::Explicit => "explicit",
        }
    }

    #[must_use]
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }
}

impl fmt::Display for MirrorMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorTarget {
    address: SocketAddr,
    isolated: bool,
}

impl MirrorTarget {
    #[must_use]
    pub const fn new(address: SocketAddr, isolated: bool) -> Self {
        Self { address, isolated }
    }

    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    #[must_use]
    pub const fn is_isolated(&self) -> bool {
        self.isolated
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MirrorSafetyGate {
    #[default]
    Disabled,
    TargetConfigured,
    TargetIsolated,
    Writes,
    Transactions,
    Copy,
    ListenNotify,
    TempTable,
    SessionMutation,
    Sampling,
}

impl MirrorSafetyGate {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::TargetConfigured => "target_configured",
            Self::TargetIsolated => "target_isolated",
            Self::Writes => "writes",
            Self::Transactions => "transactions",
            Self::Copy => "copy",
            Self::ListenNotify => "listen_notify",
            Self::TempTable => "temp_table",
            Self::SessionMutation => "session_mutation",
            Self::Sampling => "sampling",
        }
    }
}

impl fmt::Display for MirrorSafetyGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MirrorReason {
    #[default]
    Disabled,
    Eligible,
    TargetMissing,
    TargetSharedWithProduction,
    WritesDisabled,
    TransactionsDisabled,
    CopyDisabled,
    ListenNotifyDisabled,
    TempTableDisabled,
    SessionMutationDisabled,
    SampledOut,
    UnsupportedMode,
}

impl MirrorReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Eligible => "eligible",
            Self::TargetMissing => "target_missing",
            Self::TargetSharedWithProduction => "target_shared_with_production",
            Self::WritesDisabled => "writes_disabled",
            Self::TransactionsDisabled => "transactions_disabled",
            Self::CopyDisabled => "copy_disabled",
            Self::ListenNotifyDisabled => "listen_notify_disabled",
            Self::TempTableDisabled => "temp_table_disabled",
            Self::SessionMutationDisabled => "session_mutation_disabled",
            Self::SampledOut => "sampled_out",
            Self::UnsupportedMode => "unsupported_mode",
        }
    }
}

impl fmt::Display for MirrorReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MirrorOutcome {
    #[default]
    Skipped,
    Mirrored,
    Rejected,
}

impl MirrorOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skipped => "skipped",
            Self::Mirrored => "mirrored",
            Self::Rejected => "rejected",
        }
    }
}

impl fmt::Display for MirrorOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MirrorDecision {
    mode: MirrorMode,
    safety_gate: MirrorSafetyGate,
    reason: MirrorReason,
    outcome: MirrorOutcome,
}

impl MirrorDecision {
    #[must_use]
    pub const fn mirrored(mode: MirrorMode, safety_gate: MirrorSafetyGate) -> Self {
        Self {
            mode,
            safety_gate,
            reason: MirrorReason::Eligible,
            outcome: MirrorOutcome::Mirrored,
        }
    }

    #[must_use]
    pub const fn skipped(
        mode: MirrorMode,
        safety_gate: MirrorSafetyGate,
        reason: MirrorReason,
    ) -> Self {
        Self {
            mode,
            safety_gate,
            reason,
            outcome: MirrorOutcome::Skipped,
        }
    }

    #[must_use]
    pub const fn rejected(
        mode: MirrorMode,
        safety_gate: MirrorSafetyGate,
        reason: MirrorReason,
    ) -> Self {
        Self {
            mode,
            safety_gate,
            reason,
            outcome: MirrorOutcome::Rejected,
        }
    }

    #[must_use]
    pub const fn mode(self) -> MirrorMode {
        self.mode
    }

    #[must_use]
    pub const fn safety_gate(self) -> MirrorSafetyGate {
        self.safety_gate
    }

    #[must_use]
    pub const fn reason(self) -> MirrorReason {
        self.reason
    }

    #[must_use]
    pub const fn outcome(self) -> MirrorOutcome {
        self.outcome
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MirrorSample {
    rate: f64,
}

impl MirrorSample {
    #[must_use]
    pub fn new(rate: f64) -> Self {
        let rate = if rate.is_finite() {
            rate.clamp(0.0, 1.0)
        } else {
            0.0
        };
        Self { rate }
    }

    #[must_use]
    pub const fn rate(self) -> f64 {
        self.rate
    }

    #[must_use]
    pub fn should_sample(self, sample: f64) -> bool {
        sample.is_finite() && sample >= 0.0 && sample <= self.rate
    }
}

impl Default for MirrorSample {
    fn default() -> Self {
        Self::new(0.0)
    }
}
