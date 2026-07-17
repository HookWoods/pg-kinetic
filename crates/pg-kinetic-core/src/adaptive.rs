use std::{fmt, str::FromStr, sync::Arc};

use clap::ValueEnum;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AdaptiveMode {
    #[default]
    Recommend,
    Apply,
}

impl AdaptiveMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Recommend => "recommend",
            Self::Apply => "apply",
        }
    }

    #[must_use]
    pub const fn is_apply(self) -> bool {
        matches!(self, Self::Apply)
    }
}

impl fmt::Display for AdaptiveMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AdaptiveMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "recommend" => Ok(Self::Recommend),
            "apply" => Ok(Self::Apply),
            other => Err(format!("unknown adaptive mode '{other}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AdaptiveSignal {
    #[default]
    PoolSizePressure,
    BackpressureThresholdPressure,
    MirrorSamplingPressure,
    TimeoutPressure,
}

impl AdaptiveSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PoolSizePressure => "pool_size_pressure",
            Self::BackpressureThresholdPressure => "backpressure_threshold_pressure",
            Self::MirrorSamplingPressure => "mirror_sampling_pressure",
            Self::TimeoutPressure => "timeout_pressure",
        }
    }
}

impl fmt::Display for AdaptiveSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AdaptiveAction {
    #[default]
    Recommend,
    Apply,
    Reject,
}

impl AdaptiveAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Recommend => "recommend",
            Self::Apply => "apply",
            Self::Reject => "reject",
        }
    }
}

impl fmt::Display for AdaptiveAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AdaptiveOutcome {
    #[default]
    Recommended,
    Applied,
    Rejected,
    Skipped,
}

impl AdaptiveOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Recommended => "recommended",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Skipped => "skipped",
        }
    }
}

impl fmt::Display for AdaptiveOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AdaptiveGuardrail {
    #[default]
    ConfidenceFloor,
    Allowlist,
    MaxChangePercent,
    UnboundedChange,
}

impl AdaptiveGuardrail {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfidenceFloor => "confidence_floor",
            Self::Allowlist => "allowlist",
            Self::MaxChangePercent => "max_change_percent",
            Self::UnboundedChange => "unbounded_change",
        }
    }
}

impl fmt::Display for AdaptiveGuardrail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum TunableKnob {
    PoolSize,
    BackpressureThresholds,
    MirrorSampling,
    Timeout,
}

impl TunableKnob {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PoolSize => "pool_size",
            Self::BackpressureThresholds => "backpressure_thresholds",
            Self::MirrorSampling => "mirror_sampling",
            Self::Timeout => "timeout",
        }
    }
}

impl fmt::Display for TunableKnob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TunableKnob {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pool_size" => Ok(Self::PoolSize),
            "backpressure_thresholds" => Ok(Self::BackpressureThresholds),
            "mirror_sampling" => Ok(Self::MirrorSampling),
            "timeout" => Ok(Self::Timeout),
            other => Err(format!("unknown tunable knob '{other}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TuningBound {
    Percent(u8),
    Unbounded,
}

impl TuningBound {
    #[must_use]
    pub const fn percent(max_change_percent: u8) -> Self {
        Self::Percent(max_change_percent)
    }

    #[must_use]
    pub const fn unbounded() -> Self {
        Self::Unbounded
    }

    #[must_use]
    pub const fn is_bounded(self) -> bool {
        matches!(self, Self::Percent(_))
    }

    #[must_use]
    pub const fn max_change_percent(self) -> Option<u8> {
        match self {
            Self::Percent(value) => Some(value),
            Self::Unbounded => None,
        }
    }
}

impl fmt::Display for TuningBound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Percent(value) => write!(formatter, "percent_{value}"),
            Self::Unbounded => formatter.write_str("unbounded"),
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum AdaptiveRecommendationError {
    #[error("adaptive recommendation confidence must be finite")]
    NonFiniteConfidence,
    #[error("adaptive recommendation confidence must be between 0.0 and 1.0")]
    ConfidenceOutOfRange,
    #[error("adaptive recommendation reason cannot be empty")]
    EmptyReason,
    #[error("adaptive recommendation window must be greater than zero")]
    EmptyWindow,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveRecommendation {
    signal: AdaptiveSignal,
    action: AdaptiveAction,
    knob: TunableKnob,
    confidence: f64,
    reason: Arc<str>,
    window_ms: u64,
    safety_bound: TuningBound,
}

impl AdaptiveRecommendation {
    pub fn new(
        signal: AdaptiveSignal,
        action: AdaptiveAction,
        knob: TunableKnob,
        confidence: f64,
        reason: impl Into<Arc<str>>,
        window_ms: u64,
        safety_bound: TuningBound,
    ) -> Result<Self, AdaptiveRecommendationError> {
        if !confidence.is_finite() {
            return Err(AdaptiveRecommendationError::NonFiniteConfidence);
        }
        if !(0.0..=1.0).contains(&confidence) {
            return Err(AdaptiveRecommendationError::ConfidenceOutOfRange);
        }
        if window_ms == 0 {
            return Err(AdaptiveRecommendationError::EmptyWindow);
        }

        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(AdaptiveRecommendationError::EmptyReason);
        }

        Ok(Self {
            signal,
            action,
            knob,
            confidence,
            reason,
            window_ms,
            safety_bound,
        })
    }

    #[must_use]
    pub const fn signal(&self) -> AdaptiveSignal {
        self.signal
    }

    #[must_use]
    pub const fn action(&self) -> AdaptiveAction {
        self.action
    }

    #[must_use]
    pub const fn knob(&self) -> TunableKnob {
        self.knob
    }

    #[must_use]
    pub const fn confidence(&self) -> f64 {
        self.confidence
    }

    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    #[must_use]
    pub const fn window_ms(&self) -> u64 {
        self.window_ms
    }

    #[must_use]
    pub const fn safety_bound(&self) -> TuningBound {
        self.safety_bound
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{message}")]
pub struct AdaptiveApplyError {
    guardrail: AdaptiveGuardrail,
    message: String,
}

impl AdaptiveApplyError {
    #[must_use]
    pub fn new(guardrail: AdaptiveGuardrail, message: impl Into<String>) -> Self {
        Self {
            guardrail,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn guardrail(&self) -> AdaptiveGuardrail {
        self.guardrail
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}
