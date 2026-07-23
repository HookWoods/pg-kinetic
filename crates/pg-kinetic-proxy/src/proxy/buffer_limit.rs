use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BufferBudgetKind {
    Client,
    Backend,
}

impl BufferBudgetKind {
    #[must_use]
    const fn metric_label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Backend => "backend",
        }
    }
}

impl std::fmt::Display for BufferBudgetKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.metric_label())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{kind} buffer limit exceeded")]
pub(super) struct BufferLimitExceeded {
    kind: BufferBudgetKind,
}

pub(super) fn buffer_limit_exceeded(kind: BufferBudgetKind) -> anyhow::Error {
    record_buffer_limit(kind);
    BufferLimitExceeded { kind }.into()
}

pub(super) fn buffer_limit_kind(error: &anyhow::Error) -> Option<BufferBudgetKind> {
    error
        .downcast_ref::<BufferLimitExceeded>()
        .map(|error| error.kind)
}

pub(super) fn record_buffer_limit(kind: BufferBudgetKind) {
    metrics_crate::counter!(
        MetricName::BufferLimitTotal.as_str(),
        "kind" => kind.metric_label()
    )
    .increment(1);
}
