use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider},
};
use pg_kinetic_core::observability::{MetricOutcome, ProtocolPhase};
use pg_kinetic_core::{
    recovery::{RecoveryAction, RecoveryTrigger},
    route::{QueryClass, RouteKey},
    security::AuthMode,
};
pub trait PhaseTimingRecorder: Send + Sync {
    fn record_protocol_phase_duration(
        &self,
        phase: ProtocolPhase,
        outcome: MetricOutcome,
        duration: Duration,
    );
}

#[derive(Clone, Debug, Default)]
pub struct MetricsPhaseTimingRecorder;

impl PhaseTimingRecorder for MetricsPhaseTimingRecorder {
    fn record_protocol_phase_duration(
        &self,
        phase: ProtocolPhase,
        outcome: MetricOutcome,
        duration: Duration,
    ) {
        record_protocol_phase_duration(phase, outcome, duration);
    }
}

#[must_use]
pub fn shared_phase_timing_recorder() -> Arc<dyn PhaseTimingRecorder> {
    Arc::new(MetricsPhaseTimingRecorder)
}

pub fn record_protocol_phase_duration(
    phase: ProtocolPhase,
    outcome: MetricOutcome,
    duration: Duration,
) {
    crate::metrics::record_protocol_phase_duration(phase, outcome, duration);
}

pub const STARTUP_SPAN_NAME: &str = "pg_kinetic.startup";
pub const AUTH_SPAN_NAME: &str = "pg_kinetic.auth";
pub const CHECKOUT_SPAN_NAME: &str = "pg_kinetic.checkout";
pub const QUERY_SPAN_NAME: &str = "pg_kinetic.query";
pub const ROWS_SPAN_NAME: &str = "pg_kinetic.rows";
pub const RECOVERY_SPAN_NAME: &str = "pg_kinetic.recovery";
pub const CLOSE_SPAN_NAME: &str = "pg_kinetic.close";

pub fn build_otel_tracer_provider(
    config: &crate::config::ObservabilityConfig,
) -> anyhow::Result<SdkTracerProvider> {
    let sampler = if config.otel_enabled {
        Sampler::TraceIdRatioBased(config.trace_sampling_ratio())
    } else {
        Sampler::AlwaysOff
    };

    let mut provider = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_resource(Resource::builder().with_service_name(config.otel_service_name.clone()).build());

    if config.otel_enabled {
        let endpoint = config
            .otel_endpoint
            .as_deref()
            .context("OTEL endpoint must be configured when OTEL is enabled")?;
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(15))
            .build()
            .context("build OTLP span exporter")?;
        provider = provider.with_batch_exporter(exporter);
    }

    Ok(provider.build())
}

#[must_use]
pub fn startup_span() -> tracing::Span {
    tracing::info_span!(target: "pg_kinetic", STARTUP_SPAN_NAME)
}

#[must_use]
pub fn auth_span(auth_mode: AuthMode) -> tracing::Span {
    tracing::info_span!(target: "pg_kinetic", AUTH_SPAN_NAME, auth_mode = %auth_mode.as_str())
}

#[must_use]
pub fn checkout_span(route: &RouteKey) -> tracing::Span {
    tracing::info_span!(target: "pg_kinetic", CHECKOUT_SPAN_NAME, route = %route.metric_label())
}

#[must_use]
pub fn query_span(route: &RouteKey, query_class: QueryClass) -> tracing::Span {
    tracing::info_span!(
        target: "pg_kinetic",
        QUERY_SPAN_NAME,
        route = %route.metric_label(),
        query_class = %query_class
    )
}

#[must_use]
pub fn rows_span(row_count: usize) -> tracing::Span {
    tracing::info_span!(target: "pg_kinetic", ROWS_SPAN_NAME, row_count)
}

#[must_use]
pub fn recovery_span(trigger: RecoveryTrigger, action: RecoveryAction) -> tracing::Span {
    tracing::info_span!(
        target: "pg_kinetic",
        RECOVERY_SPAN_NAME,
        trigger = %trigger.metric_label(),
        action = %action.metric_label()
    )
}

#[must_use]
pub fn close_span() -> tracing::Span {
    tracing::info_span!(target: "pg_kinetic", CLOSE_SPAN_NAME)
}

pub struct PhaseTimer<'a> {
    phase: ProtocolPhase,
    started: Instant,
    recorder: &'a dyn PhaseTimingRecorder,
}

impl std::fmt::Debug for PhaseTimer<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PhaseTimer")
            .field("phase", &self.phase)
            .field("started", &self.started)
            .finish()
    }
}

impl<'a> PhaseTimer<'a> {
    #[must_use]
    pub fn start(phase: ProtocolPhase, recorder: &'a dyn PhaseTimingRecorder) -> Self {
        Self {
            phase,
            started: Instant::now(),
            recorder,
        }
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn finish(self, outcome: MetricOutcome) -> Duration {
        let duration = self.started.elapsed();
        self.recorder
            .record_protocol_phase_duration(self.phase, outcome, duration);
        duration
    }
}
