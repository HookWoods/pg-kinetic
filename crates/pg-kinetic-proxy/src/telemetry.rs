use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use pg_kinetic_core::observability::{MetricOutcome, ProtocolPhase};
use pg_kinetic_core::{
    recovery::{RecoveryAction, RecoveryTrigger},
    route::{QueryClass, RouteKey},
    security::AuthMode,
    virtual_session::PinReason,
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
        .with_resource(
            Resource::builder()
                .with_service_name(config.otel_service_name.clone())
                .build(),
        );

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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DebugSampler {
    sampling_ratio: f64,
    sampling_threshold: u64,
}

impl DebugSampler {
    #[must_use]
    pub fn new(sampling_ratio: f64) -> Self {
        let sampling_ratio = if sampling_ratio.is_finite() {
            sampling_ratio.clamp(0.0, 1.0)
        } else {
            0.0
        };

        let sampling_threshold = (sampling_ratio * (u64::MAX as f64)) as u64;

        Self {
            sampling_ratio,
            sampling_threshold,
        }
    }

    #[must_use]
    pub const fn sampling_ratio(self) -> f64 {
        self.sampling_ratio
    }

    #[must_use]
    pub fn should_sample(self, session_id: u64) -> bool {
        if self.sampling_ratio <= 0.0 {
            return false;
        }
        if self.sampling_ratio >= 1.0 {
            return true;
        }

        splitmix64(session_id) < self.sampling_threshold
    }

    #[must_use]
    pub fn sample(self, session_id: u64, sample: DebugSample) -> Option<DebugSample> {
        self.should_sample(session_id).then_some(sample)
    }

    #[must_use]
    pub fn sample_with(
        self,
        session_id: u64,
        build_sample: impl FnOnce() -> DebugSample,
    ) -> Option<DebugSample> {
        self.should_sample(session_id).then(build_sample)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DebugEvent {
    ClientAccepted,
    StartupComplete,
    BackendCheckout,
    QueryComplete,
    Pinning,
    Recovery,
    OverloadRejected,
    ClientClose,
}

impl DebugEvent {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientAccepted => "client_accepted",
            Self::StartupComplete => "startup_complete",
            Self::BackendCheckout => "backend_checkout",
            Self::QueryComplete => "query_complete",
            Self::Pinning => "pinning",
            Self::Recovery => "recovery",
            Self::OverloadRejected => "overload_rejected",
            Self::ClientClose => "client_close",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DebugSample {
    pub event: DebugEvent,
    pub session_id: u64,
    pub route_key: Option<RouteKey>,
    pub phase: Option<ProtocolPhase>,
    pub outcome: Option<MetricOutcome>,
    pub pin_reason: Option<PinReason>,
    pub recovery_action: Option<RecoveryAction>,
    pub details: BTreeMap<String, String>,
}

impl DebugSample {
    #[must_use]
    pub fn client_accepted(
        session_id: u64,
        client_addr: SocketAddr,
        client_tls_mode: &'static str,
        has_peer_certificates: bool,
    ) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("client_addr"), client_addr.to_string());
        details.insert(String::from("client_tls_mode"), client_tls_mode.to_owned());
        details.insert(
            String::from("has_peer_certificates"),
            has_peer_certificates.to_string(),
        );

        Self {
            event: DebugEvent::ClientAccepted,
            session_id,
            route_key: None,
            phase: Some(ProtocolPhase::Startup),
            outcome: Some(MetricOutcome::Ok),
            pin_reason: None,
            recovery_action: None,
            details,
        }
    }

    #[must_use]
    pub fn startup_complete(
        session_id: u64,
        route_key: RouteKey,
        auth_mode: &'static str,
        client_tls_mode: &'static str,
        outcome: MetricOutcome,
    ) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("auth_mode"), auth_mode.to_owned());
        details.insert(String::from("client_tls_mode"), client_tls_mode.to_owned());

        Self {
            event: DebugEvent::StartupComplete,
            session_id,
            route_key: Some(route_key),
            phase: Some(ProtocolPhase::Startup),
            outcome: Some(outcome),
            pin_reason: None,
            recovery_action: None,
            details,
        }
    }

    #[must_use]
    pub fn backend_checkout(
        session_id: u64,
        route_key: RouteKey,
        checkout_mode: &'static str,
        outcome: MetricOutcome,
        wait: Duration,
    ) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("checkout_mode"), checkout_mode.to_owned());
        details.insert(String::from("wait_ms"), wait.as_millis().to_string());

        Self {
            event: DebugEvent::BackendCheckout,
            session_id,
            route_key: Some(route_key),
            phase: Some(ProtocolPhase::BackendCheckout),
            outcome: Some(outcome),
            pin_reason: None,
            recovery_action: None,
            details,
        }
    }

    #[must_use]
    pub fn query_complete(
        session_id: u64,
        route_key: RouteKey,
        outcome: MetricOutcome,
        rows: usize,
        ready_status: &'static str,
        sql: Option<&str>,
        bind_values: &[&str],
    ) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("rows"), rows.to_string());
        details.insert(String::from("ready_status"), ready_status.to_owned());
        if let Some(sql) = sql {
            details.insert(String::from("sql"), redact_debug_value(sql));
        }
        if !bind_values.is_empty() {
            details.insert(
                String::from("bind_value_count"),
                bind_values.len().to_string(),
            );
            details.insert(
                String::from("bind_values"),
                redact_debug_value("bind_values"),
            );
        }

        Self {
            event: DebugEvent::QueryComplete,
            session_id,
            route_key: Some(route_key),
            phase: Some(ProtocolPhase::Rows),
            outcome: Some(outcome),
            pin_reason: None,
            recovery_action: None,
            details,
        }
    }

    #[must_use]
    pub fn pinning(
        session_id: u64,
        route_key: RouteKey,
        pin_reason: PinReason,
        backend_id: u64,
        duration: Duration,
    ) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("backend_id"), backend_id.to_string());
        details.insert(
            String::from("duration_ms"),
            duration.as_millis().to_string(),
        );

        Self {
            event: DebugEvent::Pinning,
            session_id,
            route_key: Some(route_key),
            phase: Some(ProtocolPhase::Close),
            outcome: Some(MetricOutcome::Ok),
            pin_reason: Some(pin_reason),
            recovery_action: None,
            details,
        }
    }

    #[must_use]
    pub fn recovery(
        session_id: u64,
        route_key: RouteKey,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        outcome: MetricOutcome,
    ) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("trigger"), trigger.metric_label().to_owned());
        details.insert(String::from("action"), action.metric_label().to_owned());

        Self {
            event: DebugEvent::Recovery,
            session_id,
            route_key: Some(route_key),
            phase: Some(ProtocolPhase::Cancel),
            outcome: Some(outcome),
            pin_reason: None,
            recovery_action: Some(action),
            details,
        }
    }

    #[must_use]
    pub fn overload_rejected(session_id: u64, route_key: RouteKey, reason: &'static str) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("reason"), reason.to_owned());

        Self {
            event: DebugEvent::OverloadRejected,
            session_id,
            route_key: Some(route_key),
            phase: Some(ProtocolPhase::BackendCheckout),
            outcome: Some(MetricOutcome::Rejected),
            pin_reason: None,
            recovery_action: None,
            details,
        }
    }

    #[must_use]
    pub fn client_close(session_id: u64, client_addr: SocketAddr, duration: Duration) -> Self {
        let mut details = BTreeMap::new();
        details.insert(String::from("client_addr"), client_addr.to_string());
        details.insert(
            String::from("duration_ms"),
            duration.as_millis().to_string(),
        );

        Self {
            event: DebugEvent::ClientClose,
            session_id,
            route_key: None,
            phase: Some(ProtocolPhase::Close),
            outcome: Some(MetricOutcome::Canceled),
            pin_reason: None,
            recovery_action: None,
            details,
        }
    }
}

#[must_use]
pub fn redact_debug_value(value: impl AsRef<str>) -> String {
    if value.as_ref().is_empty() {
        String::new()
    } else {
        String::from("<redacted>")
    }
}

pub fn emit_debug_sample(sampler: &DebugSampler, sample: DebugSample) {
    let Some(sample) = sampler.sample(sample.session_id, sample) else {
        return;
    };

    tracing::debug!(
        target: "pg_kinetic",
        event = sample.event.as_str(),
        session_id = sample.session_id,
        route_key = ?sample.route_key,
        phase = ?sample.phase,
        outcome = ?sample.outcome,
        pin_reason = ?sample.pin_reason,
        recovery_action = ?sample.recovery_action,
        details = ?sample.details,
        "debug sample"
    );
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

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}
