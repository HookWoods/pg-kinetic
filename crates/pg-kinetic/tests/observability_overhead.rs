use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

use pg_kinetic::{
    config::ObservabilityConfig,
    core::observability::{metric_catalog, LabelPolicy},
    proxy_runtime::telemetry::{build_otel_tracer_provider, DebugSample, DebugSampler},
};

#[test]
fn metric_catalog_and_label_validation_use_static_definitions() {
    let catalog = metric_catalog();
    assert_eq!(catalog.as_ptr(), metric_catalog().as_ptr());
    assert!(!catalog.is_empty());
    assert!(LabelPolicy::allows("stage"));

    for descriptor in catalog {
        for label in descriptor.labels {
            assert!(LabelPolicy::allows(label.as_str()));
        }
    }

    for label in ["query", "query_text", "sql", "tenant", "client_addr"] {
        assert!(
            !LabelPolicy::allows(label),
            "high-cardinality label {label}"
        );
    }
}

#[test]
fn disabled_otel_configuration_skips_endpoint_setup() {
    let disabled = ObservabilityConfig {
        otel_endpoint: Some(String::from("not-a-valid-otlp-endpoint")),
        ..ObservabilityConfig::default()
    };
    assert!(disabled.metrics_addr.is_none());
    assert!(!disabled.otel_enabled);
    assert_eq!(disabled.trace_sampling_ratio(), 0.0);
    assert!(build_otel_tracer_provider(&disabled).is_ok());

    let enabled_without_endpoint = ObservabilityConfig {
        otel_enabled: true,
        ..ObservabilityConfig::default()
    };
    assert!(build_otel_tracer_provider(&enabled_without_endpoint).is_err());

    let enabled_metrics = ObservabilityConfig {
        metrics_addr: Some(
            "127.0.0.1:9090"
                .parse::<SocketAddr>()
                .expect("metrics address"),
        ),
        ..disabled
    };
    assert!(enabled_metrics.metrics_addr.is_some());
}

#[test]
fn debug_sampling_is_deterministic_and_skips_unsampled_construction() {
    let disabled = DebugSampler::new(0.0);
    let constructed = AtomicUsize::new(0);
    let sample = disabled.sample_with(42, || {
        constructed.fetch_add(1, Ordering::Relaxed);
        DebugSample::client_close(
            42,
            "127.0.0.1:5432".parse().expect("client address"),
            Default::default(),
        )
    });
    assert!(sample.is_none());
    assert_eq!(constructed.load(Ordering::Relaxed), 0);

    let bounded = DebugSampler::new(0.25);
    let decisions = (0..128)
        .map(|session_id| bounded.should_sample(session_id))
        .collect::<Vec<_>>();
    assert_eq!(
        decisions,
        (0..128)
            .map(|session_id| bounded.should_sample(session_id))
            .collect::<Vec<_>>()
    );
    assert!(decisions.iter().any(|sampled| *sampled));
    assert!(decisions.iter().any(|sampled| !*sampled));

    let always = DebugSampler::new(1.0);
    assert!(always
        .sample_with(42, || DebugSample::client_close(
            42,
            "127.0.0.1:5432".parse().expect("client address"),
            Default::default(),
        ))
        .is_some());
}
