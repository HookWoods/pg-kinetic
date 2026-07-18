use pg_kinetic::core::performance::{
    DerivedPerformanceMetric, PerformanceMetric, ProcessMetricKind, ProcessMetricSample,
    ProcessMetricValue,
};
use pg_kinetic_proxy::benchmark::collect_process_metrics;
use serde_json::Value;

#[test]
fn collection_exposes_process_metrics_and_timestamp() {
    let collection = collect_process_metrics();
    assert!(collection.sample().sampled_at_ms() > 0);
    assert!(!collection.sample().metrics().is_empty());
    for kind in [
        ProcessMetricKind::CpuTime,
        ProcessMetricKind::ResidentMemory,
        ProcessMetricKind::OpenFileDescriptors,
    ] {
        assert!(
            collection
                .sample()
                .metrics()
                .iter()
                .any(|(candidate, _)| *candidate == kind),
            "required process metric key {kind} is missing"
        );
        assert!(
            collection.sample().metric(kind).is_unknown()
                || collection.sample().metric(kind).as_f64().is_some()
        );
    }
}

#[cfg(unix)]
#[test]
fn unix_collection_uses_metric_exposure_units() {
    let collection = collect_process_metrics();
    let sample = collection.sample();

    let cpu_time = sample.metric(ProcessMetricKind::CpuTime);
    if !cpu_time.is_unknown() {
        assert!(matches!(cpu_time, ProcessMetricValue::Float(_)));
    }

    let resident_memory = sample.metric(ProcessMetricKind::ResidentMemory);
    if let ProcessMetricValue::Integer(bytes) = resident_memory {
        assert_eq!(bytes % 1024, 0);
    }
}

#[test]
fn derived_metrics_use_process_deltas() {
    let before = ProcessMetricSample::new(
        1,
        [
            (ProcessMetricKind::CpuTime, ProcessMetricValue::Integer(10)),
            (
                ProcessMetricKind::ResidentMemory,
                ProcessMetricValue::Integer(100),
            ),
        ],
    );
    let after = ProcessMetricSample::new(
        2,
        [
            (ProcessMetricKind::CpuTime, ProcessMetricValue::Integer(30)),
            (
                ProcessMetricKind::ResidentMemory,
                ProcessMetricValue::Integer(180),
            ),
        ],
    );
    assert_eq!(
        DerivedPerformanceMetric::cpu_per_query(&before, &after, 4).value(),
        Some(5.0)
    );
    assert_eq!(
        DerivedPerformanceMetric::memory_per_client(&before, &after, 2).value(),
        Some(40.0)
    );
    assert_eq!(
        DerivedPerformanceMetric::cpu_per_query(&before, &after, 0).metric(),
        PerformanceMetric::CpuPerQuery
    );
}

#[test]
fn unknown_values_are_supported_without_panicking() {
    let before = ProcessMetricSample::new(
        1,
        [(ProcessMetricKind::CpuTime, ProcessMetricValue::Unknown)],
    );
    let after = ProcessMetricSample::new(
        2,
        [(ProcessMetricKind::CpuTime, ProcessMetricValue::Integer(1))],
    );
    assert_eq!(
        DerivedPerformanceMetric::cpu_per_query(&before, &after, 1).value(),
        None
    );
    assert_eq!(ProcessMetricValue::Unknown.as_f64(), None);
}

#[test]
fn process_samples_render_redacted_json() {
    let sample = ProcessMetricSample::new(
        42,
        [(
            ProcessMetricKind::ResidentMemory,
            ProcessMetricValue::Integer(12),
        )],
    );
    let json = sample.to_json();
    let parsed: Value =
        serde_json::from_str(&json).expect("serialized sample should be valid JSON");
    assert_eq!(parsed["sampled_at_ms"], 42);
    assert_eq!(parsed["metrics"]["resident_memory"], 12);
    assert!(parsed["process_id"].is_null());
    assert!(parsed["command_line"].is_null());
}
