use pg_kinetic::core::performance::{
    DerivedPerformanceMetric, PerformanceMetric, ProcessMetricKind, ProcessMetricSample,
    ProcessMetricValue,
};
use pg_kinetic_proxy::benchmark::collect_process_metrics;

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
            collection.sample().metric(kind).is_unknown()
                || collection.sample().metric(kind).as_f64().is_some()
        );
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
    assert!(json.contains("sampled_at_ms"));
    assert!(json.contains("resident_memory"));
    assert!(json.contains("\"process_id\":null"));
    assert!(json.contains("\"command_line\":null"));
}
