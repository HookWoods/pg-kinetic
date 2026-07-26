use pg_kinetic_core::cluster::runtime::{RuntimeEngine, RuntimeEngineStatus};
use pg_kinetic_proxy::engine::runtime_engine::{
    RuntimeEngineCapabilities, RuntimeEngineExperiment, RuntimeEngineSelectionError,
    RuntimeEngineSelector,
};

#[test]
fn default_runtime_engine_is_stable_and_supported_on_all_platforms() {
    let selector = RuntimeEngineSelector::default();
    let capabilities = selector.capabilities();

    assert_eq!(selector.engine(), RuntimeEngine::ThreadPerCore);
    assert_eq!(capabilities.engine(), RuntimeEngine::ThreadPerCore);
    assert!(capabilities.is_stable());
    assert_eq!(capabilities.status(), RuntimeEngineStatus::Stable);
    assert!(capabilities.platform_supported());
    assert!(capabilities.is_available());
    assert_eq!(capabilities.benchmark_label(), "thread_per_core");
    assert!(selector.validate().is_ok());
}

#[test]
fn io_uring_is_stable_and_feature_gated() {
    let engine = RuntimeEngine::IoUring;
    let selector = RuntimeEngineSelector::new(engine);
    let capabilities = selector.capabilities();

    assert!(capabilities.is_stable());
    assert!(!capabilities.is_experimental());
    assert_eq!(capabilities.benchmark_label(), "io_uring");
    if capabilities.platform_supported() && capabilities.feature_supported() {
        assert_eq!(capabilities.status(), engine.status());
        assert!(capabilities.is_available());
        assert!(selector.validate().is_ok());
    } else if !capabilities.platform_supported() {
        assert!(!capabilities.is_available());
        assert_eq!(capabilities.status(), RuntimeEngineStatus::Unsupported);
        assert!(matches!(
            selector.validate(),
            Err(RuntimeEngineSelectionError::UnsupportedPlatform { engine: selected, .. })
                if selected == engine
        ));
    } else {
        assert!(!capabilities.is_available());
        assert!(matches!(
            selector.validate(),
            Err(RuntimeEngineSelectionError::MissingFeature { engine: selected })
                if selected == engine
        ));
    }
}

#[test]
fn stable_thread_per_core_does_not_need_experimental_config_gate() {
    let selector = RuntimeEngineSelector::new(RuntimeEngine::ThreadPerCore);
    let capabilities = selector.capabilities();

    assert!(capabilities.is_stable());
    assert!(!capabilities.is_experimental());
    assert_eq!(capabilities.benchmark_label(), "thread_per_core");
    assert!(selector.validate().is_ok());
    assert!(capabilities.is_available());
}

#[test]
fn linux_only_runtime_engine_is_rejected_on_unsupported_platforms() {
    let selector = RuntimeEngineSelector::new(RuntimeEngine::IoUring);
    let capabilities = selector.capabilities();

    if cfg!(target_os = "linux") {
        assert!(capabilities.platform_supported());
        assert_ne!(capabilities.status(), RuntimeEngineStatus::Unsupported);
    } else {
        assert!(!capabilities.platform_supported());
        assert_eq!(capabilities.status(), RuntimeEngineStatus::Unsupported);
        assert!(matches!(
            selector.validate(),
            Err(RuntimeEngineSelectionError::UnsupportedPlatform { engine, .. })
                if engine == RuntimeEngine::IoUring
        ));
    }
}

#[test]
fn runtime_engine_selection_is_visible_in_snapshots_and_metrics() {
    let selector = RuntimeEngineSelector::new(RuntimeEngine::TokioCurrentThread);
    let snapshot = selector.selection_snapshot();
    let metrics = selector.selection_metrics();

    assert_eq!(snapshot.runtime_engine, RuntimeEngine::TokioCurrentThread);
    assert_eq!(snapshot.status, RuntimeEngineStatus::Stable);
    assert!(snapshot.platform_supported);
    assert!(snapshot.available);
    assert_eq!(snapshot.benchmark_label, "tokio_current_thread");
    assert_eq!(snapshot.platform, std::env::consts::OS);

    assert_eq!(metrics.runtime_engine, "tokio_current_thread");
    assert_eq!(metrics.status, "stable");
    assert_eq!(metrics.availability, "available");
    assert_eq!(metrics.benchmark_label, "tokio_current_thread");
    assert_eq!(metrics.platform, std::env::consts::OS);
}

#[test]
fn io_uring_does_not_need_experimental_config_gate() {
    let disabled = RuntimeEngineExperiment::new(false);
    assert!(!disabled.is_enabled());

    let selector = RuntimeEngineSelector::new(RuntimeEngine::IoUring).with_experiment(disabled);
    if !selector.capabilities().platform_supported() {
        assert!(matches!(
            selector.validate(),
            Err(RuntimeEngineSelectionError::UnsupportedPlatform { engine, .. })
                if engine == RuntimeEngine::IoUring
        ));
    } else if !selector.capabilities().feature_supported() {
        assert!(matches!(
            selector.validate(),
            Err(RuntimeEngineSelectionError::MissingFeature { engine })
                if engine == RuntimeEngine::IoUring
        ));
    } else {
        assert!(selector.validate().is_ok());
        assert!(selector.selection_snapshot().available);
    }
}

#[test]
fn runtime_engine_benchmark_label_is_stable() {
    let selector = RuntimeEngineSelector::new(RuntimeEngine::TokioDefault);
    let capabilities = RuntimeEngineCapabilities::new(
        RuntimeEngine::TokioDefault,
        RuntimeEngineExperiment::default(),
    );

    assert_eq!(selector.benchmark_label(), "tokio_default");
    assert_eq!(capabilities.benchmark_label(), "tokio_default");
    assert_eq!(
        selector.selection_snapshot().benchmark_label,
        "tokio_default"
    );
    assert_eq!(
        selector.selection_metrics().benchmark_label,
        "tokio_default"
    );
}
