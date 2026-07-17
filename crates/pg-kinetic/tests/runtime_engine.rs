use pg_kinetic_core::runtime::{RuntimeEngine, RuntimeEngineStatus};
use pg_kinetic_proxy::runtime_engine::{
    RuntimeEngineCapabilities, RuntimeEngineExperiment, RuntimeEngineSelectionError,
    RuntimeEngineSelector,
};

#[test]
fn default_runtime_engine_is_stable_and_supported_on_all_platforms() {
    let selector = RuntimeEngineSelector::default();
    let capabilities = selector.capabilities();

    assert_eq!(selector.engine(), RuntimeEngine::TokioDefault);
    assert_eq!(capabilities.engine(), RuntimeEngine::TokioDefault);
    assert!(capabilities.is_stable());
    assert_eq!(capabilities.status(), RuntimeEngineStatus::Stable);
    assert!(capabilities.platform_supported());
    assert!(capabilities.is_available());
    assert_eq!(capabilities.benchmark_label(), "tokio_default");
    assert!(selector.validate().is_ok());
}

#[test]
fn experimental_runtime_engines_are_disabled_by_default() {
    for engine in [
        RuntimeEngine::ExperimentalThreadPerCore,
        RuntimeEngine::ExperimentalIoUring,
    ] {
        let selector = RuntimeEngineSelector::new(engine);
        let capabilities = selector.capabilities();

        assert!(capabilities.is_experimental());
        assert!(!RuntimeEngineExperiment::default().is_enabled());
        assert!(!capabilities.is_available());
        if capabilities.platform_supported() {
            assert_eq!(capabilities.status(), engine.status());
            assert!(matches!(
                selector.validate(),
                Err(RuntimeEngineSelectionError::ExperimentalDisabled { engine: selected })
                    if selected == engine
            ));
        } else {
            assert_eq!(capabilities.status(), RuntimeEngineStatus::Unsupported);
            assert!(matches!(
                selector.validate(),
                Err(RuntimeEngineSelectionError::UnsupportedPlatform { engine: selected, .. })
                    if selected == engine
            ));
        }
    }
}

#[test]
fn linux_only_runtime_engine_is_rejected_on_unsupported_platforms() {
    let selector = RuntimeEngineSelector::new(RuntimeEngine::ExperimentalIoUring);
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
                if engine == RuntimeEngine::ExperimentalIoUring
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
fn experimental_runtime_engine_cannot_be_enabled_without_feature_or_config_gate() {
    let disabled = RuntimeEngineExperiment::new(false);
    assert!(!disabled.is_enabled());

    let ungated_selector = RuntimeEngineSelector::new(RuntimeEngine::ExperimentalThreadPerCore)
        .with_experiment(disabled);
    assert!(matches!(
        ungated_selector.validate(),
        Err(RuntimeEngineSelectionError::ExperimentalDisabled { engine })
            if engine == RuntimeEngine::ExperimentalThreadPerCore
    ));
    assert!(!ungated_selector.selection_snapshot().available);

    let gated = RuntimeEngineExperiment::new(true);
    let gated_selector =
        RuntimeEngineSelector::new(RuntimeEngine::ExperimentalThreadPerCore).with_experiment(gated);

    if gated.feature_enabled() {
        assert!(gated.feature_enabled());
        assert!(gated.is_enabled());
        assert!(gated_selector.validate().is_ok());
        assert!(gated_selector.selection_snapshot().available);
    } else {
        assert!(!gated.feature_enabled());
        assert!(!gated.is_enabled());
        assert!(matches!(
            gated_selector.validate(),
            Err(RuntimeEngineSelectionError::ExperimentalDisabled { engine })
                if engine == RuntimeEngine::ExperimentalThreadPerCore
        ));
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
