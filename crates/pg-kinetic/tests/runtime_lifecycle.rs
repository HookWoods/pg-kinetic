use pg_kinetic_core::runtime::{
    LifecycleTransition, LifecycleTransitionError, NodeId, ReadinessState, RuntimeEngine,
    RuntimeEngineStatus, RuntimeLifecycleState, ShutdownReason,
};

#[test]
fn runtime_lifecycle_state_labels_are_stable() {
    assert_eq!(RuntimeLifecycleState::Starting.as_str(), "starting");
    assert_eq!(RuntimeLifecycleState::Ready.as_str(), "ready");
    assert_eq!(RuntimeLifecycleState::Draining.as_str(), "draining");
    assert_eq!(RuntimeLifecycleState::Stopping.as_str(), "stopping");
    assert_eq!(RuntimeLifecycleState::Stopped.as_str(), "stopped");
}

#[test]
fn readiness_state_labels_are_stable() {
    assert_eq!(ReadinessState::Ready.as_str(), "ready");
    assert_eq!(ReadinessState::NotReady.as_str(), "not_ready");
    assert_eq!(ReadinessState::Draining.as_str(), "draining");
}

#[test]
fn shutdown_reason_labels_are_stable() {
    assert_eq!(ShutdownReason::Signal.as_str(), "signal");
    assert_eq!(ShutdownReason::AdminRequest.as_str(), "admin_request");
    assert_eq!(ShutdownReason::PreStopHook.as_str(), "pre_stop_hook");
    assert_eq!(ShutdownReason::StartupFailure.as_str(), "startup_failure");
    assert_eq!(ShutdownReason::RuntimeFailure.as_str(), "runtime_failure");
}

#[test]
fn node_id_rejects_empty_ids() {
    assert!(NodeId::new("").is_err());
}

#[test]
fn tokio_default_is_the_default_runtime_engine() {
    assert_eq!(RuntimeEngine::default(), RuntimeEngine::TokioDefault);
    assert_eq!(RuntimeEngine::TokioDefault.as_str(), "tokio_default");
    assert_eq!(
        RuntimeEngine::TokioDefault.status(),
        RuntimeEngineStatus::Stable
    );
}

#[test]
fn experimental_runtime_engines_are_explicitly_labeled() {
    for engine in [
        RuntimeEngine::ExperimentalThreadPerCore,
        RuntimeEngine::ExperimentalIoUring,
    ] {
        assert!(engine.is_experimental());
        assert_eq!(engine.status(), RuntimeEngineStatus::Experimental);
        assert!(engine.as_str().starts_with("experimental_"));
    }

    assert!(!RuntimeEngine::TokioCurrentThread.is_experimental());
}

#[test]
fn lifecycle_transitions_reject_invalid_state_jumps() {
    assert_eq!(
        LifecycleTransition::new(
            RuntimeLifecycleState::Starting,
            RuntimeLifecycleState::Draining,
        ),
        Err(LifecycleTransitionError::InvalidTransition {
            from: RuntimeLifecycleState::Starting,
            to: RuntimeLifecycleState::Draining,
        })
    );
    assert_eq!(
        LifecycleTransition::new(RuntimeLifecycleState::Ready, RuntimeLifecycleState::Stopped,),
        Err(LifecycleTransitionError::InvalidTransition {
            from: RuntimeLifecycleState::Ready,
            to: RuntimeLifecycleState::Stopped,
        })
    );

    assert!(LifecycleTransition::new(
        RuntimeLifecycleState::Starting,
        RuntimeLifecycleState::Ready,
    )
    .is_ok());
    assert!(LifecycleTransition::new(
        RuntimeLifecycleState::Ready,
        RuntimeLifecycleState::Draining,
    )
    .is_ok());
    assert!(LifecycleTransition::new(
        RuntimeLifecycleState::Draining,
        RuntimeLifecycleState::Stopping,
    )
    .is_ok());
    assert!(LifecycleTransition::new(
        RuntimeLifecycleState::Stopping,
        RuntimeLifecycleState::Stopped,
    )
    .is_ok());
}
