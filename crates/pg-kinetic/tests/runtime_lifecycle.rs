use pg_kinetic_core::runtime::{
    LifecycleTransition, LifecycleTransitionError, NodeId, ReadinessState, RuntimeEngine,
    RuntimeEngineStatus, RuntimeLifecycleState, ShutdownReason,
};
use pg_kinetic_proxy::{
    drain::{DrainController, DrainOutcome},
    lifecycle::{LifecycleController, ShutdownCoordinator},
};
use std::{sync::Arc, time::Duration};

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

fn lifecycle(drain_grace: Duration, shutdown_grace: Duration) -> LifecycleController {
    LifecycleController::new(
        Arc::new(DrainController::new()),
        drain_grace,
        shutdown_grace,
        true,
    )
}

#[test]
fn startup_waits_for_listeners_and_backend_pools() {
    let lifecycle = lifecycle(Duration::from_secs(1), Duration::from_secs(1));
    lifecycle.mark_listeners_initialized();
    assert_eq!(
        lifecycle.state().lifecycle(),
        RuntimeLifecycleState::Starting
    );

    lifecycle.mark_backend_pools_initialized();
    let state = lifecycle.state();
    assert_eq!(state.lifecycle(), RuntimeLifecycleState::Ready);
    assert_eq!(state.readiness(), ReadinessState::Ready);
}

#[test]
fn readiness_is_not_ready_during_startup() {
    let lifecycle = lifecycle(Duration::from_secs(1), Duration::from_secs(1));
    assert_eq!(lifecycle.state().readiness(), ReadinessState::NotReady);
}

#[test]
fn drain_marks_not_ready_and_rejects_new_sessions() {
    let lifecycle = lifecycle(Duration::from_secs(1), Duration::from_secs(1));
    lifecycle.mark_backend_pools_initialized();
    lifecycle.mark_listeners_initialized();

    assert!(lifecycle.begin_drain(ShutdownReason::AdminRequest));
    let state = lifecycle.state();
    assert_eq!(state.lifecycle(), RuntimeLifecycleState::Draining);
    assert_eq!(state.readiness(), ReadinessState::NotReady);
    assert!(lifecycle.drain_token().try_enter().is_none());
}

#[tokio::test]
async fn existing_sessions_finish_within_drain_grace() {
    let lifecycle = lifecycle(Duration::from_secs(1), Duration::from_millis(10));
    lifecycle.mark_backend_pools_initialized();
    lifecycle.mark_listeners_initialized();
    let session = lifecycle
        .drain_token()
        .try_enter()
        .expect("session accepted before drain");
    assert!(lifecycle.begin_drain(ShutdownReason::Signal));

    let coordinator = ShutdownCoordinator::new(lifecycle.clone());
    let shutdown = tokio::spawn(async move { coordinator.coordinate().await });
    tokio::task::yield_now().await;
    drop(session);

    let outcome = shutdown.await.expect("shutdown task");
    assert_eq!(outcome.drain_outcome(), DrainOutcome::Completed);
    assert_eq!(outcome.forced_sessions(), 0);
}

#[tokio::test]
async fn shutdown_requests_force_close_after_shutdown_grace() {
    let lifecycle = lifecycle(Duration::from_millis(1), Duration::from_millis(1));
    lifecycle.mark_backend_pools_initialized();
    lifecycle.mark_listeners_initialized();
    let session = lifecycle
        .drain_token()
        .try_enter()
        .expect("session accepted before drain");
    assert!(lifecycle.begin_drain(ShutdownReason::Signal));

    let coordinator = ShutdownCoordinator::new(lifecycle.clone());
    let outcome = coordinator.coordinate().await;
    assert_eq!(outcome.drain_outcome(), DrainOutcome::TimedOut);
    assert_eq!(outcome.forced_sessions(), 1);
    assert!(lifecycle.state().force_close_requested());
    drop(session);
}

#[test]
fn repeated_drain_requests_are_idempotent() {
    let lifecycle = lifecycle(Duration::from_secs(1), Duration::from_secs(1));
    lifecycle.mark_backend_pools_initialized();
    lifecycle.mark_listeners_initialized();

    assert!(lifecycle.begin_drain(ShutdownReason::AdminRequest));
    assert!(!lifecycle.begin_drain(ShutdownReason::AdminRequest));
    assert_eq!(lifecycle.state().transition_count(), 2);
}

#[test]
fn lifecycle_updates_observable_snapshots_and_metric_states() {
    let lifecycle = lifecycle(Duration::from_secs(1), Duration::from_secs(1));
    let mut updates = lifecycle.subscribe();
    lifecycle.mark_backend_pools_initialized();
    lifecycle.mark_listeners_initialized();

    assert!(updates.has_changed().expect("lifecycle update"));
    let snapshot = updates.borrow_and_update().clone();
    assert_eq!(snapshot.lifecycle(), RuntimeLifecycleState::Ready);
    assert_eq!(snapshot.readiness(), ReadinessState::Ready);
    assert!(snapshot.listeners_initialized());
    assert!(snapshot.backend_pools_initialized());
    assert_eq!(snapshot.transition_count(), 1);
}
