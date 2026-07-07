use pg_kinetic_core::routing::{
    BackendRole, FallbackPolicy, FreshnessPolicy, QueryClass, ReadRoutingMode, RoutingDecision,
    RoutingHint, RoutingReason,
};

#[test]
fn backend_role_labels_are_stable() {
    assert_eq!(BackendRole::Primary.as_str(), "primary");
    assert_eq!(BackendRole::Replica.as_str(), "replica");
}

#[test]
fn read_routing_mode_defaults_to_off() {
    assert_eq!(ReadRoutingMode::default(), ReadRoutingMode::Off);
    assert_eq!(ReadRoutingMode::Off.as_str(), "off");
}

#[test]
fn write_queries_stay_on_primary() {
    assert!(!QueryClass::Write.routes_to_replica());
    assert_eq!(QueryClass::Write.target_role(), BackendRole::Primary);
}

#[test]
fn fallback_policy_labels_are_stable() {
    assert_eq!(FallbackPolicy::Primary.as_str(), "primary");
}

#[test]
fn freshness_policy_labels_are_stable() {
    assert_eq!(
        FreshnessPolicy::SessionWriteLsn.as_str(),
        "session_write_lsn"
    );
}

#[test]
fn routing_decision_carries_core_fields() {
    let decision = RoutingDecision::new(
        BackendRole::Replica,
        QueryClass::ReadCandidate,
        RoutingHint::StrictFresh,
        RoutingReason::FreshnessRequired,
        FallbackPolicy::Wait,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
    );

    assert_eq!(decision.target_role, BackendRole::Replica);
    assert_eq!(decision.query_class, QueryClass::ReadCandidate);
    assert_eq!(decision.hint, RoutingHint::StrictFresh);
    assert_eq!(decision.reason, RoutingReason::FreshnessRequired);
    assert_eq!(decision.fallback_policy, FallbackPolicy::Wait);
    assert_eq!(
        decision.freshness_requirement,
        FreshnessPolicy::SessionWriteLsnAndMaxLag
    );
}
