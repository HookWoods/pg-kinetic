use std::net::SocketAddr;

use pg_kinetic::route::{QueryClass, RouteKey};

#[test]
fn route_key_uses_database_user_and_application_name() {
    let client_addr = "127.0.0.1:5000".parse::<SocketAddr>().ok();
    let key = RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        client_addr,
        QueryClass::Default,
    );

    assert_eq!(key.database(), "pgkinetic");
    assert_eq!(key.user(), "postgres");
    assert_eq!(key.application_name(), Some("api"));
    assert_eq!(key.client_addr(), client_addr);
    assert_eq!(key.query_class(), QueryClass::Default);
}

#[test]
fn missing_application_name_is_grouped() {
    let key = RouteKey::new("pgkinetic", "postgres", None, None, QueryClass::Default);

    assert_eq!(key.application_name(), None);
    assert_eq!(key.metric_label(), "pgkinetic/postgres/<none>/default");
}

#[test]
fn client_address_participates_in_route_identity_but_not_metrics() {
    let first = RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        "127.0.0.1:5000".parse::<SocketAddr>().ok(),
        QueryClass::Default,
    );
    let second = RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        "127.0.0.1:5001".parse::<SocketAddr>().ok(),
        QueryClass::Default,
    );

    assert_ne!(first, second);
    assert_eq!(first.metric_label(), second.metric_label());
}
