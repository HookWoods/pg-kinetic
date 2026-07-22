use std::{net::SocketAddr, sync::Arc, time::Duration};

use pg_kinetic::{
    config::{Config, PoolLifecycleConfig},
    pool::{
        BackendPool, BackendPoolRef, CheckoutMode, ReplicaSelectionStrategy, ReplicaSelector,
        RoutePoolRegistry, RoutePools,
    },
    route::{QueryClass, RouteKey},
};
use tokio::net::TcpListener;

async fn backend_listener() -> (SocketAddr, Arc<tokio::sync::Notify>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let address = listener.local_addr().expect("backend address");
    let accepted = Arc::new(tokio::sync::Notify::new());
    let accepted_clone = Arc::clone(&accepted);
    tokio::spawn(async move {
        while let Ok((_stream, _)) = listener.accept().await {
            accepted_clone.notify_one();
        }
    });
    (address, accepted)
}

fn pool(address: SocketAddr) -> BackendPoolRef {
    BackendPoolRef::primary(BackendPool::new(
        address,
        Default::default(),
        1,
        2,
        2,
        2,
        Duration::from_secs(1),
        "DISCARD ALL",
    ))
}

fn limited_pool(
    address: SocketAddr,
    global_backend_slots: Arc<tokio::sync::Semaphore>,
    global_backend_available: Arc<tokio::sync::Notify>,
) -> Arc<BackendPool> {
    let mut lifecycle = PoolLifecycleConfig::default();
    lifecycle.max_size = 1;
    BackendPool::new_with_socket_lifecycle_and_global_limit_and_notify(
        address,
        Default::default(),
        Default::default(),
        2,
        2,
        2,
        Duration::from_secs(1),
        "DISCARD ALL",
        lifecycle,
        Some(global_backend_slots),
        Some(global_backend_available),
    )
}

#[tokio::test]
async fn database_user_selects_pool_while_application_name_is_ignored() {
    let (database_a_address, database_a_accepted) = backend_listener().await;
    let (database_b_address, database_b_accepted) = backend_listener().await;
    let registry = RoutePoolRegistry::new();
    let route_a = RouteKey::new("database_a", "user_a", None, None, QueryClass::Default);
    let route_b = RouteKey::new("database_b", "user_b", None, None, QueryClass::Default);
    registry.insert(
        route_a.clone(),
        RoutePools::new(
            pool(database_a_address),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );
    registry.insert(
        route_b.clone(),
        RoutePools::new(
            pool(database_b_address),
            Vec::new(),
            ReplicaSelector::new(ReplicaSelectionStrategy::LeastWaiting),
        ),
    );

    let application_route = RouteKey::new(
        "database_a",
        "user_a",
        Some("different_application"),
        None,
        QueryClass::Default,
    );
    let backend = registry
        .checkout_primary(&application_route, CheckoutMode::AllowConnect)
        .await
        .expect("configured database/user selects pool");
    backend.release().await;
    database_a_accepted.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), database_b_accepted.notified())
            .await
            .is_err()
    );

    let backend = registry
        .checkout_primary(&route_b, CheckoutMode::AllowConnect)
        .await
        .expect("second configured database/user selects pool");
    backend.release().await;
    database_b_accepted.notified().await;
}

#[tokio::test]
async fn global_capacity_release_wakes_a_different_pool() {
    let (held_address, held_accepted) = backend_listener().await;
    let (waiting_address, waiting_accepted) = backend_listener().await;
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let available = Arc::new(tokio::sync::Notify::new());
    let held_pool = limited_pool(held_address, Arc::clone(&slots), Arc::clone(&available));
    let waiting_pool = limited_pool(waiting_address, Arc::clone(&slots), Arc::clone(&available));
    let held_route = RouteKey::new("held", "user", None, None, QueryClass::Default);
    let waiting_route = RouteKey::new("waiting", "user", None, None, QueryClass::Default);

    let held_backend = held_pool
        .checkout_primary(held_route)
        .await
        .expect("first pool checkout");
    held_accepted.notified().await;

    let waiting = tokio::spawn(async move {
        waiting_pool
            .checkout_primary(waiting_route)
            .await
            .expect("second pool checkout after release")
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!waiting.is_finished());

    held_backend.discard();
    let waiting_backend = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("global release wakes waiter")
        .expect("waiting checkout task succeeds");
    waiting_backend.discard();
    waiting_accepted.notified().await;
}

#[test]
fn unmatched_database_user_has_no_pool_and_duplicate_config_is_rejected() {
    let registry = RoutePoolRegistry::new();
    let configured = RouteKey::new("database_a", "user_a", None, None, QueryClass::Default);
    assert!(registry.route_pools(&configured).is_none());

    let error = toml::from_str::<Config>(
        r#"
        [connection]
        listen_addr = "127.0.0.1:6543"
        backend_addr = "127.0.0.1:5432"

        [[pools]]
        database = "database_a"
        user = "user_a"
        backend_addr = "127.0.0.1:5432"

        [[pools]]
        database = "database_a"
        user = "user_a"
        backend_addr = "127.0.0.1:5433"
        "#,
    )
    .expect_err("duplicate database/user pool must be rejected");
    assert!(error.to_string().contains("duplicate pool"));
}
