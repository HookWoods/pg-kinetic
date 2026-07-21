use pg_kinetic::config::{PoolLifecycleConfig, TlsConfig};
use pg_kinetic::pool::{BackendPool, PoolError};
use pg_kinetic::route::{QueryClass, RouteKey};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

fn route_key() -> RouteKey {
    RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("lifecycle"),
        None,
        QueryClass::Default,
    )
}

async fn backend_listener() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let address = listener.local_addr().expect("backend address");
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept backend");
            tokio::spawn(async move {
                let mut buffer = [0_u8; 1];
                let _ = stream.read(&mut buffer).await;
            });
        }
    });
    address
}

fn lifecycle(max_size: usize, idle_timeout_ms: u64, min_idle: usize) -> PoolLifecycleConfig {
    PoolLifecycleConfig {
        max_size,
        min_idle,
        idle_timeout: Duration::from_millis(idle_timeout_ms),
        max_lifetime: Duration::ZERO,
    }
}

fn pool(
    address: std::net::SocketAddr,
    lifecycle: PoolLifecycleConfig,
) -> std::sync::Arc<BackendPool> {
    BackendPool::new_with_socket_and_lifecycle(
        address,
        TlsConfig::default(),
        Default::default(),
        0,
        1,
        1,
        Duration::from_millis(100),
        "DISCARD ALL",
        lifecycle,
    )
}

#[tokio::test]
async fn idle_connections_are_evicted_after_idle_timeout() {
    let address = backend_listener().await;
    let pool = pool(address, lifecycle(1, 10, 0));
    let backend = pool.checkout(route_key()).await.expect("checkout backend");
    backend.release().await;

    tokio::time::sleep(Duration::from_millis(25)).await;
    pool.reap_idle().await;

    assert_eq!(pool.idle_count(), 0);
    assert_eq!(pool.active_count(), 0);
}

#[tokio::test]
async fn pool_never_opens_more_than_max_size() {
    let address = backend_listener().await;
    let pool = pool(address, lifecycle(1, 0, 0));
    let first = pool.checkout(route_key()).await.expect("first checkout");

    let error = pool
        .checkout(route_key())
        .await
        .expect_err("second checkout is rejected");
    assert!(matches!(error, PoolError::Backpressure(_)));

    first.release().await;
}

#[tokio::test]
async fn minimum_idle_connections_are_preserved() {
    let address = backend_listener().await;
    let pool = pool(address, lifecycle(2, 1_000, 1));
    let first = pool.checkout(route_key()).await.expect("first checkout");
    first.release().await;

    tokio::time::sleep(Duration::from_millis(25)).await;
    pool.reap_idle().await;

    assert_eq!(pool.idle_count(), 1);
}
