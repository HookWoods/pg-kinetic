use pg_kinetic::pool::{BackendPool, PoolError};
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::test]
async fn reports_connection_failure_when_backend_unavailable() {
    let backend_addr: SocketAddr = "127.0.0.1:9".parse().expect("valid socket");
    let pool = BackendPool::new(backend_addr, 1, 1, Duration::from_millis(10), "DISCARD ALL");

    let error = pool.checkout().await.expect_err("checkout fails");

    assert!(matches!(error, PoolError::Connect(_)));
}

#[test]
fn exposes_pool_limits() {
    let backend_addr: SocketAddr = "127.0.0.1:5432".parse().expect("valid socket");
    let pool = BackendPool::new(
        backend_addr,
        4,
        8,
        Duration::from_millis(100),
        "DISCARD ALL",
    );

    assert_eq!(pool.max_backends(), 4);
    assert_eq!(pool.max_waiters(), 8);
}
