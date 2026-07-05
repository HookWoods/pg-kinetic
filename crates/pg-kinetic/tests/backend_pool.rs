use pg_kinetic::pool::{BackendPool, PoolError};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;

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

#[tokio::test]
async fn marks_only_new_backend_connections_for_startup() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = listener.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let _ = listener.accept().await.expect("accept backend");
        }
    });

    let pool = BackendPool::new(
        backend_addr,
        1,
        1,
        Duration::from_millis(100),
        "DISCARD ALL",
    );

    let backend = pool.checkout().await.expect("fresh checkout");
    assert!(backend.requires_startup());
    backend.release().await;

    let backend = pool.checkout().await.expect("reused checkout");
    assert!(!backend.requires_startup());
}
