use pg_kinetic::pool::{BackendPool, PoolError};
use pg_kinetic::route::{QueryClass, RouteKey};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{io::AsyncReadExt, net::TcpListener, time};

fn route_key() -> RouteKey {
    RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        None,
        QueryClass::Default,
    )
}

#[tokio::test]
async fn reports_connection_failure_when_backend_unavailable() {
    let backend_addr: SocketAddr = "127.0.0.1:9".parse().expect("valid socket");
    let pool = BackendPool::new(
        backend_addr,
        1,
        1,
        1,
        1,
        Duration::from_secs(1),
        "DISCARD ALL",
    );

    let error = pool
        .checkout(route_key())
        .await
        .expect_err("checkout fails");

    assert!(matches!(
        error,
        PoolError::Connect(_) | PoolError::Backpressure(_)
    ));
}

#[test]
fn exposes_pool_limits() {
    let backend_addr: SocketAddr = "127.0.0.1:5432".parse().expect("valid socket");
    let pool = BackendPool::new(
        backend_addr,
        4,
        8,
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
        1,
        1,
        Duration::from_millis(100),
        "DISCARD ALL",
    );

    let backend = pool.checkout(route_key()).await.expect("fresh checkout");
    assert!(backend.requires_startup());
    backend.release().await;

    let backend = pool.checkout(route_key()).await.expect("reused checkout");
    assert!(!backend.requires_startup());
}

#[tokio::test]
async fn reusable_checkout_waits_for_started_backend_instead_of_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = listener.local_addr().expect("backend addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_probe = accepted.clone();

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept backend");
            accepted_probe.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let mut byte = [0_u8; 1];
                let _ = stream.read(&mut byte).await;
            });
        }
    });

    let pool = BackendPool::new(
        backend_addr,
        2,
        2,
        2,
        2,
        Duration::from_millis(500),
        "DISCARD ALL",
    );

    let backend = pool.checkout(route_key()).await.expect("fresh checkout");
    assert!(backend.requires_startup());
    time::sleep(Duration::from_millis(25)).await;
    assert_eq!(accepted.load(Ordering::Relaxed), 1);

    let waiting_pool = pool.clone();
    let waiting = tokio::spawn(async move { waiting_pool.checkout_reusable(route_key()).await });
    time::sleep(Duration::from_millis(25)).await;

    assert!(!waiting.is_finished());
    assert_eq!(accepted.load(Ordering::Relaxed), 1);

    backend.release().await;

    let backend = time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("reusable checkout notified")
        .expect("checkout task completed")
        .expect("reusable checkout succeeds");
    assert!(!backend.requires_startup());
    assert_eq!(accepted.load(Ordering::Relaxed), 1);
}
