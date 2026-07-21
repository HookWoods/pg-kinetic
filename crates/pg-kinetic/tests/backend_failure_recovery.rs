use std::{net::SocketAddr, sync::Arc, time::Duration};

use pg_kinetic::{
    config::{SocketConfig, TlsConfig},
    proxy::{retry_disposition, BackendFailureKind, RetryDisposition},
    proxy_runtime::{drain::DrainController, health},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[test]
fn retries_a_read_when_backend_disconnects_before_any_response() {
    assert_eq!(
        retry_disposition(BackendFailureKind::Read, false, true),
        RetryDisposition::RetryBeforeResponse
    );
}

#[test]
fn never_retries_a_write_or_partially_forwarded_response() {
    assert_eq!(
        retry_disposition(BackendFailureKind::Write, false, true),
        RetryDisposition::Never
    );
    assert_eq!(
        retry_disposition(BackendFailureKind::Read, true, true),
        RetryDisposition::Never
    );
}

#[tokio::test]
async fn readiness_becomes_not_ready_when_primary_is_unreachable() {
    let health_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind health");
    let health_addr = health_listener.local_addr().expect("health address");
    drop(health_listener);

    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend_listener.local_addr().expect("backend address");
    drop(backend_listener);

    let drain = Arc::new(DrainController::new());
    let server = health::spawn(
        health_addr,
        drain.clone(),
        backend_addr,
        TlsConfig::default(),
        SocketConfig::default(),
        Duration::from_millis(50),
        Duration::from_millis(10),
    )
    .await
    .expect("start health server");

    let response = poll_ready(health_addr).await;
    assert!(response.starts_with("HTTP/1.1 503"));

    drain.begin_drain(Duration::from_millis(10));
    server.abort();
    let _ = server.await;
}

async fn poll_ready(addr: SocketAddr) -> String {
    for _ in 0..20 {
        if let Ok(response) = http_get(addr).await {
            if response.starts_with("HTTP/1.1 503") {
                return response;
            }
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    panic!("readiness did not become unavailable")
}

async fn http_get(addr: SocketAddr) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(addr).await?;
    stream
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(String::from_utf8_lossy(&response).into_owned())
}
