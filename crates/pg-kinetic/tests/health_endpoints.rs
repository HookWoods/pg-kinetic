use std::{net::SocketAddr, sync::Arc, time::Duration};

use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, CapacityConfig, Config,
        ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig, PerformanceConfig,
        QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    proxy::Proxy,
    proxy_runtime::drain::DrainController,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[tokio::test]
async fn healthz_returns_200_while_process_is_alive() {
    let backend_addr = healthy_backend().await;
    let (proxy_addr, health_addr, drain, run_handle) = spawn_proxy(Some(backend_addr)).await;

    let response = http_get(health_addr, "/healthz").await;
    assert_eq!(response.status, 200);
    assert_eq!(response.body, "live");

    drain.begin_drain(Duration::from_millis(100));
    run_handle.await.expect("proxy run");

    let _ = proxy_addr;
}

#[tokio::test]
async fn readyz_returns_200_when_accepting_and_backend_healthy() {
    let backend_addr = healthy_backend().await;
    let (proxy_addr, health_addr, drain, run_handle) = spawn_proxy(Some(backend_addr)).await;

    let response = poll_ready(health_addr, "/readyz", 200).await;
    assert_eq!(response.status, 200);
    assert_eq!(response.body, "ready");

    drain.begin_drain(Duration::from_millis(100));
    run_handle.await.expect("proxy run");

    let _ = proxy_addr;
}

#[tokio::test]
async fn readyz_returns_503_while_draining() {
    let backend_addr = healthy_backend().await;
    let (proxy_addr, health_addr, drain, run_handle) = spawn_proxy(Some(backend_addr)).await;

    let healthy = poll_ready(health_addr, "/readyz", 200).await;
    assert_eq!(healthy.status, 200);

    drain.begin_drain(Duration::from_millis(100));
    time::sleep(Duration::from_millis(50)).await;

    let response = http_get(health_addr, "/readyz").await;
    assert_eq!(response.status, 503);
    assert_eq!(response.body, "not_ready");

    run_handle.await.expect("proxy run");

    let _ = proxy_addr;
}

#[tokio::test]
async fn readyz_returns_503_when_backend_connectivity_is_failing() {
    let (_, health_addr, drain, run_handle) = spawn_proxy(None).await;

    let response = poll_ready(health_addr, "/readyz", 503).await;
    assert_eq!(response.status, 503);
    assert_eq!(response.body, "not_ready");

    drain.begin_drain(Duration::from_millis(100));
    run_handle.await.expect("proxy run");
}

#[tokio::test]
async fn state_endpoint_does_not_expose_secrets() {
    let backend_addr = healthy_backend().await;
    let (proxy_addr, health_addr, drain, run_handle) = spawn_proxy(Some(backend_addr)).await;

    let response = poll_ready(health_addr, "/state", 200).await;
    assert_eq!(response.status, 200);
    assert!(response.body.contains(r#""drain_state":"accepting""#));
    assert!(response.body.contains(r#""active_clients":0"#));
    assert!(response.body.contains(r#""backend_health":"ready""#));
    assert!(!response.body.contains("backend_password_env_var_name"));
    assert!(!response.body.contains("auth_users_file"));

    drain.begin_drain(Duration::from_millis(100));
    run_handle.await.expect("proxy run");

    let _ = proxy_addr;
}

async fn spawn_proxy(
    backend: Option<SocketAddr>,
) -> (
    SocketAddr,
    SocketAddr,
    Arc<DrainController>,
    tokio::task::JoinHandle<()>,
) {
    let backend_addr = match backend {
        Some(addr) => addr,
        None => "127.0.0.1:9".parse().expect("closed backend port"),
    };

    let health_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind health listener");
    let health_addr = health_listener.local_addr().expect("health addr");
    drop(health_listener);

    let proxy_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy listener");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    drop(proxy_listener);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr: proxy_addr,
            backend_addr,
        },
        capacity: CapacityConfig {
            max_clients: 4,
            max_backends: 1,
            max_checkout_waiters: 4,
        },
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            recovery_mode: pg_kinetic::recovery::RecoveryMode::Recover,
            recovery_timeout_ms: 1_000,
            backend_reset_query: String::from("DISCARD ALL"),
        },
        qos: QosConfig {
            max_route_in_flight: 100,
            max_route_waiters: 1_000,
            query_timeout_ms: 30_000,
            idle_client_timeout_ms: 300_000,
            idle_transaction_timeout_ms: 60_000,
            max_client_buffer_bytes: 1_048_576,
            max_backend_buffer_bytes: 4_194_304,
            overload_error_code: String::from("53300"),
        },
        observability: ObservabilityConfig { metrics_addr: None, ..Default::default() },
        tls: TlsConfig {
            client_tls_mode: pg_kinetic::config::ClientTlsMode::Disable,
            client_cert_path: None,
            client_key_path: None,
            client_ca_path: None,
            backend_tls_mode: BackendTlsMode::Disable,
            backend_ca_path: None,
            backend_server_name: None,
        },
        auth: AuthConfig {
            auth_mode: AuthMode::PassThrough,
            auth_users_file: None,
            backend_user: Some(String::from("proxy-user")),
            backend_password_env_var_name: Some(String::from("TOP_SECRET")),
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        },
        reload: ReloadConfig {
            config_file: None,
            config_reload_interval_ms: 50,
            reload_enabled: false,
        },
        drain: DrainConfig {
            drain_timeout_ms: 100,
            reject_new_clients_during_drain: true,
        },
        health: HealthConfig {
            health_addr: Some(health_addr),
            readiness_backend_check_interval_ms: 25,
            readiness_timeout_ms: 100,
        },
        socket: SocketConfig::default(),
    };

    let proxy = Proxy::new(config);
    let drain = proxy.drain_controller();
    let run_handle = tokio::spawn(async move {
        proxy.run().await.expect("proxy run");
    });

    time::sleep(Duration::from_millis(100)).await;
    (proxy_addr, health_addr, drain, run_handle)
}

async fn healthy_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = listener.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept backend");
            tokio::spawn(async move {
                let mut buffer = [0_u8; 256];
                let _ = stream.read(&mut buffer).await.expect("read backend probe");
                let _ = stream.shutdown().await;
            });
        }
    });

    backend_addr
}

struct HttpResponse {
    status: u16,
    body: String,
}

async fn http_get(addr: SocketAddr, path: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect health");
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");

    let mut response = Vec::new();
    time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .expect("read health response")
        .expect("health response");

    parse_response(&response)
}

async fn poll_ready(addr: SocketAddr, path: &str, expected_status: u16) -> HttpResponse {
    let mut attempts = 0_u32;
    loop {
        let response = http_get(addr, path).await;
        if response.status == expected_status {
            return response;
        }

        attempts += 1;
        assert!(
            attempts < 40,
            "timed out waiting for {path} to return {expected_status}"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
}

fn parse_response(response: &[u8]) -> HttpResponse {
    let response = std::str::from_utf8(response).expect("utf8 response");
    let mut parts = response.splitn(2, "\r\n\r\n");
    let head = parts.next().expect("response head");
    let body = parts.next().unwrap_or_default().to_string();
    let status_line = head.lines().next().expect("status line");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status code");

    HttpResponse { status, body }
}
