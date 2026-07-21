use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        BackendTlsMode, CapacityConfig, ClientTlsMode, Config, ConnectionConfig,
        ObservabilityConfig, PerformanceConfig, QosConfig, TlsConfig,
    },
    proxy::Proxy,
    wire::protocol::ProtocolVersion,
};
use pg_kinetic_proxy::backend::Backend;
use pg_kinetic_proxy::tls::{load_backend_client_config, load_server_config};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
};
use tokio_rustls::{
    client::TlsStream as ClientTlsStream, rustls::pki_types::ServerName, TlsAcceptor, TlsConnector,
};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

fn proxy_tls_config(client_tls_mode: ClientTlsMode) -> TlsConfig {
    TlsConfig {
        client_tls_mode,
        client_cert_path: Some(fixture_path("server-chain.pem")),
        client_key_path: Some(fixture_path("server-key.pem")),
        client_ca_path: if matches!(client_tls_mode, ClientTlsMode::VerifyClient) {
            Some(fixture_path("ca.pem"))
        } else {
            None
        },
        backend_tls_mode: BackendTlsMode::Disable,
        backend_ca_path: Some(fixture_path("ca.pem")),
        backend_server_name: Some(String::from("localhost")),
    }
}

fn client_tls_config() -> TlsConfig {
    TlsConfig {
        client_tls_mode: ClientTlsMode::Disable,
        client_cert_path: None,
        client_key_path: None,
        client_ca_path: None,
        backend_tls_mode: BackendTlsMode::Disable,
        backend_ca_path: Some(fixture_path("ca.pem")),
        backend_server_name: Some(String::from("localhost")),
    }
}

fn backend_tls_config(
    backend_tls_mode: BackendTlsMode,
    backend_ca_path: Option<PathBuf>,
    backend_server_name: Option<String>,
) -> TlsConfig {
    TlsConfig {
        client_tls_mode: ClientTlsMode::Disable,
        client_cert_path: None,
        client_key_path: None,
        client_ca_path: None,
        backend_tls_mode,
        backend_ca_path,
        backend_server_name,
    }
}

fn base_config(
    client_tls_mode: ClientTlsMode,
    backend_addr: SocketAddr,
    listen_addr: SocketAddr,
) -> Config {
    Config {
        connection: ConnectionConfig {
            listen_addr,
            backend_addr,
        },
        routes: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 1,
            max_checkout_waiters: 4,
        },
        pool_lifecycle: Default::default(),
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
        admin: Default::default(),
        observability: ObservabilityConfig {
            metrics_addr: None,
            ..Default::default()
        },
        tls: proxy_tls_config(client_tls_mode),
        auth: Default::default(),
        reload: Default::default(),
        drain: Default::default(),
        health: Default::default(),
        socket: Default::default(),
    }
}

fn startup_packet() -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0postgres\0database\0pgkinetic\0\0");

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet.to_vec()
}

fn ssl_request_packet() -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_i32(8);
    packet.put_i32(pg_kinetic::wire::protocol::SSL_REQUEST_CODE);
    packet.to_vec()
}

fn startup_ready() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'R');
    bytes.put_i32(8);
    bytes.put_i32(0);
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}

async fn spawn_proxy(client_tls_mode: ClientTlsMode) -> (SocketAddr, mpsc::Receiver<String>) {
    let (sender, receiver) = mpsc::channel(16);
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await.expect("accept backend");
            let sender = sender.clone();
            tokio::spawn(async move {
                let mut startup = [0_u8; 2048];
                let read = stream.read(&mut startup).await.expect("read startup");
                if read == 0 {
                    sender
                        .send(String::from("backend_closed"))
                        .await
                        .expect("send close");
                    return;
                }

                sender
                    .send(String::from("backend_startup"))
                    .await
                    .expect("send startup");
                stream
                    .write_all(&startup_ready())
                    .await
                    .expect("write startup ready");

                let mut buffer = [0_u8; 256];
                let _ = stream.read(&mut buffer).await.expect("read follow-up");
            });
        }
    });

    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let config = base_config(client_tls_mode, backend_addr, listen_addr);
    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });

    time::sleep(Duration::from_millis(50)).await;
    (listen_addr, receiver)
}

async fn collect_events(receiver: &mut mpsc::Receiver<String>) -> Vec<String> {
    let mut events = Vec::new();
    while let Ok(Some(event)) = time::timeout(Duration::from_millis(200), receiver.recv()).await {
        events.push(event);
    }
    events
}

async fn connect_tls_client(addr: SocketAddr) -> ClientTlsStream<TcpStream> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&ssl_request_packet())
        .await
        .expect("ssl request");

    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .expect("ssl response");
    assert_eq!(response, *b"S");

    let connector = TlsConnector::from(
        load_backend_client_config(&client_tls_config()).expect("client TLS config"),
    );
    let server_name = ServerName::try_from("localhost").expect("server name");
    connector
        .connect(server_name, stream)
        .await
        .expect("TLS handshake")
}

async fn run_plain_startup(addr: SocketAddr) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut response = [0_u8; 256];
    let read = stream.read(&mut response).await.expect("startup response");
    response[..read].to_vec()
}

async fn run_tls_startup(addr: SocketAddr) -> Vec<u8> {
    let mut stream = connect_tls_client(addr).await;
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut response = [0_u8; 256];
    let read = stream.read(&mut response).await.expect("startup response");
    response[..read].to_vec()
}

fn backend_server_config() -> Arc<tokio_rustls::rustls::ServerConfig> {
    load_server_config(&proxy_tls_config(ClientTlsMode::Allow)).expect("server config")
}

#[tokio::test]
async fn server_config_loads_certificate_chain_and_key() {
    let config = proxy_tls_config(ClientTlsMode::Allow);

    let tls_config = load_server_config(&config).expect("server TLS config loads");

    assert_eq!(Arc::strong_count(&tls_config), 1);
}

#[tokio::test]
async fn backend_client_config_loads_ca_roots_from_pem() {
    let config = client_tls_config();

    let client_config = load_backend_client_config(&config).expect("backend TLS config loads");

    assert_eq!(Arc::strong_count(&client_config), 1);
}

#[tokio::test]
async fn invalid_cert_and_key_paths_are_reported_clearly() {
    let mut missing_cert = proxy_tls_config(ClientTlsMode::Allow);
    missing_cert.client_cert_path = Some(fixture_path("missing-cert.pem"));

    let cert_error = load_server_config(&missing_cert).expect_err("missing cert path fails");
    let cert_error = cert_error.to_string();
    assert!(
        cert_error.contains("missing-cert.pem"),
        "error should mention missing certificate path: {cert_error}"
    );
    assert!(
        cert_error.contains("client TLS certificate"),
        "error should mention certificate loading: {cert_error}"
    );

    let mut missing_key = proxy_tls_config(ClientTlsMode::Allow);
    missing_key.client_key_path = Some(fixture_path("missing-key.pem"));

    let key_error = load_server_config(&missing_key).expect_err("missing key path fails");
    let key_error = key_error.to_string();
    assert!(
        key_error.contains("missing-key.pem"),
        "error should mention missing key path: {key_error}"
    );
    assert!(
        key_error.contains("client TLS private key"),
        "error should mention key loading: {key_error}"
    );
}

#[tokio::test]
async fn verify_client_mode_requires_client_ca() {
    let mut config = proxy_tls_config(ClientTlsMode::VerifyClient);
    config.client_ca_path = None;

    let error = load_server_config(&config).expect_err("verify client requires ca");
    let error = error.to_string();
    assert!(
        error.contains("client TLS CA path is required"),
        "error should explain missing client CA: {error}"
    );
}

#[tokio::test]
async fn client_tls_disable_denies_ssl_request_with_n() {
    let (proxy_addr, mut events) = spawn_proxy(ClientTlsMode::Disable).await;
    let mut stream = TcpStream::connect(proxy_addr).await.expect("connect proxy");

    stream
        .write_all(&ssl_request_packet())
        .await
        .expect("ssl request");

    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .expect("ssl response");
    assert_eq!(response, *b"N");

    drop(stream);
    let events = collect_events(&mut events).await;
    assert!(events.is_empty());
}

#[tokio::test]
async fn client_tls_disable_accepts_plain_startup_after_ssl_denial() {
    let (proxy_addr, mut events) = spawn_proxy(ClientTlsMode::Disable).await;
    let mut stream = TcpStream::connect(proxy_addr).await.expect("connect proxy");

    stream
        .write_all(&ssl_request_packet())
        .await
        .expect("ssl request");

    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .expect("ssl response");
    assert_eq!(response, *b"N");

    stream.write_all(&startup_packet()).await.expect("startup");
    let mut startup_response = [0_u8; 256];
    let read = time::timeout(Duration::from_secs(1), stream.read(&mut startup_response))
        .await
        .expect("startup response timeout")
        .expect("startup response");
    assert!(read > 0, "plain startup should reach backend");

    let events = collect_events(&mut events).await;
    assert!(events.iter().any(|event| event == "backend_startup"));
}

#[tokio::test]
async fn client_tls_allow_accepts_plain_startup() {
    let (proxy_addr, mut events) = spawn_proxy(ClientTlsMode::Allow).await;

    let response = run_plain_startup(proxy_addr).await;
    assert!(!response.is_empty(), "plain startup should reach backend");

    let events = collect_events(&mut events).await;
    assert!(events.iter().any(|event| event == "backend_startup"));
}

#[tokio::test]
async fn client_tls_allow_accepts_tls_startup() {
    let (proxy_addr, mut events) = spawn_proxy(ClientTlsMode::Allow).await;

    let response = run_tls_startup(proxy_addr).await;
    assert!(!response.is_empty(), "TLS startup should reach backend");

    let events = collect_events(&mut events).await;
    assert!(events.iter().any(|event| event == "backend_startup"));
}

#[tokio::test]
async fn client_tls_require_rejects_plain_startup() {
    let (proxy_addr, mut events) = spawn_proxy(ClientTlsMode::Require).await;
    let mut stream = TcpStream::connect(proxy_addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut response = [0_u8; 1];
    let read = time::timeout(Duration::from_secs(1), stream.read(&mut response))
        .await
        .expect("plain startup rejection")
        .expect("read rejection");
    assert_eq!(read, 0);

    let events = collect_events(&mut events).await;
    assert!(events.is_empty(), "backend should not be touched");
}

#[tokio::test]
async fn client_tls_require_accepts_tls_startup() {
    let (proxy_addr, mut events) = spawn_proxy(ClientTlsMode::Require).await;

    let response = run_tls_startup(proxy_addr).await;
    assert!(!response.is_empty(), "TLS startup should reach backend");

    let events = collect_events(&mut events).await;
    assert!(events.iter().any(|event| event == "backend_startup"));
}

#[tokio::test]
async fn client_tls_verify_client_rejects_clients_without_cert() {
    let (proxy_addr, mut events) = spawn_proxy(ClientTlsMode::VerifyClient).await;
    let mut stream = TcpStream::connect(proxy_addr).await.expect("connect proxy");
    stream
        .write_all(&ssl_request_packet())
        .await
        .expect("ssl request");

    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .expect("ssl response");
    assert_eq!(response, *b"S");

    let connector = TlsConnector::from(
        load_backend_client_config(&client_tls_config()).expect("client TLS config"),
    );
    let server_name = ServerName::try_from("localhost").expect("server name");
    let mut stream = connector
        .connect(server_name, stream)
        .await
        .expect("TLS handshake");

    stream.write_all(&startup_packet()).await.expect("startup");
    let mut response = [0_u8; 1];
    let read = time::timeout(Duration::from_secs(1), stream.read(&mut response))
        .await
        .expect("mTLS rejection");
    assert!(read.is_err() || read.expect("read rejection") == 0);

    let events = collect_events(&mut events).await;
    assert!(events.is_empty(), "backend should not be touched");
}

#[tokio::test]
async fn backend_tls_disable_connects_without_ssl_request() {
    let (sender, mut receiver) = mpsc::channel(8);
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.expect("accept backend");
        let mut header = [0_u8; 4];
        stream
            .read_exact(&mut header)
            .await
            .expect("read startup len");
        let len = i32::from_be_bytes(header) as usize;
        assert_ne!(len, 8, "backend should not receive an SSLRequest");
        let mut body = vec![0_u8; len - 4];
        stream
            .read_exact(&mut body)
            .await
            .expect("read startup body");
        sender
            .send(String::from("startup"))
            .await
            .expect("send startup");
    });

    let config = backend_tls_config(BackendTlsMode::Disable, None, None);
    let mut backend = Backend::connect(backend_addr, &config)
        .await
        .expect("backend connect");
    backend
        .stream_mut()
        .write_all(&startup_packet())
        .await
        .expect("write startup");

    let events = collect_events(&mut receiver).await;
    assert_eq!(events, vec![String::from("startup")]);
}

#[tokio::test]
async fn backend_tls_prefer_falls_back_when_backend_denies_tls() {
    let (sender, mut receiver) = mpsc::channel(8);
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.expect("accept backend");
        let mut ssl_request = [0_u8; 8];
        stream
            .read_exact(&mut ssl_request)
            .await
            .expect("read ssl request");
        assert_eq!(ssl_request.to_vec(), ssl_request_packet());
        sender
            .send(String::from("ssl_request"))
            .await
            .expect("send ssl request");
        stream
            .write_all(&[u8::from(pg_kinetic_wire::tls::SslResponse::Deny)])
            .await
            .expect("deny tls");

        let mut header = [0_u8; 4];
        stream
            .read_exact(&mut header)
            .await
            .expect("read startup len");
        let len = i32::from_be_bytes(header) as usize;
        assert_ne!(len, 8, "fallback should continue with startup packet");
        let mut body = vec![0_u8; len - 4];
        stream
            .read_exact(&mut body)
            .await
            .expect("read startup body");
        sender
            .send(String::from("startup"))
            .await
            .expect("send startup");
    });

    let config = backend_tls_config(
        BackendTlsMode::Prefer,
        None,
        Some(String::from("localhost")),
    );
    let mut backend = Backend::connect(backend_addr, &config)
        .await
        .expect("backend connect");
    backend
        .stream_mut()
        .write_all(&startup_packet())
        .await
        .expect("write startup");

    let events = collect_events(&mut receiver).await;
    assert_eq!(
        events,
        vec![String::from("ssl_request"), String::from("startup")]
    );
}

#[tokio::test]
async fn backend_tls_require_fails_when_backend_denies_tls() {
    let (sender, mut receiver) = mpsc::channel(8);
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.expect("accept backend");
        let mut ssl_request = [0_u8; 8];
        stream
            .read_exact(&mut ssl_request)
            .await
            .expect("read ssl request");
        assert_eq!(ssl_request.to_vec(), ssl_request_packet());
        sender
            .send(String::from("ssl_request"))
            .await
            .expect("send ssl request");
        stream
            .write_all(&[u8::from(pg_kinetic_wire::tls::SslResponse::Deny)])
            .await
            .expect("deny tls");
    });

    let config = backend_tls_config(
        BackendTlsMode::Require,
        None,
        Some(String::from("localhost")),
    );
    let error = Backend::connect(backend_addr, &config)
        .await
        .expect_err("backend tls require should fail");
    assert!(
        error.to_string().contains("backend denied TLS negotiation"),
        "unexpected error: {error}"
    );

    let events = collect_events(&mut receiver).await;
    assert_eq!(events, vec![String::from("ssl_request")]);
}

#[tokio::test]
async fn backend_tls_verify_ca_requires_ca_configuration() {
    let config = backend_tls_config(
        BackendTlsMode::VerifyCa,
        None,
        Some(String::from("localhost")),
    );
    let error = Backend::connect("127.0.0.1:5432".parse().expect("backend addr"), &config)
        .await
        .expect_err("verify_ca without CA should fail");
    assert!(
        error
            .to_string()
            .contains("backend TLS CA path is required for backend TLS verification"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn backend_tls_verify_full_requires_server_name() {
    let config = backend_tls_config(
        BackendTlsMode::VerifyFull,
        Some(fixture_path("ca.pem")),
        None,
    );
    let error = Backend::connect("127.0.0.1:5432".parse().expect("backend addr"), &config)
        .await
        .expect_err("verify_full without server name should fail");
    assert!(
        error
            .to_string()
            .contains("backend TLS server name is required for backend TLS verify_full"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn backend_tls_verify_full_accepts_tls_with_ca_and_server_name() {
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");
    let server_config = backend_server_config();

    tokio::spawn(async move {
        let (stream, _) = backend.accept().await.expect("accept backend");
        let mut stream = stream;
        let mut ssl_request = [0_u8; 8];
        stream
            .read_exact(&mut ssl_request)
            .await
            .expect("read ssl request");
        assert_eq!(ssl_request.to_vec(), ssl_request_packet());
        stream
            .write_all(&[u8::from(pg_kinetic_wire::tls::SslResponse::Accept)])
            .await
            .expect("accept tls");
        let mut stream = TlsAcceptor::from(server_config)
            .accept(stream)
            .await
            .expect("backend tls handshake");
        let mut header = [0_u8; 4];
        stream
            .read_exact(&mut header)
            .await
            .expect("read startup len");
        let len = i32::from_be_bytes(header) as usize;
        assert_ne!(len, 8, "verify_full should continue with startup packet");
        let mut body = vec![0_u8; len - 4];
        stream
            .read_exact(&mut body)
            .await
            .expect("read startup body");
    });

    let config = backend_tls_config(
        BackendTlsMode::VerifyFull,
        Some(fixture_path("ca.pem")),
        Some(String::from("localhost")),
    );
    let mut backend = Backend::connect(backend_addr, &config)
        .await
        .expect("backend connect");
    backend
        .stream_mut()
        .write_all(&startup_packet())
        .await
        .expect("write startup");
}
