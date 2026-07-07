use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, BackendTlsMode, CapacityConfig, Config,
        ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig, PerformanceConfig,
        QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    proxy::Proxy,
    wire::{
        backend::{parse_backend_frame, BackendFrame, ReadyStatus},
        protocol::ProtocolVersion,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[tokio::test]
async fn admin_listener_is_disabled_by_default() {
    let admin_addr = free_port().await;
    let backend_addr = free_port().await;
    let run_handle = spawn_proxy(None, None, backend_addr).await;

    let connect = time::timeout(Duration::from_millis(200), TcpStream::connect(admin_addr)).await;
    match connect {
        Err(_) => {}
        Ok(result) => assert!(result.is_err(), "expected no admin listener"),
    }

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn admin_listener_accepts_startup_without_backend_connection() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let run_handle = spawn_proxy(Some(admin_addr), Some("admin"), backend_addr).await;

    let mut stream = TcpStream::connect(admin_addr).await.expect("connect admin");
    stream
        .write_all(&startup_packet("admin"))
        .await
        .expect("startup");

    let frames = read_until_ready(&mut stream).await;
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn admin_listener_rejects_non_admin_users() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let run_handle = spawn_proxy(Some(admin_addr), Some("admin"), backend_addr).await;

    let mut stream = TcpStream::connect(admin_addr).await.expect("connect admin");
    stream
        .write_all(&startup_packet("postgres"))
        .await
        .expect("startup");

    let frames = read_until_ready(&mut stream).await;
    let error = frames.iter().find(|frame| frame.tag == b'E').expect("error response");
    assert!(error_message(error)
        .expect("error message")
        .contains("admin access restricted"));
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

#[tokio::test]
async fn unknown_command_returns_error_response() {
    let backend_hits = Arc::new(AtomicUsize::new(0));
    let backend_addr = spawn_backend_monitor(Arc::clone(&backend_hits)).await;
    let admin_addr = free_port().await;
    let run_handle = spawn_proxy(Some(admin_addr), Some("admin"), backend_addr).await;

    let mut stream = TcpStream::connect(admin_addr).await.expect("connect admin");
    stream
        .write_all(&startup_packet("admin"))
        .await
        .expect("startup");
    let _ = read_until_ready(&mut stream).await;

    stream
        .write_all(&query_packet("SELECT 1"))
        .await
        .expect("query");

    let frames = read_until_ready(&mut stream).await;
    let error = frames.iter().find(|frame| frame.tag == b'E').expect("error response");
    assert_eq!(error.sqlstate().map(|state| state.as_str()), Some("0A000"));
    assert!(error_message(error)
        .expect("error message")
        .contains("unsupported admin command"));
    assert_eq!(backend_hits.load(Ordering::SeqCst), 0);

    run_handle.abort();
    let _ = run_handle.await;
}

async fn spawn_proxy(
    admin_addr: Option<SocketAddr>,
    admin_allowed_user: Option<&str>,
    backend_addr: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr,
            backend_addr,
        },
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 1,
            max_checkout_waiters: 4,
        },
        performance: PerformanceConfig {
            checkout_timeout_ms: 250,
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
        admin: pg_kinetic::config::AdminConfig {
            admin_addr,
            admin_require_tls: false,
            admin_allowed_user: admin_allowed_user.map(str::to_owned),
            admin_query_timeout_ms: 100,
            admin_max_clients: 4,
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
            auth_mode: pg_kinetic::config::AuthMode::PassThrough,
            auth_users_file: None,
            backend_user: None,
            backend_password_env_var_name: None,
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        },
        reload: ReloadConfig::default(),
        drain: DrainConfig::default(),
        health: HealthConfig::default(),
        socket: SocketConfig::default(),
    };

    let proxy = Proxy::new(config);
    let handle = tokio::spawn(async move {
        proxy.run().await.expect("proxy run");
    });
    time::sleep(Duration::from_millis(50)).await;
    handle
}

async fn spawn_backend_monitor(hits: Arc<AtomicUsize>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = listener.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept backend");
            let hits = Arc::clone(&hits);
            tokio::spawn(async move {
                hits.fetch_add(1, Ordering::SeqCst);
                let mut startup = [0_u8; 1024];
                let _ = stream.read(&mut startup).await.expect("read startup");
                let mut sink = [0_u8; 256];
                let _ = stream.read(&mut sink).await.expect("read follow-up");
            });
        }
    });

    backend_addr
}

async fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port");
    let addr = listener.local_addr().expect("free addr");
    drop(listener);
    addr
}

async fn read_until_ready(stream: &mut TcpStream) -> Vec<BackendFrame> {
    let mut buffer = BytesMut::new();
    let mut frames = Vec::new();

    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            let ready = frame.ready_status();
            frames.push(frame);
            if ready == Some(ReadyStatus::Idle) {
                return frames;
            }
        }

        let mut chunk = [0_u8; 4096];
        match time::timeout(Duration::from_millis(250), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(read)) => buffer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => panic!("read admin response: {error}"),
        }
    }

    frames
}

fn startup_packet(user: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.extend_from_slice(b"\0database\0pgkinetic\0\0");

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet.to_vec()
}

fn query_packet(sql: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.put_u8(0);

    let mut packet = BytesMut::new();
    packet.put_u8(b'Q');
    packet.put_i32((payload.len() + 4) as i32);
    packet.extend_from_slice(&payload);
    packet.to_vec()
}

fn error_message(frame: &BackendFrame) -> Option<&str> {
    if frame.tag != b'E' {
        return None;
    }

    let mut offset = 0;
    while offset < frame.payload.len() {
        let field_kind = frame.payload[offset];
        offset += 1;
        if field_kind == 0 {
            return None;
        }

        let remaining = frame.payload.get(offset..)?;
        let terminator = remaining.iter().position(|byte| *byte == 0)?;
        let value = std::str::from_utf8(&remaining[..terminator]).ok()?;
        if field_kind == b'M' {
            return Some(value);
        }
        offset += terminator + 1;
    }

    None
}
