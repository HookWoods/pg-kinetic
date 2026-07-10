use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, BackendTlsMode, CapacityConfig, Config,
        ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig, PerformanceConfig,
        QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    proxy::Proxy,
    proxy_runtime::drain::{DrainController, DrainOutcome},
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
async fn drain_stops_accepting_new_clients_and_rejects_late_arrivals() {
    let (proxy_addr, drain, mut events, run_handle) = spawn_proxy().await;

    let mut first_client = TcpStream::connect(proxy_addr)
        .await
        .expect("connect first client");
    first_client
        .write_all(&startup_packet("postgres"))
        .await
        .expect("startup first client");
    let first_frames = collect_backend_frames(&mut first_client).await;
    assert!(first_frames.iter().any(|frame| frame.tag == b'Z'));

    drain.begin_drain(Duration::from_millis(500));
    time::sleep(Duration::from_millis(50)).await;

    let mut second_client = TcpStream::connect(proxy_addr)
        .await
        .expect("connect second client");
    second_client
        .write_all(&startup_packet("postgres"))
        .await
        .expect("startup second client");

    let rejection = collect_backend_frames(&mut second_client).await;
    let error = rejection
        .iter()
        .find(|frame| frame.tag == b'E')
        .expect("drain rejection error");
    assert_eq!(
        error.sqlstate(),
        Some(pg_kinetic::wire::sqlstate::SqlState::OperatorIntervention)
    );
    assert!(
        error_message(error)
            .expect("drain message")
            .contains("proxy is draining"),
        "expected a clear drain rejection"
    );

    drop(second_client);
    drop(first_client);

    run_handle.await.expect("proxy run");
    let events = collect_events(&mut events).await;
    assert_eq!(events, vec![String::from("backend_accept")]);
    assert_eq!(drain.state().as_str(), "drained");
    assert_eq!(drain.active_clients(), 0);
}

#[tokio::test]
async fn active_clients_complete_before_drain_timeout() {
    let (proxy_addr, drain, mut events, run_handle) = spawn_proxy().await;

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("connect client");
    client
        .write_all(&startup_packet("postgres"))
        .await
        .expect("startup client");
    let frames = collect_backend_frames(&mut client).await;
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));

    drain.begin_drain(Duration::from_secs(1));
    time::sleep(Duration::from_millis(100)).await;
    drop(client);

    run_handle.await.expect("proxy run");

    let events = collect_events(&mut events).await;
    assert_eq!(events, vec![String::from("backend_accept")]);
    assert_eq!(drain.state().as_str(), "drained");
    assert_eq!(drain.active_clients(), 0);
}

#[tokio::test]
async fn drain_controller_reports_not_ready_while_draining() {
    let drain = Arc::new(DrainController::new());

    assert!(drain.is_ready());
    assert_eq!(drain.state().as_str(), "accepting");

    drain.begin_drain(Duration::from_millis(50));

    assert!(drain.is_draining());
    assert!(!drain.is_ready());
    assert_eq!(drain.state().as_str(), "draining");
}

#[tokio::test]
async fn drain_wait_times_out_when_clients_do_not_finish() {
    let drain = Arc::new(DrainController::new());
    let _client = drain.try_enter_client().expect("client guard");

    drain.begin_drain(Duration::from_millis(25));

    let outcome = drain.wait_for_completion().await;
    assert_eq!(outcome, DrainOutcome::TimedOut);
    assert!(drain.is_draining());
}

async fn spawn_proxy() -> (
    SocketAddr,
    Arc<DrainController>,
    tokio::sync::mpsc::Receiver<String>,
    tokio::task::JoinHandle<()>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(16);
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await.expect("accept backend");
            let sender = sender.clone();
            tokio::spawn(async move {
                sender
                    .send(String::from("backend_accept"))
                    .await
                    .expect("send backend accept");

                let mut startup = [0_u8; 1024];
                let _ = stream.read(&mut startup).await.expect("read startup");
                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("write startup response");

                let mut sink = [0_u8; 256];
                let _ = stream.read(&mut sink).await.expect("read follow-up");
            });
        }
    });

    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let config = Config {
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
        admin: Default::default(),
        observability: ObservabilityConfig {
            metrics_addr: None,
            ..Default::default()
        },
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
    let drain = proxy.drain_controller();
    let run_handle = tokio::spawn(async move {
        proxy.run().await.expect("proxy run");
    });
    time::sleep(Duration::from_millis(50)).await;

    (listen_addr, drain, receiver, run_handle)
}

async fn collect_backend_frames(stream: &mut TcpStream) -> Vec<BackendFrame> {
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
        match time::timeout(Duration::from_millis(200), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(read)) => buffer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => panic!("read proxy response: {error}"),
        }
    }

    frames
}

async fn collect_events(receiver: &mut tokio::sync::mpsc::Receiver<String>) -> Vec<String> {
    let mut events = Vec::new();
    while let Ok(Some(event)) = time::timeout(Duration::from_millis(200), receiver.recv()).await {
        events.push(event);
    }
    events
}

fn startup_packet(user: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"database\0pgkinetic\0\0");

    let mut packet = BytesMut::new();
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet.to_vec()
}

fn auth_ok_ready() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'R');
    bytes.put_i32(8);
    bytes.put_i32(0);
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}

fn error_message(frame: &BackendFrame) -> Option<&str> {
    if frame.tag != b'E' {
        return None;
    }

    let mut offset = 0;
    while offset < frame.payload.len() {
        let field_type = frame.payload[offset];
        offset += 1;
        if field_type == 0 {
            return None;
        }

        let remaining = frame.payload.get(offset..)?;
        let terminator = remaining.iter().position(|byte| *byte == 0)?;
        let value = std::str::from_utf8(&remaining[..terminator]).ok()?;
        if field_type == b'M' {
            return Some(value);
        }
        offset += terminator + 1;
    }

    None
}
