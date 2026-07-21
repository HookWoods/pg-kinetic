use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{BufMut, BytesMut};
use hmac::{Hmac, KeyInit, Mac};
use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, CapacityConfig, Config,
        ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig, PerformanceConfig,
        QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    proxy::Proxy,
    wire::{
        backend::{parse_backend_frame, BackendFrame, ReadyStatus},
        protocol::ProtocolVersion,
        startup::{parse_startup_packet, StartupPacket},
    },
};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
    time,
};

type HmacSha256 = Hmac<Sha256>;

const SCRAM_VERIFIER: &str = "SCRAM-SHA-256$4096:c2FsdHlzYWx0$RdRL9M4hIQ6KSGRy8YdcY/rWTt9c53a35goFQzcrGXw=:lNY6toUrz5jlkvLtdJbAj5bXIomZuncUbgsZq5rYF5M=";
static AUTH_SMOKE_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Clone, Copy)]
enum BackendAuthChallenge {
    Md5,
    ScramSha256,
}

#[tokio::test]
async fn pass_through_mode_keeps_backend_auth_behavior() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let (proxy_addr, mut events) = spawn_proxy(AuthMode::PassThrough, None).await;
    let frames = run_simple_startup(proxy_addr, "postgres").await;

    assert!(frames.iter().any(|frame| frame.tag == b'R'));
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(
        collect_events(&mut events).await,
        vec![String::from("backend_accept")]
    );
}

#[tokio::test]
async fn trust_mode_accepts_configured_user_without_password() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file("alice = trust\n");
    let (proxy_addr, mut events) = spawn_proxy(AuthMode::Trust, Some(auth_users_file)).await;
    let frames = run_simple_startup(proxy_addr, "alice").await;

    assert!(frames.iter().any(|frame| frame.tag == b'R'));
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(
        collect_events(&mut events).await,
        vec![String::from("backend_accept")]
    );
}

#[tokio::test]
async fn local_auth_uses_the_configured_backend_service_user() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file("alice = trust\n");
    let (proxy_addr, mut events) = spawn_proxy_with_backend_user(
        AuthMode::Trust,
        Some(auth_users_file),
        String::from("pool_user"),
    )
    .await;

    let frames = run_simple_startup(proxy_addr, "alice").await;

    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(
        collect_events(&mut events).await,
        vec![
            String::from("backend_accept"),
            String::from("backend_startup_user:pool_user"),
        ]
    );
}

#[tokio::test]
async fn local_auth_uses_the_service_password_for_backend_md5_authentication() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file("alice = trust\n");
    let (proxy_addr, mut events) =
        spawn_proxy_with_backend_service_auth(auth_users_file, BackendAuthChallenge::Md5).await;

    let frames = run_simple_startup(proxy_addr, "alice").await;

    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(
        collect_events(&mut events).await,
        vec![
            String::from("backend_accept"),
            String::from("backend_startup_user:pool_user"),
            String::from("backend_md5_password"),
        ]
    );
}

#[tokio::test]
async fn local_auth_uses_the_service_password_for_backend_scram_authentication() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file("alice = trust\n");
    let (proxy_addr, mut events) =
        spawn_proxy_with_backend_service_auth(auth_users_file, BackendAuthChallenge::ScramSha256)
            .await;

    let frames = run_simple_startup(proxy_addr, "alice").await;

    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(
        collect_events(&mut events).await,
        vec![
            String::from("backend_accept"),
            String::from("backend_startup_user:pool_user"),
            String::from("backend_scram_password"),
        ]
    );
}

#[tokio::test]
async fn scram_mode_accepts_valid_credentials() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file(&format!("alice = {SCRAM_VERIFIER}\n"));
    let (proxy_addr, mut events) = spawn_proxy(AuthMode::ScramSha256, Some(auth_users_file)).await;
    let frames = run_scram_startup(proxy_addr, "alice", b"pencil").await;

    assert!(frames.iter().any(|frame| frame.tag == b'R'));
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(
        collect_events(&mut events).await,
        vec![String::from("backend_accept")]
    );
}

#[tokio::test]
async fn scram_mode_rejects_invalid_password() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file(&format!("alice = {SCRAM_VERIFIER}\n"));
    let (proxy_addr, mut events) = spawn_proxy(AuthMode::ScramSha256, Some(auth_users_file)).await;
    let frames = run_scram_startup_expect_error(proxy_addr, "alice", b"wrong-password").await;

    assert!(
        frames
            .iter()
            .any(|frame| error_sqlstate(frame) == Some("28P01"))
            || frames.iter().any(|frame| frame.tag == b'E'),
        "SCRAM rejection should return a PostgreSQL error"
    );
    assert!(collect_events(&mut events).await.is_empty());
}

#[tokio::test]
async fn unknown_user_is_rejected_before_backend_checkout() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file("alice = trust\n");
    let (proxy_addr, mut events) = spawn_proxy(AuthMode::Trust, Some(auth_users_file)).await;
    let frames = run_simple_startup(proxy_addr, "charlie").await;

    assert!(
        frames
            .iter()
            .any(|frame| error_sqlstate(frame) == Some("28P01"))
            || frames.iter().any(|frame| frame.tag == b'E'),
        "unknown user should be rejected with a PostgreSQL auth failure"
    );
    assert!(collect_events(&mut events).await.is_empty());
}

#[tokio::test]
async fn auth_failure_does_not_checkout_a_backend() {
    let _guard = AUTH_SMOKE_LOCK.lock().await;
    let auth_users_file = write_auth_users_file(&format!("alice = {SCRAM_VERIFIER}\n"));
    let (proxy_addr, mut events) = spawn_proxy(AuthMode::ScramSha256, Some(auth_users_file)).await;
    let _ = run_scram_startup_expect_error(proxy_addr, "alice", b"bad-password").await;

    assert!(collect_events(&mut events).await.is_empty());
}

fn write_auth_users_file(contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pg-kinetic-auth-users-{}-{}.txt",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::write(&path, contents).expect("write auth users file");
    path
}

async fn spawn_proxy(
    auth_mode: AuthMode,
    auth_users_file: Option<PathBuf>,
) -> (SocketAddr, mpsc::Receiver<String>) {
    spawn_proxy_with_auth(
        AuthConfig {
            auth_mode,
            auth_users_file,
            backend_user: None,
            backend_password_env_var_name: None,
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        },
        false,
    )
    .await
}

async fn spawn_proxy_with_backend_user(
    auth_mode: AuthMode,
    auth_users_file: Option<PathBuf>,
    backend_user: String,
) -> (SocketAddr, mpsc::Receiver<String>) {
    spawn_proxy_with_auth(
        AuthConfig {
            auth_mode,
            auth_users_file,
            backend_user: Some(backend_user),
            backend_password_env_var_name: Some(String::from("CARGO_PKG_NAME")),
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        },
        true,
    )
    .await
}

async fn spawn_proxy_with_backend_service_auth(
    auth_users_file: PathBuf,
    challenge: BackendAuthChallenge,
) -> (SocketAddr, mpsc::Receiver<String>) {
    let (sender, receiver) = mpsc::channel(16);
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.expect("accept backend");
        sender
            .send(String::from("backend_accept"))
            .await
            .expect("send backend accept");

        let mut startup = [0_u8; 1024];
        let read = stream.read(&mut startup).await.expect("read startup");
        let StartupPacket::Startup { parameters, .. } =
            parse_startup_packet(&startup[..read]).expect("parse backend startup")
        else {
            panic!("expected backend startup packet");
        };
        let user = parameters
            .iter()
            .find(|(key, _)| key == "user")
            .map(|(_, value)| value.as_str())
            .expect("backend startup user");
        sender
            .send(format!("backend_startup_user:{user}"))
            .await
            .expect("send backend startup user");

        match challenge {
            BackendAuthChallenge::Md5 => {
                let mut md5_request = BytesMut::new();
                md5_request.put_u8(b'R');
                md5_request.put_i32(12);
                md5_request.put_i32(5);
                md5_request.extend_from_slice(&[1, 2, 3, 4]);
                stream
                    .write_all(&md5_request)
                    .await
                    .expect("write MD5 request");

                let payload = read_frontend_password_payload(&mut stream).await;
                assert!(payload.starts_with(b"md5"));
                assert_eq!(payload.last(), Some(&0));
                sender
                    .send(String::from("backend_md5_password"))
                    .await
                    .expect("send backend MD5 password");
            }
            BackendAuthChallenge::ScramSha256 => {
                let mut request = BytesMut::new();
                request.put_u8(b'R');
                request.put_i32(4 + 4 + "SCRAM-SHA-256".len() as i32 + 2);
                request.put_i32(10);
                request.extend_from_slice(b"SCRAM-SHA-256\0\0");
                stream
                    .write_all(&request)
                    .await
                    .expect("write SCRAM authentication request");

                let initial = read_frontend_password_payload(&mut stream).await;
                let mechanism_end = initial
                    .iter()
                    .position(|byte| *byte == 0)
                    .expect("SCRAM mechanism terminator");
                assert_eq!(&initial[..mechanism_end], b"SCRAM-SHA-256");
                let initial_len = i32::from_be_bytes(
                    initial[mechanism_end + 1..mechanism_end + 5]
                        .try_into()
                        .expect("SCRAM initial length"),
                ) as usize;
                let client_first = std::str::from_utf8(
                    &initial[mechanism_end + 5..mechanism_end + 5 + initial_len],
                )
                .expect("client first");
                let client_first_bare = client_first
                    .strip_prefix("n,,")
                    .expect("SCRAM client first prefix");
                let client_nonce = client_first_bare
                    .split(',')
                    .find_map(|item| item.strip_prefix("r="))
                    .expect("client nonce");
                let server_first = format!("r={client_nonce}server,s=c2FsdA==,i=4096");
                let mut continue_request = BytesMut::new();
                continue_request.put_u8(b'R');
                continue_request.put_i32((8 + server_first.len()) as i32);
                continue_request.put_i32(11);
                continue_request.extend_from_slice(server_first.as_bytes());
                stream
                    .write_all(&continue_request)
                    .await
                    .expect("write SCRAM server first");

                let final_response = read_frontend_password_payload(&mut stream).await;
                let final_response = std::str::from_utf8(&final_response).expect("SCRAM final");
                assert!(final_response.starts_with("c=biws,"));
                let final_without_proof = final_response
                    .rsplit_once(",p=")
                    .map(|(without_proof, _)| without_proof)
                    .expect("SCRAM client proof");
                let password = std::env::var("CARGO_PKG_NAME").expect("service password");
                let salted_password = pbkdf2_hmac_sha256(password.as_bytes(), b"salt", 4096);
                let server_key = hmac_sha256(&salted_password, b"Server Key");
                let auth_message =
                    format!("{client_first_bare},{server_first},{final_without_proof}");
                let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());
                let server_final = format!("v={}", STANDARD.encode(server_signature));
                let mut final_request = BytesMut::new();
                final_request.put_u8(b'R');
                final_request.put_i32((8 + server_final.len()) as i32);
                final_request.put_i32(12);
                final_request.extend_from_slice(server_final.as_bytes());
                stream
                    .write_all(&final_request)
                    .await
                    .expect("write SCRAM server final");
                sender
                    .send(String::from("backend_scram_password"))
                    .await
                    .expect("send backend SCRAM password");
            }
        }
        stream
            .write_all(&auth_ok_ready())
            .await
            .expect("write startup response");
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
        pool_lifecycle: Default::default(),
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
            auth_mode: AuthMode::Trust,
            auth_users_file: Some(auth_users_file),
            backend_user: Some(String::from("pool_user")),
            backend_password_env_var_name: Some(String::from("CARGO_PKG_NAME")),
            auth_failure_message_mode: AuthFailureMessageMode::Generic,
        },
        reload: ReloadConfig::default(),
        drain: DrainConfig::default(),
        health: HealthConfig::default(),
        socket: SocketConfig::default(),
    };

    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });
    time::sleep(Duration::from_millis(50)).await;

    (listen_addr, receiver)
}

async fn read_frontend_password_payload(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0_u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .expect("read password response header");
    assert_eq!(header[0], b'p');
    let payload_len =
        i32::from_be_bytes(header[1..5].try_into().expect("frame length")) as usize - 4;
    let mut payload = vec![0_u8; payload_len];
    stream
        .read_exact(&mut payload)
        .await
        .expect("read password response payload");
    payload
}

async fn spawn_proxy_with_auth(
    auth: AuthConfig,
    record_backend_startup_user: bool,
) -> (SocketAddr, mpsc::Receiver<String>) {
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
                sender
                    .send(String::from("backend_accept"))
                    .await
                    .expect("send backend accept");

                let mut startup = [0_u8; 1024];
                let read = stream.read(&mut startup).await.expect("read startup");
                if record_backend_startup_user {
                    let StartupPacket::Startup { parameters, .. } =
                        parse_startup_packet(&startup[..read]).expect("parse backend startup")
                    else {
                        panic!("expected backend startup packet");
                    };
                    let user = parameters
                        .iter()
                        .find(|(key, _)| key == "user")
                        .map(|(_, value)| value.as_str())
                        .expect("backend startup user");
                    sender
                        .send(format!("backend_startup_user:{user}"))
                        .await
                        .expect("send backend startup user");
                }
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
        pool_lifecycle: Default::default(),
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
        auth,
        reload: ReloadConfig::default(),
        drain: DrainConfig::default(),
        health: HealthConfig::default(),
        socket: SocketConfig::default(),
    };

    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });
    time::sleep(Duration::from_millis(50)).await;

    (listen_addr, receiver)
}

async fn run_simple_startup(addr: SocketAddr, user: &str) -> Vec<BackendFrame> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet(user))
        .await
        .expect("startup");

    collect_backend_frames(&mut stream).await
}

async fn run_scram_startup(addr: SocketAddr, user: &str, password: &[u8]) -> Vec<BackendFrame> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet(user))
        .await
        .expect("startup");

    let mut buffer = BytesMut::new();
    let mut frames = Vec::new();
    let client_nonce = "clientnonce";
    let mut sent_final = false;

    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            frames.push(frame.clone());
            if frame.tag == b'R' {
                let code = auth_code(&frame);
                match code {
                    10 => {
                        let initial = scram_initial_message(user, client_nonce);
                        stream
                            .write_all(&initial)
                            .await
                            .expect("write SCRAM initial");
                    }
                    11 => {
                        let server_first =
                            std::str::from_utf8(&frame.payload[4..]).expect("server first");
                        let final_message =
                            scram_final_response(user, password, client_nonce, server_first);
                        stream
                            .write_all(&scram_final_message(&final_message))
                            .await
                            .expect("write SCRAM final");
                        sent_final = true;
                    }
                    12 => {}
                    0 => {}
                    other => panic!("unexpected auth code {other}"),
                }
            }

            if sent_final
                && frames.iter().any(|frame| {
                    frame.tag == b'Z' && frame.ready_status() == Some(ReadyStatus::Idle)
                })
            {
                return frames;
            }
        }

        let mut chunk = [0_u8; 4096];
        let read = time::timeout(Duration::from_secs(1), stream.read(&mut chunk))
            .await
            .expect("read proxy response")
            .expect("read proxy response");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    frames
}

async fn run_scram_startup_expect_error(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Vec<BackendFrame> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet(user))
        .await
        .expect("startup");

    let mut buffer = BytesMut::new();
    let mut frames = Vec::new();
    let client_nonce = "clientnonce";

    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            frames.push(frame.clone());
            if frame.tag == b'R' {
                match auth_code(&frame) {
                    10 => {
                        let initial = scram_initial_message(user, client_nonce);
                        stream
                            .write_all(&initial)
                            .await
                            .expect("write SCRAM initial");
                    }
                    11 => {
                        let server_first =
                            std::str::from_utf8(&frame.payload[4..]).expect("server first");
                        let final_message =
                            scram_final_response(user, password, client_nonce, server_first);
                        stream
                            .write_all(&scram_final_message(&final_message))
                            .await
                            .expect("write SCRAM final");
                    }
                    12 => {}
                    0 => {}
                    other => panic!("unexpected auth code {other}"),
                }
            }
        }

        let mut chunk = [0_u8; 4096];
        match time::timeout(Duration::from_millis(200), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(read)) => buffer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => panic!("read proxy response: {error}"),
        }
    }

    if let Some(error) = frames.iter().find(|frame| frame.tag == b'E') {
        assert_eq!(error_sqlstate(error), Some("28P01"));
    }

    frames
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

async fn collect_events(receiver: &mut mpsc::Receiver<String>) -> Vec<String> {
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

fn scram_initial_message(user: &str, client_nonce: &str) -> Vec<u8> {
    let payload = scram_initial_response(user, client_nonce);
    let mut body = BytesMut::new();
    body.extend_from_slice(b"SCRAM-SHA-256\0");
    body.put_i32(payload.len() as i32);
    body.extend_from_slice(payload.as_bytes());

    let mut bytes = BytesMut::new();
    bytes.put_u8(b'p');
    bytes.put_i32((body.len() + 4) as i32);
    bytes.extend_from_slice(&body);
    bytes.to_vec()
}

fn scram_final_message(payload: &str) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'p');
    bytes.put_i32((payload.len() + 4) as i32);
    bytes.extend_from_slice(payload.as_bytes());
    bytes.to_vec()
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

fn scram_initial_response(user: &str, client_nonce: &str) -> String {
    format!("n,,n={user},r={client_nonce}")
}

fn scram_final_response(
    user: &str,
    password: &[u8],
    client_nonce: &str,
    server_first: &str,
) -> String {
    let (combined_nonce, salt, iterations) = parse_server_first(server_first);
    let client_first_bare = format!("n={user},r={client_nonce}");
    let client_final_without_proof = format!("c=biws,r={combined_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let proof = scram_client_proof(password, &salt, iterations, auth_message.as_bytes());
    format!("{client_final_without_proof},p={}", STANDARD.encode(proof))
}

fn parse_server_first(server_first: &str) -> (String, Vec<u8>, u32) {
    let mut combined_nonce = None;
    let mut salt = None;
    let mut iterations = None;

    for item in server_first.split(',') {
        let (key, value) = item.split_once('=').expect("server first item");
        match key {
            "r" => combined_nonce = Some(value.to_owned()),
            "s" => salt = Some(STANDARD.decode(value).expect("server salt")),
            "i" => iterations = Some(value.parse::<u32>().expect("iterations")),
            _ => {}
        }
    }

    (
        combined_nonce.expect("combined nonce"),
        salt.expect("salt"),
        iterations.expect("iterations"),
    )
}

fn scram_client_proof(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> [u8; 32] {
    let salted_password = pbkdf2_hmac_sha256(password, salt, iterations);
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = Sha256::digest(client_key);
    let client_signature = hmac_sha256(stored_key.as_slice(), auth_message);
    xor_bytes(&client_key, &client_signature)
}

fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut block = Vec::with_capacity(salt.len() + 4);
    block.extend_from_slice(salt);
    block.extend_from_slice(&1u32.to_be_bytes());

    let mut u = hmac_sha256(password, &block);
    let mut output = u;

    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        xor_in_place(&mut output, &u);
    }

    output
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("valid hmac key");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn xor_bytes(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = left[index] ^ right[index];
    }
    output
}

fn xor_in_place(left: &mut [u8; 32], right: &[u8; 32]) {
    for (left_byte, right_byte) in left.iter_mut().zip(right) {
        *left_byte ^= *right_byte;
    }
}

fn auth_code(frame: &BackendFrame) -> i32 {
    i32::from_be_bytes([
        frame.payload[0],
        frame.payload[1],
        frame.payload[2],
        frame.payload[3],
    ])
}

fn error_sqlstate(frame: &BackendFrame) -> Option<&str> {
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
        if field_type == b'C' {
            return Some(value);
        }
        offset += terminator + 1;
    }

    None
}
