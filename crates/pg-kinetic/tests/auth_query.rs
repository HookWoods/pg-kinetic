use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{BufMut, BytesMut};
use md5::Md5;
use pg_kinetic::{
    config::{
        AuthConfig, AuthFailureMessageMode, AuthMode, BackendTlsMode, CapacityConfig, Config,
        ConnectionConfig, DrainConfig, HealthConfig, ObservabilityConfig, PerformanceConfig,
        QosConfig, ReloadConfig, SocketConfig, TlsConfig,
    },
    proxy::Proxy,
    wire::{
        backend::{parse_backend_frame, BackendFrame, ReadyStatus},
        protocol::{BackendTag, ProtocolVersion, ReadyStatusByte},
    },
};
use sha2::Digest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time,
};

static AUTH_QUERY_LOCK: Mutex<()> = Mutex::const_new(());

#[tokio::test]
async fn unknown_static_user_is_looked_up_via_auth_query_and_cached() {
    let _guard = AUTH_QUERY_LOCK.lock().await;
    let query_count = Arc::new(AtomicUsize::new(0));
    let auth_secret = format!("md5{}", md5_password_hash("dynpass", "dyn_user"));
    let backend_addr = spawn_auth_query_backend(
        Arc::clone(&query_count),
        "SELECT usename, passwd FROM pg_shadow WHERE usename = 'dyn_user'",
        "dyn_user",
        &auth_secret,
    )
    .await;
    let users_file = write_auth_users_file("");
    let proxy_addr = spawn_proxy_with_auth_query(backend_addr, users_file).await;

    let frames = run_md5_startup(proxy_addr, "dyn_user", "dynpass").await;
    assert!(frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));

    let frames = run_md5_startup(proxy_addr, "dyn_user", "wrong").await;
    let error = frames
        .iter()
        .find(|frame| frame.tag == u8::from(BackendTag::ErrorResponse))
        .expect("bad password rejected");
    assert_eq!(error_sqlstate(error), Some("28P01"));
    assert_eq!(query_count.load(Ordering::SeqCst), 1);
}

async fn spawn_auth_query_backend(
    query_count: Arc<AtomicUsize>,
    expected_query: &'static str,
    username: &'static str,
    secret: &str,
) -> SocketAddr {
    let secret = secret.to_owned();
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await.expect("accept backend");
            let query_count = Arc::clone(&query_count);
            let secret = secret.clone();
            tokio::spawn(async move {
                let mut startup = [0_u8; 2048];
                let read = stream.read(&mut startup).await.expect("read startup");
                assert!(read >= 8, "backend startup packet is present");
                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("write startup response");

                let mut header = [0_u8; 5];
                match time::timeout(Duration::from_millis(200), stream.read_exact(&mut header))
                    .await
                {
                    Ok(Ok(_)) if header[0] == b'Q' => {
                        let payload_len =
                            i32::from_be_bytes(header[1..5].try_into().expect("query length"))
                                as usize
                                - 4;
                        let mut payload = vec![0_u8; payload_len];
                        stream
                            .read_exact(&mut payload)
                            .await
                            .expect("read query payload");
                        let query =
                            std::str::from_utf8(payload.strip_suffix(&[0]).expect("query nul"))
                                .expect("query utf8");
                        assert_eq!(query, expected_query);
                        query_count.fetch_add(1, Ordering::SeqCst);
                        stream
                            .write_all(&auth_query_response(username, &secret))
                            .await
                            .expect("write auth query response");
                    }
                    _ => {}
                }
            });
        }
    });

    backend_addr
}

async fn spawn_proxy_with_auth_query(
    backend_addr: SocketAddr,
    auth_users_file: PathBuf,
) -> SocketAddr {
    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr,
            backend_addr,
        },
        routes: Vec::new(),
        pools: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 2,
            max_checkout_waiters: 4,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 250,
            pool_mode: Default::default(),
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
            auth_mode: AuthMode::Md5,
            auth_users_file: Some(auth_users_file),
            backend_user: Some(String::from("auth_service")),
            backend_password_env_var_name: Some(String::from("CARGO_PKG_NAME")),
            auth_query_enabled: true,
            auth_query: String::from("SELECT usename, passwd FROM pg_shadow WHERE usename = $1"),
            auth_query_cache_ttl_ms: 60_000,
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
    listen_addr
}

async fn run_md5_startup(addr: SocketAddr, user: &str, password: &str) -> Vec<BackendFrame> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet(user))
        .await
        .expect("startup");

    let mut buffer = BytesMut::new();
    let mut frames = Vec::new();

    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            let ready = frame.ready_status();
            if frame.tag == b'R' && auth_code(&frame) == 5 {
                let salt: [u8; 4] = frame.payload[4..8].try_into().expect("md5 salt");
                stream
                    .write_all(&md5_password_message(password, user, salt))
                    .await
                    .expect("write MD5 password");
            }
            let is_error = frame.tag == b'E';
            frames.push(frame);
            if ready == Some(ReadyStatus::Idle) || is_error {
                return frames;
            }
        }

        let mut chunk = [0_u8; 4096];
        match time::timeout(Duration::from_millis(500), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(read)) => buffer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => panic!("read proxy response: {error}"),
        }
    }

    frames
}

fn write_auth_users_file(contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    path.push(format!("pg-kinetic-auth-query-{nanos}.txt"));
    std::fs::write(&path, contents).expect("write auth users file");
    path
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
    bytes.put_u8(u8::from(ReadyStatusByte::Idle));
    bytes.to_vec()
}

fn auth_query_response(username: &str, secret: &str) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&row_description());
    bytes.extend_from_slice(&data_row(username, secret));
    bytes.extend_from_slice(&command_complete());
    bytes.extend_from_slice(&ready_for_query());
    bytes.to_vec()
}

fn row_description() -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_i16(2);
    for name in ["usename", "passwd"] {
        payload.extend_from_slice(name.as_bytes());
        payload.put_u8(0);
        payload.put_i32(0);
        payload.put_i16(0);
        payload.put_i32(25);
        payload.put_i16(-1);
        payload.put_i32(-1);
        payload.put_i16(0);
    }
    backend_frame_raw(b'T', payload)
}

fn data_row(username: &str, secret: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_i16(2);
    for value in [username, secret] {
        payload.put_i32(value.len() as i32);
        payload.extend_from_slice(value.as_bytes());
    }
    backend_frame(BackendTag::DataRow, payload)
}

fn command_complete() -> BytesMut {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(b"SELECT 1\0");
    backend_frame(BackendTag::CommandComplete, payload)
}

fn ready_for_query() -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_u8(u8::from(ReadyStatusByte::Idle));
    backend_frame(BackendTag::ReadyForQuery, payload)
}

fn backend_frame(tag: BackendTag, payload: BytesMut) -> BytesMut {
    backend_frame_raw(u8::from(tag), payload)
}

fn backend_frame_raw(tag: u8, payload: BytesMut) -> BytesMut {
    let mut frame = BytesMut::new();
    frame.put_u8(tag);
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(&payload);
    frame
}

fn auth_code(frame: &BackendFrame) -> i32 {
    i32::from_be_bytes(frame.payload[0..4].try_into().expect("auth code"))
}

fn md5_password_message(password: &str, user: &str, salt: [u8; 4]) -> Vec<u8> {
    let first = md5_password_hash(password, user);
    let mut second_input = BytesMut::new();
    second_input.extend_from_slice(first.as_bytes());
    second_input.extend_from_slice(&salt);
    let response = format!("md5{}", hex_lower(Md5::digest(second_input).as_ref()));

    let mut bytes = BytesMut::new();
    bytes.put_u8(b'p');
    bytes.put_i32((response.len() + 5) as i32);
    bytes.extend_from_slice(response.as_bytes());
    bytes.put_u8(0);
    bytes.to_vec()
}

fn md5_password_hash(password: &str, user: &str) -> String {
    hex_lower(Md5::digest(format!("{password}{user}")).as_ref())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn error_sqlstate(frame: &BackendFrame) -> Option<&str> {
    let payload = frame.payload.as_ref();
    let mut offset = 0;
    while offset < payload.len() {
        let kind = payload[offset];
        offset += 1;
        if kind == 0 {
            return None;
        }
        let rest = payload.get(offset..)?;
        let nul = rest.iter().position(|byte| *byte == 0)?;
        let value = std::str::from_utf8(&rest[..nul]).ok()?;
        if kind == b'C' {
            return Some(value);
        }
        offset += nul + 1;
    }
    None
}
