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
        CapacityConfig, Config, ConnectionConfig, ObservabilityConfig, PerformanceConfig, QosConfig,
    },
    proxy::{retry_disposition, BackendFailureKind, Proxy, RetryDisposition},
    recovery::RecoveryMode,
    wire::{
        backend::{parse_backend_frame, BackendFrame},
        frame::parse_frontend_frame,
        message::parse_simple_query,
        protocol::{FrontendTag, ProtocolVersion},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[test]
fn retry_disposition_retries_a_read_when_backend_disconnects_before_any_response() {
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
async fn retries_a_read_when_backend_disconnects_before_any_response() {
    let (proxy_addr, backend_attempts) =
        spawn_proxy_with_backend(BackendBehavior::CloseBeforeResponse).await;

    let mut client = open_client(proxy_addr).await;
    send_query(&mut client, "select 1").await;

    let frames = read_backend_frames(&mut client, true).await;
    assert!(frames.iter().any(|frame| frame.tag == b'Z'));
    assert_eq!(backend_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn does_not_retry_after_a_partial_response_is_forwarded() {
    let (proxy_addr, backend_attempts) =
        spawn_proxy_with_backend(BackendBehavior::CloseAfterPartialResponse).await;

    let mut client = open_client(proxy_addr).await;
    send_query(&mut client, "select 1").await;

    let frames = read_backend_frames(&mut client, false).await;
    assert!(frames.iter().any(|frame| frame.tag == b'D'));
    assert_eq!(backend_attempts.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendBehavior {
    CloseBeforeResponse,
    CloseAfterPartialResponse,
}

async fn spawn_proxy_with_backend(behavior: BackendBehavior) -> (SocketAddr, Arc<AtomicUsize>) {
    let backend_attempts = Arc::new(AtomicUsize::new(0));
    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend_listener.local_addr().expect("backend addr");

    tokio::spawn({
        let backend_attempts = backend_attempts.clone();
        async move {
            loop {
                let (stream, _) = backend_listener.accept().await.expect("accept backend");
                let backend_attempts = backend_attempts.clone();
                tokio::spawn(async move {
                    handle_backend_connection(stream, backend_attempts, behavior).await;
                });
            }
        }
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    drop(proxy_listener);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr: proxy_addr,
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
            pool_mode: Default::default(),
            recovery_mode: RecoveryMode::Recover,
            recovery_timeout_ms: 1_000,
            backend_reset_query: "DISCARD ALL".to_string(),
        },
        qos: QosConfig {
            max_route_in_flight: 100,
            max_route_waiters: 1_000,
            query_timeout_ms: 30_000,
            idle_client_timeout_ms: 300_000,
            idle_transaction_timeout_ms: 60_000,
            max_client_buffer_bytes: 1_048_576,
            max_backend_buffer_bytes: 4_194_304,
            overload_error_code: "53300".to_string(),
        },
        admin: Default::default(),
        observability: ObservabilityConfig {
            metrics_addr: None,
            ..Default::default()
        },
        tls: Default::default(),
        auth: Default::default(),
        reload: Default::default(),
        drain: Default::default(),
        health: Default::default(),
        socket: Default::default(),
    };

    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });
    time::sleep(Duration::from_millis(100)).await;

    (proxy_addr, backend_attempts)
}

async fn handle_backend_connection(
    mut stream: TcpStream,
    backend_attempts: Arc<AtomicUsize>,
    behavior: BackendBehavior,
) {
    backend_attempts.fetch_add(1, Ordering::SeqCst);

    let mut startup = [0_u8; 2048];
    let read = stream.read(&mut startup).await.expect("read startup");
    if read == 0 {
        return;
    }

    stream
        .write_all(&auth_ok_ready())
        .await
        .expect("write startup response");

    let mut buffer = BytesMut::with_capacity(4096);
    loop {
        let read = stream.read_buf(&mut buffer).await.expect("read frontend");
        if read == 0 {
            return;
        }

        while let Some(frame) = parse_frontend_frame(&mut buffer).expect("parse frontend frame") {
            if frame.tag != u8::from(FrontendTag::Query) {
                continue;
            }

            let query = parse_simple_query(&frame)
                .expect("parse simple query")
                .unwrap_or("");
            if query.is_empty() {
                continue;
            }

            match behavior {
                BackendBehavior::CloseBeforeResponse => {
                    stream
                        .shutdown()
                        .await
                        .expect("close backend before response");
                    return;
                }
                BackendBehavior::CloseAfterPartialResponse => {
                    stream
                        .write_all(&data_row("1"))
                        .await
                        .expect("write partial response");
                    stream
                        .shutdown()
                        .await
                        .expect("close backend after partial response");
                    return;
                }
            }
        }
    }
}

async fn open_client(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");
    let _ = read_backend_frames(&mut stream, true).await;
    stream
}

async fn send_query(stream: &mut TcpStream, query: &str) {
    stream.write_all(&query_packet(query)).await.expect("query");
}

async fn read_backend_frames(stream: &mut TcpStream, stop_on_ready: bool) -> Vec<BackendFrame> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut frames = Vec::new();

    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            let ready = frame.tag == b'Z';
            frames.push(frame);
            if stop_on_ready && ready {
                return frames;
            }
        }

        let read = time::timeout(Duration::from_secs(2), stream.read_buf(&mut buffer))
            .await
            .expect("timeout waiting for backend response")
            .expect("read backend response");
        if read == 0 {
            return frames;
        }
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

fn query_packet(sql: &str) -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_u8(u8::from(FrontendTag::Query));
    packet.put_i32((sql.len() + 5) as i32);
    packet.extend_from_slice(sql.as_bytes());
    packet.put_u8(0);
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

fn data_row(value: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_i16(1);
    payload.put_i32(value.len() as i32);
    payload.extend_from_slice(value.as_bytes());

    let mut packet = BytesMut::new();
    packet.put_u8(b'D');
    packet.put_i32((payload.len() + 4) as i32);
    packet.extend_from_slice(&payload);
    packet.to_vec()
}
