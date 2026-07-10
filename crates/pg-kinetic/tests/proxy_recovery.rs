use std::{net::SocketAddr, time::Duration};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        CapacityConfig, Config, ConnectionConfig, ObservabilityConfig, PerformanceConfig, QosConfig,
    },
    proxy::Proxy,
    recovery::RecoveryMode,
    wire::{
        frame::parse_frontend_frame,
        message::{parse_parse_message, parse_simple_query},
        protocol::{FrontendTag, ProtocolVersion, GSSENC_REQUEST_CODE, SSL_REQUEST_CODE},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
};

#[tokio::test]
async fn client_disconnect_after_begin_rolls_back_before_release() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Recover, 1_000, StreamBehavior::RespondToSync).await;

    run_client_query_and_read_response(proxy_addr, "begin").await;

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event == "begin"));
    assert!(events.iter().any(|event| event == "ROLLBACK"));
    assert!(!events.iter().any(|event| event == "SYNC"));
}

#[tokio::test]
async fn client_terminate_after_begin_rolls_back_before_release() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Recover, 1_000, StreamBehavior::RespondToSync).await;

    run_client_query_then_terminate(proxy_addr, "begin").await;

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event == "begin"));
    assert!(events.iter().any(|event| event == "ROLLBACK"));
}

#[tokio::test]
async fn client_disconnect_before_startup_does_not_touch_backend() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Recover, 1_000, StreamBehavior::RespondToSync).await;

    connect_and_close(proxy_addr).await;

    let events = collect_events(&mut seen).await;
    assert!(events.is_empty());
}

#[tokio::test]
async fn fragmented_startup_packet_is_accepted() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Recover, 1_000, StreamBehavior::RespondToSync).await;

    run_client_fragmented_startup_query(proxy_addr, "select 1").await;

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event == "select 1"));
}

#[tokio::test]
async fn startup_encryption_negotiation_is_rejected_then_startup_continues() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Recover, 1_000, StreamBehavior::RespondToSync).await;

    run_client_negotiated_startup_query(proxy_addr, "select 1").await;

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event == "select 1"));
}

#[tokio::test]
async fn client_disconnect_during_streamed_response_drains_and_syncs() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Recover, 1_000, StreamBehavior::RespondToSync).await;

    run_client_extended_query_and_drop(proxy_addr, "select stream_rows").await;

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event == "select stream_rows"));
    assert!(events.iter().any(|event| event == "SYNC"));
}

#[tokio::test]
async fn recovery_timeout_discards_uncertain_connection() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Recover, 50, StreamBehavior::IgnoreSync).await;

    run_client_extended_query_and_drop(proxy_addr, "select stream_rows").await;

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event == "select stream_rows"));
    assert!(events.iter().any(|event| event == "SYNC"));
    assert!(events.iter().any(|event| event == "CLOSED"));
}

#[tokio::test]
async fn rollback_only_rolls_back_transactions_but_discards_streams() {
    let (transaction_addr, mut transaction_seen) = spawn_proxy_with_backend(
        RecoveryMode::RollbackOnly,
        1_000,
        StreamBehavior::RespondToSync,
    )
    .await;

    run_client_query_and_read_response(transaction_addr, "begin").await;

    let transaction_events = collect_events(&mut transaction_seen).await;
    assert!(transaction_events.iter().any(|event| event == "ROLLBACK"));

    let (stream_addr, mut stream_seen) = spawn_proxy_with_backend(
        RecoveryMode::RollbackOnly,
        1_000,
        StreamBehavior::RespondToSync,
    )
    .await;

    run_client_extended_query_and_drop(stream_addr, "select stream_rows").await;

    let stream_events = collect_events(&mut stream_seen).await;
    assert!(!stream_events.iter().any(|event| event == "SYNC"));
}

#[tokio::test]
async fn drop_mode_discards_all_recovery_triggers() {
    let (proxy_addr, mut seen) =
        spawn_proxy_with_backend(RecoveryMode::Drop, 1_000, StreamBehavior::RespondToSync).await;

    run_client_query_and_read_response(proxy_addr, "begin").await;
    run_client_extended_query_and_drop(proxy_addr, "select stream_rows").await;

    let events = collect_events(&mut seen).await;
    assert!(!events.iter().any(|event| event == "ROLLBACK"));
    assert!(!events.iter().any(|event| event == "SYNC"));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamBehavior {
    RespondToSync,
    IgnoreSync,
}

async fn spawn_proxy_with_backend(
    recovery_mode: RecoveryMode,
    recovery_timeout_ms: u64,
    stream_behavior: StreamBehavior,
) -> (SocketAddr, mpsc::Receiver<String>) {
    let (sender, receiver) = mpsc::channel(128);
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
                        .send("STARTUP_CLOSED".to_string())
                        .await
                        .expect("send startup close");
                    return;
                }

                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("auth ready");

                let mut buffer = BytesMut::with_capacity(4096);
                let mut pending_stream = false;
                let mut awaiting_recovery_sync = false;

                loop {
                    let read = stream.read_buf(&mut buffer).await.expect("read frontend");
                    if read == 0 {
                        sender.send("CLOSED".to_string()).await.expect("send close");
                        return;
                    }

                    while let Some(frame) =
                        parse_frontend_frame(&mut buffer).expect("parse frontend frame")
                    {
                        if awaiting_recovery_sync && frame.tag == b'S' {
                            sender.send("SYNC".to_string()).await.expect("send sync");
                            if stream_behavior == StreamBehavior::RespondToSync {
                                stream
                                    .write_all(&ready_idle())
                                    .await
                                    .expect("write recovery ready");
                                awaiting_recovery_sync = false;
                            }
                            continue;
                        }

                        match frame.tag {
                            b'Q' => {
                                let query = parse_simple_query(&frame)
                                    .expect("parse query")
                                    .unwrap_or("");
                                sender.send(query.to_string()).await.expect("send query");

                                if query == "begin" {
                                    stream
                                        .write_all(&ready_in_transaction())
                                        .await
                                        .expect("write begin ready");
                                } else {
                                    stream
                                        .write_all(&ready_idle())
                                        .await
                                        .expect("write query ready");
                                }
                            }
                            b'P' => {
                                let parse = parse_parse_message(&frame)
                                    .expect("parse message")
                                    .expect("parse frame");
                                sender
                                    .send(parse.query.clone())
                                    .await
                                    .expect("send parsed query");
                                if parse.query == "select stream_rows" {
                                    pending_stream = true;
                                }
                            }
                            b'S' if pending_stream => {
                                pending_stream = false;
                                stream
                                    .write_all(&data_row("row-1"))
                                    .await
                                    .expect("write row 1");
                                time::sleep(Duration::from_millis(25)).await;
                                stream
                                    .write_all(&data_row("row-2"))
                                    .await
                                    .expect("write row 2");
                                awaiting_recovery_sync = true;
                            }
                            _ => {}
                        }
                    }
                }
            });
        }
    });

    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
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
            checkout_timeout_ms: 100,
            recovery_mode,
            recovery_timeout_ms,
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
    time::sleep(Duration::from_millis(25)).await;

    (listen_addr, receiver)
}

async fn run_client_query_and_read_response(addr: SocketAddr, query: &str) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut startup_response = [0_u8; 128];
    let _ = stream
        .read(&mut startup_response)
        .await
        .expect("startup response");

    stream.write_all(&query_packet(query)).await.expect("query");
    let mut response = [0_u8; 256];
    let _ = stream.read(&mut response).await.expect("query response");
}

async fn run_client_query_then_terminate(addr: SocketAddr, query: &str) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut startup_response = [0_u8; 128];
    let _ = stream
        .read(&mut startup_response)
        .await
        .expect("startup response");

    stream.write_all(&query_packet(query)).await.expect("query");
    let mut response = [0_u8; 256];
    let _ = stream.read(&mut response).await.expect("query response");

    stream
        .write_all(&terminate_packet())
        .await
        .expect("terminate");
}

async fn connect_and_close(addr: SocketAddr) {
    let stream = TcpStream::connect(addr).await.expect("connect proxy");
    drop(stream);
}

async fn run_client_fragmented_startup_query(addr: SocketAddr, query: &str) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    let startup = startup_packet();
    let (prefix, suffix) = startup.split_at(4);
    stream.write_all(prefix).await.expect("startup prefix");
    time::sleep(Duration::from_millis(25)).await;
    stream.write_all(suffix).await.expect("startup suffix");

    let mut startup_response = [0_u8; 128];
    let _ = stream
        .read(&mut startup_response)
        .await
        .expect("startup response");

    stream.write_all(&query_packet(query)).await.expect("query");
    let mut response = [0_u8; 256];
    let _ = stream.read(&mut response).await.expect("query response");
}

async fn run_client_negotiated_startup_query(addr: SocketAddr, query: &str) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");

    stream
        .write_all(&gssenc_request_packet())
        .await
        .expect("gssenc request");
    expect_rejection(&mut stream).await;

    stream
        .write_all(&ssl_request_packet())
        .await
        .expect("ssl request");
    expect_rejection(&mut stream).await;

    stream.write_all(&startup_packet()).await.expect("startup");
    let mut startup_response = [0_u8; 128];
    let _ = stream
        .read(&mut startup_response)
        .await
        .expect("startup response");

    stream.write_all(&query_packet(query)).await.expect("query");
    let mut response = [0_u8; 256];
    let _ = stream.read(&mut response).await.expect("query response");
}

async fn expect_rejection(stream: &mut TcpStream) {
    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .expect("encryption rejection");
    assert_eq!(response, *b"N");
}

async fn run_client_extended_query_and_drop(addr: SocketAddr, query: &str) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut startup_response = [0_u8; 128];
    let _ = stream
        .read(&mut startup_response)
        .await
        .expect("startup response");

    stream
        .write_all(&extended_query_cycle(query))
        .await
        .expect("extended query");
}

async fn collect_events(receiver: &mut mpsc::Receiver<String>) -> Vec<String> {
    let mut events = Vec::new();
    while let Ok(Some(event)) = time::timeout(Duration::from_millis(200), receiver.recv()).await {
        events.push(event);
    }
    events
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

fn gssenc_request_packet() -> Vec<u8> {
    startup_request_packet(GSSENC_REQUEST_CODE)
}

fn ssl_request_packet() -> Vec<u8> {
    startup_request_packet(SSL_REQUEST_CODE)
}

fn startup_request_packet(code: i32) -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_i32(8);
    packet.put_i32(code);
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

fn extended_query_cycle(query: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(parse_frame("stmt", query));
    bytes.extend(bind_frame("portal", "stmt"));
    bytes.extend(execute_frame("portal"));
    bytes.extend(sync_packet());
    bytes
}

fn parse_frame(statement_name: &str, query: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(statement_name.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(query.as_bytes());
    payload.put_u8(0);
    payload.put_i16(0);
    frontend_frame(b'P', payload)
}

fn bind_frame(portal_name: &str, statement_name: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(statement_name.as_bytes());
    payload.put_u8(0);
    payload.put_i16(0);
    payload.put_i16(0);
    payload.put_i16(0);
    frontend_frame(b'B', payload)
}

fn execute_frame(portal_name: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.put_u8(0);
    payload.put_i32(0);
    frontend_frame(b'E', payload)
}

fn sync_packet() -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_u8(b'S');
    packet.put_i32(4);
    packet.to_vec()
}

fn terminate_packet() -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_u8(b'X');
    packet.put_i32(4);
    packet.to_vec()
}

fn frontend_frame(tag: u8, payload: BytesMut) -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_u8(tag);
    packet.put_i32((payload.len() + 4) as i32);
    packet.extend_from_slice(&payload);
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

fn ready_idle() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'C');
    bytes.put_i32(13);
    bytes.extend_from_slice(b"SELECT 1\0");
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}

fn ready_in_transaction() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'C');
    bytes.put_i32(10);
    bytes.extend_from_slice(b"BEGIN\0");
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'T');
    bytes.to_vec()
}

fn data_row(value: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_i16(1);
    payload.put_i32(value.len() as i32);
    payload.extend_from_slice(value.as_bytes());
    frontend_frame(b'D', payload)
}
