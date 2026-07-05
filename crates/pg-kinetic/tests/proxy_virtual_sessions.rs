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
        message::parse_simple_query,
        protocol::{FrontendTag, ProtocolVersion},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
};

#[tokio::test]
async fn set_application_name_is_replayed_before_next_query() {
    let (proxy_addr, mut seen) = spawn_proxy_with_recording_backend().await;

    run_client_queries(proxy_addr, &["set application_name = 'api'", "select 1"]).await;

    let queries = collect_queries(&mut seen).await;
    let client_set = queries
        .iter()
        .position(|query| query == "set application_name = 'api'")
        .expect("client set query recorded");
    let reset = queries
        .iter()
        .position(|query| query == "DISCARD ALL")
        .expect("reset query recorded");
    let replay = queries
        .iter()
        .position(|query| query == "SET application_name = 'api'")
        .expect("replayed set query recorded");
    let select = queries
        .iter()
        .position(|query| query == "select 1")
        .expect("select query recorded");

    assert!(client_set < reset);
    assert!(reset < replay);
    assert!(replay < select);
}

#[tokio::test]
async fn create_temp_table_keeps_session_pinned_until_discard_temp() {
    let (proxy_addr, mut seen) = spawn_proxy_with_recording_backend().await;

    run_client_queries(
        proxy_addr,
        &[
            "create temporary table t(id int)",
            "select 1",
            "discard temp",
            "select 1",
        ],
    )
    .await;

    let queries = collect_queries(&mut seen).await;
    let create_temp = queries
        .iter()
        .position(|query| query == "create temporary table t(id int)")
        .expect("create temp query recorded");
    let discard_temp = queries
        .iter()
        .position(|query| query == "discard temp")
        .expect("discard temp query recorded");
    let second_select = queries
        .iter()
        .rposition(|query| query == "select 1")
        .expect("second select recorded");

    assert!(create_temp < discard_temp);
    assert!(discard_temp < second_select);
    assert!(!queries.iter().any(|query| query == "DISCARD ALL"));
}

async fn spawn_proxy_with_recording_backend() -> (SocketAddr, mpsc::Receiver<String>) {
    let (sender, receiver) = mpsc::channel(64);
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
                    return;
                }

                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("auth ready");

                let mut buffer = BytesMut::with_capacity(4096);
                loop {
                    let read = stream.read_buf(&mut buffer).await.expect("read query");
                    if read == 0 {
                        return;
                    }

                    while let Some(frame) = parse_frontend_frame(&mut buffer).expect("parse frame")
                    {
                        if frame.tag != b'Q' {
                            continue;
                        }

                        let query = parse_simple_query(&frame)
                            .expect("parse query")
                            .unwrap_or("");
                        sender.send(query.to_string()).await.expect("send query");
                        stream
                            .write_all(&ready_idle())
                            .await
                            .expect("write query response");
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
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 1,
            max_checkout_waiters: 4,
        },
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
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
        observability: ObservabilityConfig { metrics_addr: None },
    };

    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });
    time::sleep(Duration::from_millis(100)).await;

    (listen_addr, receiver)
}

async fn run_client_queries(addr: SocketAddr, queries: &[&str]) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut startup_response = [0_u8; 128];
    let _ = stream
        .read(&mut startup_response)
        .await
        .expect("startup response");

    for query in queries {
        stream.write_all(&query_packet(query)).await.expect("query");
        let mut response = [0_u8; 256];
        let _ = stream.read(&mut response).await.expect("response");
    }
}

async fn collect_queries(receiver: &mut mpsc::Receiver<String>) -> Vec<String> {
    let mut queries = Vec::new();
    while let Ok(Some(query)) = time::timeout(Duration::from_millis(100), receiver.recv()).await {
        queries.push(query);
    }
    queries
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
