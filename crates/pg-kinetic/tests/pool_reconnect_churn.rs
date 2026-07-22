use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        CapacityConfig, Config, ConnectionConfig, ObservabilityConfig, PerformanceConfig, QosConfig,
    },
    proxy::Proxy,
    wire::protocol::{FrontendTag, ProtocolVersion},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[tokio::test]
async fn reconnects_with_different_source_ports_reuse_a_bounded_backend_pool() {
    let (backend_addr, backend_sessions) = start_backend().await;
    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("proxy addr");
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
            max_backends: 8,
            max_checkout_waiters: 8,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 1_000,
            pool_mode: Default::default(),
            recovery_mode: pg_kinetic::recovery::RecoveryMode::Recover,
            recovery_timeout_ms: 5_000,
            backend_reset_query: "DISCARD ALL".to_owned(),
        },
        qos: QosConfig {
            max_route_in_flight: 8,
            max_route_waiters: 8,
            query_timeout_ms: 30_000,
            idle_client_timeout_ms: 300_000,
            idle_transaction_timeout_ms: 60_000,
            max_client_buffer_bytes: 1_048_576,
            max_backend_buffer_bytes: 4_194_304,
            overload_error_code: "53300".to_owned(),
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

    for _ in 0..80 {
        connect_and_execute(listen_addr).await;
    }

    assert!(backend_sessions.load(Ordering::Relaxed) <= 8);
}

async fn start_backend() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let address = listener.local_addr().expect("backend addr");
    let sessions = Arc::new(AtomicUsize::new(0));
    let session_counter = Arc::clone(&sessions);

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept backend");
            session_counter.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let mut startup = [0_u8; 1024];
                if stream.read(&mut startup).await.expect("read startup") == 0 {
                    return;
                }
                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("write startup response");

                loop {
                    let mut query = [0_u8; 4096];
                    let read = stream.read(&mut query).await.expect("read query");
                    if read == 0 {
                        return;
                    }
                    stream
                        .write_all(&select_one_ready())
                        .await
                        .expect("write query response");
                }
            });
        }
    });

    (address, sessions)
}

async fn connect_and_execute(address: std::net::SocketAddr) {
    let mut stream = TcpStream::connect(address).await.expect("connect proxy");
    stream
        .write_all(&startup_packet())
        .await
        .expect("write startup");

    let mut auth = [0_u8; 256];
    stream.read(&mut auth).await.expect("read auth");
    stream
        .write_all(&query_packet("select 1"))
        .await
        .expect("write query");

    let mut response = [0_u8; 4096];
    stream.read(&mut response).await.expect("read response");
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

fn select_one_ready() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'T');
    bytes.put_i32(33);
    bytes.put_i16(1);
    bytes.extend_from_slice(b"?column?\0");
    bytes.put_i32(0);
    bytes.put_i16(0);
    bytes.put_i32(23);
    bytes.put_i16(4);
    bytes.put_i32(-1);
    bytes.put_i16(0);
    bytes.put_u8(b'D');
    bytes.put_i32(11);
    bytes.put_i16(1);
    bytes.put_i32(1);
    bytes.extend_from_slice(b"1");
    bytes.put_u8(b'C');
    bytes.put_i32(13);
    bytes.extend_from_slice(b"SELECT 1\0");
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}
