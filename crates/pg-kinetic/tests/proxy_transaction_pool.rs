use std::net::SocketAddr;
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
async fn proxy_accepts_two_clients_with_one_backend_capacity() {
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await.expect("accept backend");
            tokio::spawn(async move {
                let mut startup = [0_u8; 1024];
                let _ = stream.read(&mut startup).await.expect("read startup");
                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("auth ready");

                loop {
                    let mut query = [0_u8; 1024];
                    let read = stream.read(&mut query).await.expect("read query");
                    if read == 0 {
                        break;
                    }

                    stream
                        .write_all(&select_one_ready())
                        .await
                        .expect("query ready");
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
            max_checkout_waiters: 2,
        },
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            recovery_mode: pg_kinetic::recovery::RecoveryMode::Recover,
            recovery_timeout_ms: 5_000,
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
    time::sleep(Duration::from_millis(25)).await;

    let first = run_fake_client(listen_addr).await;
    let second = run_fake_client(listen_addr).await;

    assert!(first.windows(6).any(|bytes| bytes == b"SELECT"));
    assert!(second.windows(6).any(|bytes| bytes == b"SELECT"));
}

async fn run_fake_client(addr: SocketAddr) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet())
        .await
        .expect("write startup");

    let mut auth = [0_u8; 128];
    let _ = stream.read(&mut auth).await.expect("read auth");

    stream
        .write_all(&query_packet("select 1"))
        .await
        .expect("write query");

    let mut response = vec![0_u8; 1024];
    let read = stream.read(&mut response).await.expect("read response");
    response.truncate(read);
    response
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
