use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        CapacityConfig, Config, ConnectionConfig, ObservabilityConfig, PerformanceConfig, PoolMode,
        QosConfig,
    },
    proxy::Proxy,
    wire::{
        backend::{parse_backend_frame, ReadyStatus},
        protocol::{FrontendTag, ProtocolVersion},
    },
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
        routes: Vec::new(),
        pools: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 1,
            max_checkout_waiters: 2,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            pool_mode: Default::default(),
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

    let first = run_fake_client(listen_addr).await;
    let second = run_fake_client(listen_addr).await;

    assert!(first.windows(6).any(|bytes| bytes == b"SELECT"));
    assert!(second.windows(6).any(|bytes| bytes == b"SELECT"));
}

#[tokio::test]
async fn session_mode_gives_each_client_a_dedicated_backend() {
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        let mut next_backend_id = 1_usize;
        loop {
            let (mut stream, _) = backend.accept().await.expect("accept backend");
            let backend_id = next_backend_id;
            next_backend_id += 1;
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
                        .write_all(&select_value_ready(&backend_id.to_string()))
                        .await
                        .expect("query ready");
                }
            });
        }
    });

    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let mut config = Config::default();
    config.connection.listen_addr = listen_addr;
    config.connection.backend_addr = backend_addr;
    config.capacity.max_clients = 10;
    config.capacity.max_backends = 4;
    config.pool_lifecycle.max_size = 4;
    config.performance.pool_mode = PoolMode::Session;

    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });
    time::sleep(Duration::from_millis(25)).await;

    let mut first = connect_fake_client(listen_addr).await;
    let mut second = connect_fake_client(listen_addr).await;
    let first_backend = query_backend_id(&mut first).await;
    let second_backend = query_backend_id(&mut second).await;

    assert_ne!(first_backend, second_backend);
    assert_eq!(first_backend, query_backend_id(&mut first).await);
    assert_eq!(second_backend, query_backend_id(&mut second).await);
}

async fn run_fake_client(addr: SocketAddr) -> Vec<u8> {
    let mut stream = connect_fake_client(addr).await;

    stream
        .write_all(&query_packet("select 1"))
        .await
        .expect("write query");

    let mut buffer = BytesMut::new();
    let mut response = Vec::new();
    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse response") {
            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return response;
            }
        }

        let mut chunk = [0_u8; 1024];
        let read = time::timeout(Duration::from_secs(1), stream.read(&mut chunk))
            .await
            .expect("read response")
            .expect("read response");
        assert!(read > 0, "proxy closed before ReadyForQuery");
        response.extend_from_slice(&chunk[..read]);
        buffer.extend_from_slice(&chunk[..read]);
    }
}

async fn connect_fake_client(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet())
        .await
        .expect("write startup");

    read_until_ready(&mut stream).await;
    stream
}

async fn query_backend_id(stream: &mut TcpStream) -> usize {
    stream
        .write_all(&query_packet("select backend_id"))
        .await
        .expect("write query");

    read_single_data_row_value(stream)
        .await
        .parse()
        .expect("backend id")
}

async fn read_until_ready(stream: &mut TcpStream) {
    let mut buffer = BytesMut::new();
    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return;
            }
        }

        let mut chunk = [0_u8; 4096];
        let read = time::timeout(Duration::from_secs(1), stream.read(&mut chunk))
            .await
            .expect("read proxy response")
            .expect("read proxy response");
        assert!(read > 0, "proxy closed before ReadyForQuery");
        buffer.extend_from_slice(&chunk[..read]);
    }
}

async fn read_single_data_row_value(stream: &mut TcpStream) -> String {
    let mut buffer = BytesMut::new();
    let mut value = None;
    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            if frame.tag == b'D' {
                let payload = frame.payload.as_ref();
                assert_eq!(&payload[0..2], &[0, 1]);
                let len = i32::from_be_bytes(payload[2..6].try_into().expect("value len"));
                assert!(len >= 0);
                value = Some(
                    std::str::from_utf8(&payload[6..6 + len as usize])
                        .expect("utf8 value")
                        .to_owned(),
                );
            }
            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return value.expect("data row");
            }
        }

        let mut chunk = [0_u8; 4096];
        let read = time::timeout(Duration::from_secs(1), stream.read(&mut chunk))
            .await
            .expect("read proxy response")
            .expect("read proxy response");
        assert!(read > 0, "proxy closed before ReadyForQuery");
        buffer.extend_from_slice(&chunk[..read]);
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

fn select_one_ready() -> Vec<u8> {
    select_value_ready("1")
}

fn select_value_ready(value: &str) -> Vec<u8> {
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
    bytes.put_i32((value.len() + 10) as i32);
    bytes.put_i16(1);
    bytes.put_i32(value.len() as i32);
    bytes.extend_from_slice(value.as_bytes());
    bytes.put_u8(b'C');
    bytes.put_i32(13);
    bytes.extend_from_slice(b"SELECT 1\0");
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}
