use std::{net::SocketAddr, time::Duration};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{Config, RouteConfig},
    proxy::Proxy,
    wire::{
        backend::{parse_backend_frame, ReadyStatus},
        protocol::{ProtocolVersion, CANCEL_REQUEST_CODE},
        startup::{parse_startup_packet, StartupPacket},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
};

#[tokio::test]
async fn cancel_request_reaches_the_bound_backend() {
    let backend_key = (4321, 8765);
    let (proxy_addr, mut stub) = spawn_proxy_with_cancel_recording_stub(backend_key).await;

    let (mut client, client_key) = connect_and_capture_backend_key_data(proxy_addr).await;
    send_simple_query_without_waiting(&mut client, "select pg_sleep(5)").await;
    stub.wait_for_query().await;

    send_cancel_request(proxy_addr, client_key).await;

    let received = stub.wait_for_cancel(Duration::from_secs(2)).await;
    assert_eq!(received, backend_key);
}

#[tokio::test]
async fn cancel_with_unknown_key_is_dropped_silently() {
    let (proxy_addr, mut stub) = spawn_proxy_with_cancel_recording_stub((4321, 8765)).await;

    send_cancel_request(proxy_addr, (12345, 67890)).await;

    assert!(stub.no_cancel_received(Duration::from_millis(500)).await);
}

struct CancelStub {
    query_rx: mpsc::Receiver<()>,
    cancel_rx: mpsc::Receiver<(i32, i32)>,
}

impl CancelStub {
    async fn wait_for_query(&mut self) {
        time::timeout(Duration::from_secs(2), self.query_rx.recv())
            .await
            .expect("query forwarded")
            .expect("query event");
    }

    async fn wait_for_cancel(&mut self, timeout: Duration) -> (i32, i32) {
        time::timeout(timeout, self.cancel_rx.recv())
            .await
            .expect("cancel forwarded")
            .expect("cancel event")
    }

    async fn no_cancel_received(&mut self, timeout: Duration) -> bool {
        time::timeout(timeout, self.cancel_rx.recv()).await.is_err()
    }
}

async fn spawn_proxy_with_cancel_recording_stub(
    backend_key: (i32, i32),
) -> (SocketAddr, CancelStub) {
    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend_listener.local_addr().expect("backend addr");
    let (query_tx, query_rx) = mpsc::channel(1);
    let (cancel_tx, cancel_rx) = mpsc::channel(1);

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend_listener.accept().await.expect("accept backend");
            let query_tx = query_tx.clone();
            let cancel_tx = cancel_tx.clone();
            tokio::spawn(async move {
                let Some(packet) = read_startup_packet(&mut stream).await else {
                    return;
                };
                match parse_startup_packet(&packet).expect("parse backend startup packet") {
                    StartupPacket::Startup { .. } => {
                        let mut response = auth_ok_frame();
                        response.extend_from_slice(&backend_key_data_frame(
                            backend_key.0,
                            backend_key.1,
                        ));
                        response.extend_from_slice(&ready_for_query_idle());
                        stream
                            .write_all(&response)
                            .await
                            .expect("write startup response");

                        let mut query = [0_u8; 1024];
                        if stream.read(&mut query).await.unwrap_or(0) > 0 {
                            let _ = query_tx.send(()).await;
                            let mut drain = [0_u8; 1024];
                            loop {
                                match stream.read(&mut drain).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(_) => {}
                                }
                            }
                        }
                    }
                    StartupPacket::CancelRequest {
                        process_id,
                        secret_key,
                    } => {
                        let _ = cancel_tx.send((process_id, secret_key)).await;
                    }
                    StartupPacket::SslRequest | StartupPacket::GssEncRequest => {}
                }
            });
        }
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    drop(proxy_listener);

    let mut config = Config::default();
    config.connection.listen_addr = proxy_addr;
    config.connection.backend_addr = backend_addr;
    config.routes = vec![RouteConfig::from_backend_addr(backend_addr)];
    config.capacity.max_backends = 1;
    config.pool_lifecycle.max_size = 1;

    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });
    time::sleep(Duration::from_millis(50)).await;

    (
        proxy_addr,
        CancelStub {
            query_rx,
            cancel_rx,
        },
    )
}

async fn connect_and_capture_backend_key_data(addr: SocketAddr) -> (TcpStream, (i32, i32)) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet("postgres"))
        .await
        .expect("startup");

    let mut buffer = BytesMut::new();
    let mut key = None;
    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            if frame.tag == b'K' && frame.payload.len() == 8 {
                key = Some((
                    i32::from_be_bytes(frame.payload[0..4].try_into().expect("pid bytes")),
                    i32::from_be_bytes(frame.payload[4..8].try_into().expect("secret bytes")),
                ));
            }
            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return (stream, key.expect("backend key data"));
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

async fn send_simple_query_without_waiting(stream: &mut TcpStream, sql: &str) {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'Q');
    bytes.put_i32((sql.len() + 5) as i32);
    bytes.extend_from_slice(sql.as_bytes());
    bytes.put_u8(0);
    stream.write_all(&bytes).await.expect("send query");
}

async fn send_cancel_request(addr: SocketAddr, key: (i32, i32)) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    let mut packet = BytesMut::new();
    packet.put_i32(16);
    packet.put_i32(CANCEL_REQUEST_CODE);
    packet.put_i32(key.0);
    packet.put_i32(key.1);
    stream.write_all(&packet).await.expect("send cancel");
}

async fn read_startup_packet(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).await.ok()?;
    let len = i32::from_be_bytes(len);
    if len < 4 {
        return None;
    }

    let mut packet = vec![0_u8; len as usize];
    packet[..4].copy_from_slice(&len.to_be_bytes());
    stream.read_exact(&mut packet[4..]).await.ok()?;
    Some(packet)
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

fn auth_ok_frame() -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'R');
    bytes.put_i32(8);
    bytes.put_i32(0);
    bytes
}

fn backend_key_data_frame(process_id: i32, secret_key: i32) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'K');
    bytes.put_i32(12);
    bytes.put_i32(process_id);
    bytes.put_i32(secret_key);
    bytes
}

fn ready_for_query_idle() -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes
}
