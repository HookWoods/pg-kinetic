//! `COPY ... FROM STDIN` drives the connection in the opposite direction from a
//! normal query: after the backend answers `CopyInResponse`, the *client* is the
//! one that must send data before the backend will produce `ReadyForQuery`.
//!
//! A forwarding loop that reads the backend to completion before looking at the
//! client again cannot service that, so these tests pin the duplex behaviour.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use pg_kinetic::{config::Config, proxy::Proxy, wire::protocol::ProtocolVersion};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[tokio::test]
async fn copy_from_stdin_forwards_client_rows_to_the_backend() {
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await.expect("accept backend");
            tokio::spawn(async move {
                let mut buffer = [0_u8; 4096];
                let _ = stream.read(&mut buffer).await.expect("read startup");
                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("write auth ok");

                loop {
                    let read = stream.read(&mut buffer).await.expect("read frontend");
                    if read == 0 {
                        return;
                    }
                    if !buffer[..read].windows(4).any(|w| w == b"COPY") {
                        stream
                            .write_all(&command_complete_ready("SELECT 1"))
                            .await
                            .expect("write query response");
                        continue;
                    }

                    // The backend now waits for the client's rows.
                    stream
                        .write_all(&copy_in_response())
                        .await
                        .expect("write CopyInResponse");

                    let mut copied = BytesMut::new();
                    while !copied.windows(5).any(|w| w == b"c\0\0\0\x04") {
                        let read = stream.read(&mut buffer).await.expect("read copy data");
                        if read == 0 {
                            return;
                        }
                        copied.extend_from_slice(&buffer[..read]);
                    }
                    assert!(
                        copied.windows(4).any(|w| w == b"1\tvi"),
                        "backend should receive the client's CopyData payload"
                    );
                    stream
                        .write_all(&command_complete_ready("COPY 1"))
                        .await
                        .expect("write copy completion");
                }
            });
        }
    });

    let mut client = start_proxy_and_connect(backend_addr).await;

    client
        .write_all(&simple_query("COPY items FROM STDIN"))
        .await
        .expect("write COPY");
    read_until_tag(&mut client, b'G')
        .await
        .expect("proxy should forward CopyInResponse to the client");

    client
        .write_all(&copy_data(b"1\tvienna\n"))
        .await
        .expect("write CopyData");
    client
        .write_all(&copy_done())
        .await
        .expect("write CopyDone");

    // Without duplex forwarding the proxy is blocked reading the backend while the
    // backend is blocked waiting for these rows, so ReadyForQuery never arrives.
    read_until_tag(&mut client, b'Z')
        .await
        .expect("COPY FROM STDIN should complete instead of deadlocking");
}

#[tokio::test]
async fn async_backend_notification_reaches_an_idle_client() {
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await.expect("accept backend");
            tokio::spawn(async move {
                let mut buffer = [0_u8; 4096];
                let _ = stream.read(&mut buffer).await.expect("read startup");
                stream
                    .write_all(&auth_ok_ready())
                    .await
                    .expect("write auth ok");

                let read = stream.read(&mut buffer).await.expect("read listen");
                if read == 0 {
                    return;
                }
                stream
                    .write_all(&command_complete_ready("LISTEN"))
                    .await
                    .expect("write listen response");

                // A NOTIFY from another session: unsolicited, with no request in
                // flight for it to ride along with.
                time::sleep(Duration::from_millis(50)).await;
                stream
                    .write_all(&notification_response("items", "row-added"))
                    .await
                    .expect("write notification");
                time::sleep(Duration::from_secs(5)).await;
            });
        }
    });

    let mut client = start_proxy_and_connect(backend_addr).await;
    client
        .write_all(&simple_query("LISTEN items"))
        .await
        .expect("write LISTEN");
    read_until_tag(&mut client, b'Z')
        .await
        .expect("LISTEN should complete");

    read_until_tag(&mut client, b'A')
        .await
        .expect("an asynchronous NotificationResponse should reach the idle client");
}

async fn start_proxy_and_connect(backend_addr: SocketAddr) -> TcpStream {
    let probe = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
    let listen_addr = probe.local_addr().expect("listen addr");
    drop(probe);

    let mut config = Config::default();
    config.connection.listen_addr = listen_addr;
    config.connection.backend_addr = backend_addr;
    config.capacity.max_clients = 4;
    config.capacity.max_backends = 4;
    config.pool_lifecycle.max_size = 4;
    config.qos.query_timeout_ms = 2_000;

    tokio::spawn(async move {
        let _ = Proxy::new(config).run().await;
    });
    time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(listen_addr)
        .await
        .expect("connect proxy");
    stream
        .write_all(&startup_packet())
        .await
        .expect("write startup");
    read_until_tag(&mut stream, b'Z')
        .await
        .expect("startup should reach ReadyForQuery");
    stream
}

/// Reads until `tag` appears at a frame boundary, or gives up. The timeout is the
/// assertion: a deadlocked proxy simply never answers.
async fn read_until_tag(stream: &mut TcpStream, tag: u8) -> Result<BytesMut, &'static str> {
    let mut seen = BytesMut::new();
    let deadline = time::Instant::now() + Duration::from_secs(3);
    loop {
        let mut buffer = [0_u8; 4096];
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            return Err("timed out waiting for tag");
        }
        match time::timeout(remaining, stream.read(&mut buffer)).await {
            Ok(Ok(0)) => return Err("connection closed"),
            Ok(Ok(read)) => {
                seen.extend_from_slice(&buffer[..read]);
                if frame_tags(&seen).contains(&tag) {
                    return Ok(seen);
                }
            }
            Ok(Err(_)) => return Err("read error"),
            Err(_) => return Err("timed out waiting for tag"),
        }
    }
}

/// Walks the tag/length framing so a tag byte inside a payload is not mistaken
/// for a message of that type.
fn frame_tags(bytes: &[u8]) -> Vec<u8> {
    let mut tags = Vec::new();
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let length = i32::from_be_bytes([
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
        ]);
        if length < 4 {
            break;
        }
        tags.push(bytes[offset]);
        offset += 1 + length as usize;
    }
    tags
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

fn simple_query(sql: &str) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'Q');
    bytes.put_i32((sql.len() + 5) as i32);
    bytes.extend_from_slice(sql.as_bytes());
    bytes.put_u8(0);
    bytes.to_vec()
}

/// `CopyInResponse`: textual format, zero columns.
fn copy_in_response() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'G');
    bytes.put_i32(7);
    bytes.put_u8(0);
    bytes.put_i16(0);
    bytes.to_vec()
}

fn copy_data(payload: &[u8]) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'd');
    bytes.put_i32((payload.len() + 4) as i32);
    bytes.extend_from_slice(payload);
    bytes.to_vec()
}

fn copy_done() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'c');
    bytes.put_i32(4);
    bytes.to_vec()
}

fn command_complete_ready(tag: &str) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'C');
    bytes.put_i32((tag.len() + 5) as i32);
    bytes.extend_from_slice(tag.as_bytes());
    bytes.put_u8(0);
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes.to_vec()
}

fn notification_response(channel: &str, payload: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(4242);
    body.extend_from_slice(channel.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(payload.as_bytes());
    body.put_u8(0);

    let mut bytes = BytesMut::new();
    bytes.put_u8(b'A');
    bytes.put_i32((body.len() + 4) as i32);
    bytes.extend_from_slice(&body);
    bytes.to_vec()
}
