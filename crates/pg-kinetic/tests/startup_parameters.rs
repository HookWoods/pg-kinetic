use std::{collections::HashMap, net::SocketAddr, time::Duration};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{Config, RouteConfig},
    proxy::Proxy,
    wire::{
        backend::{parse_backend_frame, ReadyStatus},
        protocol::{BackendTag, ProtocolVersion},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

#[tokio::test]
async fn reused_pooled_connection_receives_parameter_status() {
    let proxy_addr = spawn_proxy_with_parameter_status_backend(&[
        ("server_version", "16.4"),
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
    ])
    .await;

    let params = connect_and_collect_parameter_status(proxy_addr, "postgres").await;
    assert_eq!(
        params.get("server_version").map(String::as_str),
        Some("16.4")
    );

    let params = connect_and_collect_parameter_status(proxy_addr, "postgres").await;
    assert_eq!(
        params.get("server_version").map(String::as_str),
        Some("16.4")
    );
    assert_eq!(
        params.get("client_encoding").map(String::as_str),
        Some("UTF8")
    );
    assert_eq!(
        params
            .get("standard_conforming_strings")
            .map(String::as_str),
        Some("on")
    );
}

async fn spawn_proxy_with_parameter_status_backend(params: &[(&str, &str)]) -> SocketAddr {
    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend_listener.local_addr().expect("backend addr");
    let params = params
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect::<Vec<_>>();

    tokio::spawn(async move {
        let (mut stream, _) = backend_listener.accept().await.expect("accept backend");
        let mut startup = [0_u8; 1024];
        let _ = stream.read(&mut startup).await.expect("read startup");

        let mut response = auth_ok_frame();
        for (name, value) in &params {
            response.extend_from_slice(&parameter_status_frame(name, value));
        }
        response.extend_from_slice(&ready_for_query_idle());
        stream
            .write_all(&response)
            .await
            .expect("write startup response");

        let mut drain = [0_u8; 1024];
        loop {
            match stream.read(&mut drain).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
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

    proxy_addr
}

async fn connect_and_collect_parameter_status(
    addr: SocketAddr,
    user: &str,
) -> HashMap<String, String> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet(user))
        .await
        .expect("startup");

    let mut buffer = BytesMut::new();
    let mut params = HashMap::new();

    loop {
        while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
            if frame.tag == u8::from(BackendTag::ParameterStatus) {
                if let Some((name, value)) = parse_parameter_status_payload(&frame.payload) {
                    params.insert(name, value);
                }
            }

            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return params;
            }
        }

        let mut chunk = [0_u8; 4096];
        match time::timeout(Duration::from_millis(500), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => return params,
            Ok(Ok(read)) => buffer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => panic!("read proxy response: {error}"),
        }
    }
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

fn parse_parameter_status_payload(payload: &[u8]) -> Option<(String, String)> {
    let mut parts = payload.split(|byte| *byte == 0);
    let name = std::str::from_utf8(parts.next()?).ok()?;
    let value = std::str::from_utf8(parts.next()?).ok()?;
    Some((name.to_owned(), value.to_owned()))
}

fn auth_ok_frame() -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::Authentication));
    bytes.put_i32(8);
    bytes.put_i32(0);
    bytes
}

fn parameter_status_frame(name: &str, value: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(name.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(value.as_bytes());
    payload.put_u8(0);

    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::ParameterStatus));
    bytes.put_i32((payload.len() + 4) as i32);
    bytes.extend_from_slice(&payload);
    bytes
}

fn ready_for_query_idle() -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(b'I');
    bytes
}
