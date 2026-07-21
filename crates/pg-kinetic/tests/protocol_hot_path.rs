use std::{net::SocketAddr, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use pg_kinetic::wire::{
    backend::parse_backend_frame,
    error::WireError,
    frame::{parse_frontend_frame, FrontendFrame},
    message::parse_simple_query,
    rewrite::encode_frontend_frame,
};
use pg_kinetic::{
    config::{
        CapacityConfig, Config, ConnectionConfig, ObservabilityConfig, PerformanceConfig, QosConfig,
    },
    proxy::Proxy,
    wire::protocol::{FrontendTag, ProtocolVersion},
};
use pg_kinetic_proxy::buffers::{
    BufferReusePolicy, OversizedBufferPolicy, ProxyBufferPool, ProxyBufferStats,
};
use pretty_assertions::assert_eq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

const FRAME_HEADER_LEN: usize = 5;

fn wire_frame(tag: u8, payload: &[u8]) -> BytesMut {
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.put_u8(tag);
    frame.put_i32((payload.len() + 4) as i32);
    frame.extend_from_slice(payload);
    frame
}

#[test]
fn frontend_parser_reuses_the_input_allocation_for_common_headers() {
    let mut input = wire_frame(b'Q', b"select 1\0");
    let payload_start = input.as_ptr().wrapping_add(FRAME_HEADER_LEN);

    let frame = parse_frontend_frame(&mut input)
        .expect("frame parses")
        .expect("frame is complete");

    assert_eq!(frame.payload.as_ptr(), payload_start);
    assert_eq!(frame.payload, Bytes::from_static(b"select 1\0"));
    assert!(input.is_empty());
}

#[test]
fn backend_parser_uses_typed_errors_for_invalid_lengths() {
    let mut input = BytesMut::from(&b"Z\0\0\0\x03"[..]);

    let error = parse_backend_frame(&mut input).expect_err("short length is rejected");

    assert!(matches!(error, WireError::InvalidBackendFrameLength(3)));
    assert_eq!(&input[..], &b"Z\0\0\0\x03"[..]);
}

#[test]
fn simple_query_forwarding_preserves_wire_bytes() {
    let expected = wire_frame(b'Q', b"select 1\0");
    let mut input = expected.clone();

    let frame = parse_frontend_frame(&mut input)
        .expect("simple query parses")
        .expect("simple query is complete");

    assert_eq!(
        parse_simple_query(&frame).expect("query text parses"),
        Some("select 1")
    );
    assert_eq!(&encode_frontend_frame(&frame)[..], &expected[..]);
}

#[test]
fn extended_query_forwarding_preserves_each_wire_frame() {
    let frames = [
        wire_frame(b'P', b"statement\0select $1::int\0\0\x01\0\0\0\x17"),
        wire_frame(b'B', b"\0statement\0\0\0\0\0\0\0"),
        wire_frame(b'E', b"\0\0\0\0"),
        wire_frame(b'S', b""),
    ];
    let expected = frames.concat();
    let mut input = BytesMut::from(expected.as_slice());
    let mut forwarded = BytesMut::new();

    while let Some(frame) = parse_frontend_frame(&mut input).expect("extended frame parses") {
        forwarded.extend_from_slice(&encode_frontend_frame(&frame));
    }

    assert_eq!(&forwarded[..], expected.as_slice());
}

#[test]
fn reused_buffer_does_not_expose_prior_frame_bytes() {
    let mut input = BytesMut::with_capacity(64);
    input.extend_from_slice(&wire_frame(b'Q', b"select first\0"));
    let first = parse_frontend_frame(&mut input)
        .expect("first frame parses")
        .expect("first frame is complete");
    assert_eq!(first.payload, Bytes::from_static(b"select first\0"));
    assert!(input.is_empty());

    input.extend_from_slice(&wire_frame(b'Q', b"select second\0"));
    let second = parse_frontend_frame(&mut input)
        .expect("second frame parses")
        .expect("second frame is complete");

    assert_eq!(second.payload, Bytes::from_static(b"select second\0"));
    assert!(!second
        .payload
        .windows(b"first".len())
        .any(|bytes| bytes == b"first"));
}

#[test]
fn malformed_frames_return_safe_protocol_errors() {
    let mut invalid_length = BytesMut::from(&b"Q\0\0\0\x03"[..]);
    let error = parse_frontend_frame(&mut invalid_length).expect_err("short length is rejected");
    assert!(matches!(error, WireError::InvalidFrameLength(3)));

    let malformed_query = FrontendFrame {
        tag: b'Q',
        payload: Bytes::from_static(b"select 1"),
    };
    let error = parse_simple_query(&malformed_query).expect_err("unterminated query is rejected");
    assert!(matches!(error, WireError::IncompleteFrame));
}

#[test]
fn common_forwarding_reuses_session_write_buffers() {
    let pool = ProxyBufferPool::default();
    let initial = pool.stats();
    let mut lease = pool.acquire();
    let buffers = lease.buffers_mut();

    buffers.append_frontend_frame(b'Q', b"select 1\0");
    assert_eq!(
        buffers.backend_write(),
        wire_frame(b'Q', b"select 1\0").as_ref()
    );
    buffers.clear_backend_write();
    buffers.append_frontend_frame(b'Q', b"select 2\0");

    let current = pool.stats();
    assert_eq!(current.allocations, initial.allocations + 1);
    assert_eq!(current.copies, initial.copies + 2);
    assert_eq!(current.copied_bytes, initial.copied_bytes + 18);
}

#[test]
fn oversized_session_buffers_are_trimmed_before_reuse() {
    let pool = ProxyBufferPool::new(
        BufferReusePolicy {
            initial_capacity: 64,
            max_cached_sessions: 1,
        },
        OversizedBufferPolicy {
            max_retained_capacity: 128,
        },
    );
    {
        let mut lease = pool.acquire();
        let buffers = lease.buffers_mut();
        buffers.append_backend_frame(b'D', &[0; 1024]);
        buffers.clear_client_write();
    }

    let mut lease = pool.acquire();
    assert!(lease
        .buffers_mut()
        .capacities()
        .into_iter()
        .all(|capacity| capacity <= 128));
    assert_eq!(pool.stats().oversized_buffers_released, 1);
}

#[test]
fn buffer_stats_track_copies_and_growth() {
    let pool = ProxyBufferPool::new(
        BufferReusePolicy {
            initial_capacity: 8,
            max_cached_sessions: 1,
        },
        OversizedBufferPolicy::default(),
    );
    let mut lease = pool.acquire();
    let buffers = lease.buffers_mut();
    buffers.append_backend_frame(b'D', &[0; 64]);

    let stats = pool.stats();
    assert_eq!(stats.copies, 1);
    assert_eq!(stats.copied_bytes, 64);
    assert_eq!(stats.allocations, 1);
    assert!(stats.allocation_bytes >= 69);
}

#[tokio::test]
async fn backend_response_forwarding_does_not_copy_payloads() {
    let (forwarded, stats) = run_simple_query_forwarding_with_stats("select 1").await;

    assert_eq!(forwarded, expected_wire_bytes("select 1"));
    assert_eq!(stats.backend_to_client_copied_bytes, 0);
    assert_eq!(stats.backend_to_client_copies, 0);
}

#[test]
fn active_session_buffers_are_isolated() {
    let pool = ProxyBufferPool::default();
    let mut first = pool.acquire();
    first
        .buffers_mut()
        .append_frontend_frame(b'Q', b"select first\0");

    let mut second = pool.acquire();
    second
        .buffers_mut()
        .append_frontend_frame(b'Q', b"select second\0");

    assert_eq!(
        first.buffers_mut().backend_write(),
        wire_frame(b'Q', b"select first\0").as_ref()
    );
    assert_eq!(
        second.buffers_mut().backend_write(),
        wire_frame(b'Q', b"select second\0").as_ref()
    );
    assert_eq!(pool.stats().sessions_created, 2);
}

async fn run_simple_query_forwarding_with_stats(sql: &str) -> (Vec<u8>, ProxyBufferStats) {
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.expect("accept backend");

        let mut startup = [0_u8; 1024];
        let _ = stream.read(&mut startup).await.expect("read startup");
        stream
            .write_all(&auth_ok_ready())
            .await
            .expect("auth ready");

        let mut query = [0_u8; 1024];
        let _ = stream.read(&mut query).await.expect("read query");
        stream
            .write_all(&select_one_ready())
            .await
            .expect("query ready");
    });

    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let buffer_pool = ProxyBufferPool::default();
    let proxy =
        Proxy::with_buffer_pool(proxy_config(listen_addr, backend_addr), buffer_pool.clone());
    tokio::spawn(async move {
        let _ = proxy.run().await;
    });
    time::sleep(Duration::from_millis(25)).await;

    let mut stream = TcpStream::connect(listen_addr)
        .await
        .expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    let mut auth = vec![0_u8; auth_ok_ready().len()];
    stream
        .read_exact(&mut auth)
        .await
        .expect("read auth response");
    assert_eq!(auth, auth_ok_ready());

    stream.write_all(&query_packet(sql)).await.expect("query");

    let mut forwarded = vec![0_u8; select_one_ready().len()];
    stream
        .read_exact(&mut forwarded)
        .await
        .expect("read query response");

    (forwarded, buffer_pool.stats())
}

fn proxy_config(listen_addr: SocketAddr, backend_addr: SocketAddr) -> Config {
    Config {
        connection: ConnectionConfig {
            listen_addr,
            backend_addr,
        },
        routes: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 1,
            max_checkout_waiters: 2,
        },
        pool_lifecycle: Default::default(),
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

fn expected_wire_bytes(_sql: &str) -> Vec<u8> {
    select_one_ready()
}
