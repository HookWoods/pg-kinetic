use std::{
    mem::size_of,
    net::SocketAddr,
    path::PathBuf,
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
    core::{
        performance::{
            DerivedPerformanceMetric, ProcessMetricKind, ProcessMetricSample, ProcessMetricValue,
        },
        session::SessionState,
    },
    proxy::Proxy,
    wire::protocol::ProtocolVersion,
};
use pg_kinetic_proxy::{
    benchmark::validate_benchmark_scenario,
    buffers::{BufferReusePolicy, OversizedBufferPolicy, ProxyBufferPool},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time,
};

struct TestProxy {
    address: SocketAddr,
    _buffer_pool: ProxyBufferPool,
    backend_accepts: Arc<AtomicUsize>,
    snapshots: pg_kinetic_proxy::snapshot::SnapshotStore,
    drain: Arc<pg_kinetic_proxy::drain::DrainController>,
    proxy_task: JoinHandle<()>,
    backend_task: JoinHandle<()>,
}

impl TestProxy {
    async fn shutdown(mut self) {
        self.drain.begin_drain(Duration::from_millis(100));
        time::timeout(Duration::from_secs(1), &mut self.proxy_task)
            .await
            .expect("proxy shuts down")
            .expect("proxy task joins");
        self.backend_task.abort();
    }
}

fn idle_scenario_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates directory")
        .parent()
        .expect("workspace root")
        .join("bench")
        .join("scenarios")
        .join("benchmark-idle-clients.toml")
}

fn test_buffer_pool() -> ProxyBufferPool {
    ProxyBufferPool::new(
        BufferReusePolicy {
            initial_capacity: 64,
            max_cached_sessions: 64,
        },
        OversizedBufferPolicy {
            max_retained_capacity: 128,
        },
    )
}

#[test]
fn idle_session_state_is_compact_and_unpinned() {
    assert!(size_of::<SessionState>() <= 64);
    assert_eq!(SessionState::default().pin_reason(), None);
}

#[test]
fn idle_client_leases_defer_buffer_allocation() {
    let pool = test_buffer_pool();
    let leases: Vec<_> = (0..128).map(|_| pool.acquire()).collect();

    assert_eq!(pool.stats().sessions_created, 128);
    assert_eq!(pool.stats().allocations, 0);
    assert_eq!(pool.stats().allocation_bytes, 0);

    drop(leases);
}

#[test]
fn idle_client_leases_reuse_empty_buffer_sets() {
    let pool = test_buffer_pool();
    let first: Vec<_> = (0..32).map(|_| pool.acquire()).collect();
    drop(first);

    let second: Vec<_> = (0..32).map(|_| pool.acquire()).collect();
    let stats = pool.stats();

    assert_eq!(stats.sessions_created, 32);
    assert_eq!(stats.sessions_reused, 32);

    drop(second);
}

#[tokio::test]
async fn transaction_pooling_idle_clients_share_a_released_backend() {
    let proxy = start_proxy(
        Duration::from_millis(200),
        Duration::from_millis(100),
        test_buffer_pool(),
    )
    .await;
    let mut clients = Vec::new();

    for _ in 0..4 {
        clients.push(connect_started_client(proxy.address).await);
    }

    wait_for_backend_pool(&proxy, 1, 1).await;
    assert_eq!(proxy.backend_accepts.load(Ordering::Relaxed), 1);

    drop(clients);
    proxy.shutdown().await;
}

#[tokio::test]
async fn idle_transaction_timeout_recycles_and_trims_session_buffers() {
    let buffer_pool = test_buffer_pool();
    let proxy = start_proxy(
        Duration::from_millis(250),
        Duration::from_millis(25),
        buffer_pool.clone(),
    )
    .await;
    let mut client = connect_started_client(proxy.address).await;

    client
        .write_all(&query_packet("begin"))
        .await
        .expect("begin query");
    read_until_contains(&mut client, b"Z\0\0\0\x05T").await;

    let mut partial_frame = BytesMut::new();
    partial_frame.put_u8(b'Q');
    partial_frame.put_i32(4_100);
    partial_frame.extend_from_slice(&[b'x'; 1_024]);
    client
        .write_all(&partial_frame)
        .await
        .expect("partial query frame");

    read_until_contains(&mut client, b"idle transaction timed out").await;
    drop(client);

    time::timeout(Duration::from_secs(1), async {
        loop {
            if buffer_pool.stats().oversized_buffers_released == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timeout cleanup releases the oversized buffer");

    let mut reused = buffer_pool.acquire();
    assert!(reused
        .buffers_mut()
        .capacities()
        .into_iter()
        .all(|capacity| capacity <= 128));

    proxy.shutdown().await;
}

#[tokio::test]
async fn idle_client_timeout_trims_partial_frame_before_new_activity() {
    let buffer_pool = test_buffer_pool();
    let proxy = start_proxy(
        Duration::from_millis(25),
        Duration::from_millis(250),
        buffer_pool.clone(),
    )
    .await;
    let mut client = connect_started_client(proxy.address).await;

    let mut partial_frame = BytesMut::new();
    partial_frame.put_u8(b'Q');
    partial_frame.put_i32(4_100);
    partial_frame.extend_from_slice(&[b'x'; 1_024]);
    client
        .write_all(&partial_frame)
        .await
        .expect("partial query frame");

    read_until_contains(&mut client, b"idle client timed out").await;

    time::timeout(Duration::from_secs(1), async {
        loop {
            if buffer_pool.stats().oversized_buffers_released == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordinary idle timeout releases the oversized buffer before new activity");

    assert!(
        client.peer_addr().is_ok(),
        "client remains connected after timeout"
    );

    drop(client);
    proxy.shutdown().await;
}

#[tokio::test]
async fn idle_client_timeouts_do_not_repeat_without_client_activity() {
    let proxy = start_proxy(
        Duration::from_millis(25),
        Duration::from_millis(100),
        test_buffer_pool(),
    )
    .await;
    let mut clients = Vec::new();

    for _ in 0..4 {
        clients.push(connect_started_client(proxy.address).await);
    }

    for client in &mut clients {
        read_until_contains(client, b"idle client timed out").await;
        let mut extra = [0_u8; 64];
        assert!(
            time::timeout(Duration::from_millis(75), client.read(&mut extra))
                .await
                .is_err(),
            "idle client timeout must not schedule repeated timeout work"
        );
    }

    drop(clients);
    proxy.shutdown().await;
}

#[test]
fn idle_client_memory_estimate_is_derived_per_client() {
    let before = ProcessMetricSample::new(
        0,
        [(
            ProcessMetricKind::ResidentMemory,
            ProcessMetricValue::Integer(1_000),
        )],
    );
    let after = ProcessMetricSample::new(
        1,
        [(
            ProcessMetricKind::ResidentMemory,
            ProcessMetricValue::Integer(9_192),
        )],
    );

    assert_eq!(
        DerivedPerformanceMetric::memory_per_client(&before, &after, 1_024).value(),
        Some(8.0)
    );
}

#[test]
fn bounded_idle_client_scenario_validates_without_a_large_local_run() {
    let scenario = validate_benchmark_scenario(&idle_scenario_path()).expect("scenario parses");

    assert_eq!(scenario.connections().connection_count(), 128);
    assert!(scenario.expected_metrics().memory());
}

async fn start_proxy(
    idle_client_timeout: Duration,
    idle_transaction_timeout: Duration,
    buffer_pool: ProxyBufferPool,
) -> TestProxy {
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend address");
    let backend_accepts = Arc::new(AtomicUsize::new(0));
    let backend_task = tokio::spawn(run_backend(backend, Arc::clone(&backend_accepts)));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy probe");
    let address = listener.local_addr().expect("proxy address");
    drop(listener);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr: address,
            backend_addr,
        },
        routes: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 16,
            max_backends: 1,
            max_checkout_waiters: 4,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            recovery_mode: pg_kinetic::recovery::RecoveryMode::Recover,
            recovery_timeout_ms: 1_000,
            backend_reset_query: String::from("DISCARD ALL"),
        },
        qos: QosConfig {
            max_route_in_flight: 100,
            max_route_waiters: 1_000,
            query_timeout_ms: 1_000,
            idle_client_timeout_ms: idle_client_timeout.as_millis() as u64,
            idle_transaction_timeout_ms: idle_transaction_timeout.as_millis() as u64,
            max_client_buffer_bytes: 8_192,
            max_backend_buffer_bytes: 8_192,
            overload_error_code: String::from("53300"),
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

    let proxy = Proxy::with_buffer_pool(config, buffer_pool.clone());
    let drain = proxy.drain_controller();
    let snapshots = proxy.snapshot_store();
    let proxy_task = tokio::spawn(async move {
        let _ = proxy.run().await;
    });
    time::sleep(Duration::from_millis(25)).await;

    TestProxy {
        address,
        _buffer_pool: buffer_pool,
        backend_accepts,
        snapshots,
        drain,
        proxy_task,
        backend_task,
    }
}

async fn run_backend(listener: TcpListener, accepts: Arc<AtomicUsize>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        accepts.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(handle_backend(stream));
    }
}

async fn handle_backend(mut stream: TcpStream) {
    let mut startup = [0_u8; 1_024];
    if stream.read(&mut startup).await.expect("read startup") == 0 {
        return;
    }
    stream
        .write_all(&auth_ok_ready())
        .await
        .expect("auth ready");

    loop {
        let mut query = [0_u8; 8_192];
        let read = stream.read(&mut query).await.expect("read query");
        if read == 0 {
            return;
        }

        let status = if query[..read]
            .windows(b"begin".len())
            .any(|bytes| bytes.eq_ignore_ascii_case(b"begin"))
        {
            b'T'
        } else {
            b'I'
        };
        stream.write_all(&ready(status)).await.expect("query ready");
    }
}

async fn connect_started_client(address: SocketAddr) -> TcpStream {
    let mut client = TcpStream::connect(address).await.expect("connect proxy");
    client
        .write_all(&startup_packet())
        .await
        .expect("write startup");
    read_until_contains(&mut client, b"Z\0\0\0\x05I").await;
    client
}

async fn read_until_contains(client: &mut TcpStream, expected: &[u8]) {
    let mut received = Vec::new();
    time::timeout(Duration::from_secs(1), async {
        loop {
            let mut chunk = [0_u8; 512];
            let read = client.read(&mut chunk).await.expect("read proxy response");
            assert_ne!(read, 0, "proxy closed before expected response");
            received.extend_from_slice(&chunk[..read]);
            if received
                .windows(expected.len())
                .any(|bytes| bytes == expected)
            {
                return;
            }
        }
    })
    .await
    .expect("proxy response arrives");
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
    packet.put_u8(b'Q');
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
    bytes.extend_from_slice(&ready(b'I'));
    bytes.to_vec()
}

fn ready(status: u8) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(status);
    bytes.to_vec()
}

async fn wait_for_backend_pool(proxy: &TestProxy, active: usize, idle: usize) {
    time::timeout(Duration::from_secs(1), async {
        loop {
            let pool = proxy.snapshots.pool_snapshot();
            if pool.active_backends == active && pool.idle_backends == idle {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("backend returns to the transaction pool");
}
