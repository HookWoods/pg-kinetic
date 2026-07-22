use std::{
    collections::HashMap,
    io::ErrorKind,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use metrics::{Counter, CounterFn, Gauge, Histogram, Key, Metadata, Recorder};
use pg_kinetic::{
    config::{
        CapacityConfig, Config, ConnectionConfig, ObservabilityConfig, PerformanceConfig, QosConfig,
    },
    proxy::Proxy,
    recovery::RecoveryMode,
    wire::{
        backend::{parse_backend_frame, BackendFrame, ReadyStatus},
        frame::parse_frontend_frame,
        message::parse_simple_query,
        protocol::{FrontendTag, ProtocolVersion},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

static METRICS_RECORDER: OnceLock<Arc<TestRecorder>> = OnceLock::new();

#[tokio::test(flavor = "current_thread")]
async fn client_buffer_overflow_closes_before_backend_checkout() {
    let _guard = test_lock().lock().await;
    let recorder = install_metrics_recorder();
    recorder.clear();
    let (proxy_addr, backend_connections) =
        spawn_proxy(BackendMode::Normal, client_budget_qos(64, 4_194_304)).await;

    let mut client = TcpStream::connect(proxy_addr).await.expect("connect proxy");
    let oversized_application_name = "x".repeat(256);
    client
        .write_all(&startup_packet(
            "pgkinetic",
            "postgres",
            Some(oversized_application_name.as_str()),
        ))
        .await
        .expect("send startup");

    expect_connection_close(&mut client).await;
    assert_eq!(backend_connections.load(Ordering::SeqCst), 0);
    assert!(recorder.counter_count("pg_kinetic_buffer_limit_total", &[("kind", "client")]) > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn backend_buffer_overflow_discards_the_backend_mid_query() {
    let _guard = test_lock().lock().await;
    let recorder = install_metrics_recorder();
    recorder.clear();
    let (proxy_addr, backend_connections) = spawn_proxy(
        BackendMode::OversizedResponseOnFirstQuery,
        backend_budget_qos(1_048_576, 64),
    )
    .await;

    let mut client = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;
    send_query(&mut client, "select 1").await;
    expect_connection_close(&mut client).await;

    let mut second = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;
    send_query(&mut second, "select 1").await;
    let frames = read_until_ready(&mut second).await;
    assert!(frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));

    assert!(backend_connections.load(Ordering::SeqCst) >= 2);
    assert!(recorder.counter_count("pg_kinetic_buffer_limit_total", &[("kind", "backend")]) > 0);
}

fn client_budget_qos(max_client_buffer_bytes: usize, max_backend_buffer_bytes: usize) -> QosConfig {
    QosConfig {
        max_route_in_flight: 1,
        max_route_waiters: 1,
        query_timeout_ms: 1_000,
        idle_client_timeout_ms: 5_000,
        idle_transaction_timeout_ms: 5_000,
        max_client_buffer_bytes,
        max_backend_buffer_bytes,
        overload_error_code: "53300".to_string(),
    }
}

fn backend_budget_qos(
    max_client_buffer_bytes: usize,
    max_backend_buffer_bytes: usize,
) -> QosConfig {
    QosConfig {
        max_route_in_flight: 1,
        max_route_waiters: 1,
        query_timeout_ms: 1_000,
        idle_client_timeout_ms: 5_000,
        idle_transaction_timeout_ms: 5_000,
        max_client_buffer_bytes,
        max_backend_buffer_bytes,
        overload_error_code: "53300".to_string(),
    }
}

fn install_metrics_recorder() -> Arc<TestRecorder> {
    METRICS_RECORDER
        .get_or_init(|| {
            let recorder = Arc::new(TestRecorder::default());
            metrics::set_global_recorder(recorder.clone()).expect("install metrics recorder");
            recorder
        })
        .clone()
}

fn test_lock() -> &'static tokio::sync::Mutex<()> {
    static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[derive(Debug, Default)]
struct TestRecorder {
    counters: Mutex<HashMap<String, Arc<TestCounter>>>,
}

#[derive(Debug, Default)]
struct TestCounter {
    value: AtomicU64,
}

impl TestRecorder {
    fn clear(&self) {
        self.counters.lock().expect("lock recorder").clear();
    }

    fn counter_count(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let signature = metric_signature(name, labels);
        self.counters
            .lock()
            .expect("lock recorder")
            .get(&signature)
            .map(|counter| counter.value.load(Ordering::SeqCst))
            .unwrap_or(0)
    }
}

impl CounterFn for TestCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::SeqCst);
    }

    fn absolute(&self, value: u64) {
        let mut current = self.value.load(Ordering::SeqCst);
        while current < value {
            match self
                .value
                .compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(updated) => current = updated,
            }
        }
    }
}

impl Recorder for TestRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let signature = metric_signature_from_key(key);
        let mut counters = self.counters.lock().expect("lock recorder");
        let counter = counters
            .entry(signature)
            .or_insert_with(|| Arc::new(TestCounter::default()))
            .clone();
        Counter::from_arc(counter)
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn metric_signature_from_key(key: &Key) -> String {
    let labels = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}|{}", key.name(), labels)
}

fn metric_signature(name: &str, labels: &[(&str, &str)]) -> String {
    let labels = labels
        .iter()
        .map(|(label_key, label_value)| format!("{label_key}={label_value}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}|{labels}")
}

async fn spawn_proxy(backend_mode: BackendMode, qos: QosConfig) -> (SocketAddr, Arc<AtomicUsize>) {
    let backend_connections = Arc::new(AtomicUsize::new(0));
    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend_listener.local_addr().expect("backend addr");

    tokio::spawn({
        let backend_connections = backend_connections.clone();
        async move {
            loop {
                let (stream, _) = backend_listener.accept().await.expect("accept backend");
                let backend_connections = backend_connections.clone();
                tokio::spawn(async move {
                    handle_backend_connection(stream, backend_connections, backend_mode).await;
                });
            }
        }
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    drop(proxy_listener);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr: proxy_addr,
            backend_addr,
        },
        routes: Vec::new(),
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 2,
            max_checkout_waiters: 4,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            pool_mode: Default::default(),
            recovery_mode: RecoveryMode::Recover,
            recovery_timeout_ms: 100,
            backend_reset_query: "DISCARD ALL".to_string(),
        },
        qos,
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

    time::sleep(Duration::from_millis(50)).await;
    (proxy_addr, backend_connections)
}

async fn handle_backend_connection(
    mut stream: TcpStream,
    backend_connections: Arc<AtomicUsize>,
    backend_mode: BackendMode,
) {
    let connection_id = backend_connections.fetch_add(1, Ordering::SeqCst) + 1;
    let mut startup = [0_u8; 4096];
    let read = match stream.read(&mut startup).await {
        Ok(read) => read,
        Err(_) => return,
    };
    if read == 0 {
        return;
    }

    if stream.write_all(&auth_ok_ready()).await.is_err() {
        return;
    }

    let mut buffer = BytesMut::with_capacity(4096);
    loop {
        let read = match stream.read_buf(&mut buffer).await {
            Ok(read) => read,
            Err(_) => return,
        };
        if read == 0 {
            return;
        }

        while let Some(frame) = match parse_frontend_frame(&mut buffer) {
            Ok(frame) => frame,
            Err(_) => return,
        } {
            if let Some(query) = match parse_simple_query(&frame) {
                Ok(query) => query,
                Err(_) => return,
            } {
                match backend_mode {
                    BackendMode::Normal => {
                        if stream.write_all(&ready_idle()).await.is_err() {
                            return;
                        }
                    }
                    BackendMode::OversizedResponseOnFirstQuery if connection_id == 1 => {
                        if stream
                            .write_all(&oversized_backend_response(8_192))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let _ = query;
                    }
                    BackendMode::OversizedResponseOnFirstQuery => {
                        if stream.write_all(&ready_idle()).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendMode {
    Normal,
    OversizedResponseOnFirstQuery,
}

async fn open_client(
    addr: SocketAddr,
    database: &str,
    user: &str,
    application_name: Option<&str>,
) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream
        .write_all(&startup_packet(database, user, application_name))
        .await
        .expect("startup");

    let startup_frames = read_until_ready(&mut stream).await;
    assert!(startup_frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));

    stream
}

async fn send_query(stream: &mut TcpStream, query: &str) {
    stream.write_all(&query_packet(query)).await.expect("query");
}

async fn expect_connection_close(stream: &mut TcpStream) {
    let mut bytes = Vec::new();
    let close = time::timeout(Duration::from_secs(1), stream.read_to_end(&mut bytes))
        .await
        .expect("wait for close");
    match close {
        Ok(read) => assert!(read > 0 || bytes.is_empty()),
        Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
        Err(error) => panic!("read close: {error}"),
    }
}

async fn read_until_ready(stream: &mut TcpStream) -> Vec<BackendFrame> {
    let mut bytes = BytesMut::new();
    let mut frames = Vec::new();
    let mut buffer = [0_u8; 1024];

    loop {
        let read = time::timeout(Duration::from_secs(1), stream.read(&mut buffer))
            .await
            .expect("timed out waiting for ready")
            .expect("read response");
        assert!(read > 0, "connection closed before ready");

        bytes.extend_from_slice(&buffer[..read]);
        while let Some(frame) = parse_backend_frame(&mut bytes).expect("parse backend frame") {
            let ready = frame.ready_status().is_some();
            frames.push(frame);
            if ready {
                return frames;
            }
        }
    }
}

fn startup_packet(database: &str, user: &str, application_name: Option<&str>) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.put_u8(0);

    if let Some(application_name) = application_name {
        body.extend_from_slice(b"application_name\0");
        body.extend_from_slice(application_name.as_bytes());
        body.put_u8(0);
    }

    body.put_u8(0);

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

fn oversized_backend_response(payload_len: usize) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'D');
    bytes.put_i32((payload_len + 4) as i32);
    bytes.extend_from_slice(&vec![0_u8; payload_len]);
    bytes.to_vec()
}
