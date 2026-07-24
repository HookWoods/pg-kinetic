#![cfg(all(target_os = "linux", feature = "io-uring"))]

use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    process::{Child, Command},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{Config, ReadRoutingConfig, RouteConfig},
    core::runtime::RuntimeEngine,
    proxy_runtime::io_uring,
};
use pg_kinetic_core::routing::ReadRoutingMode;

fn io_uring_config() -> Config {
    let mut config = Config::default();
    config.runtime.engine.runtime_engine = RuntimeEngine::ExperimentalIoUring;
    config.runtime.engine.experimental_runtime_enabled = true;
    config
}

#[test]
fn io_uring_session_lifecycle_uses_shared_runtime_path() {
    let summary = io_uring::session_lifecycle_summary_for_test(io_uring_config())
        .expect("io_uring session lifecycle summary");

    assert_eq!(summary.client_transport, "monoio");
    assert_eq!(summary.backend_checkout, "shared_pool");
    assert_eq!(summary.session_lifecycle, "shared_proxy");
}

#[test]
fn io_uring_route_shapes_use_shared_runtime_path() {
    let mut config = io_uring_config();
    let mut route = RouteConfig::from_backend_addr("127.0.0.1:6544".parse().expect("route addr"));
    route.replicas.push(Default::default());
    route.read_routing = ReadRoutingConfig {
        read_routing_mode: ReadRoutingMode::PreferReplica,
        ..ReadRoutingConfig::default()
    };
    config.routes = vec![
        route,
        RouteConfig::from_backend_addr("127.0.0.1:6545".parse().expect("route addr")),
    ];

    let summary = io_uring::session_lifecycle_summary_for_test(config)
        .expect("route shapes use shared runtime path");

    assert_eq!(summary.backend_checkout, "shared_pool");
    assert_eq!(summary.session_lifecycle, "shared_proxy");
}

#[test]
fn io_uring_runtime_no_longer_uses_direct_pass_through_loop() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../pg-kinetic-proxy/src/io_uring.rs"
    ))
    .expect("read io_uring source");

    assert!(!source.contains("take_frontend_cycle_bytes("));
    assert!(!source.contains("forward_backend_until_ready("));
    assert!(!source.contains("proxy_startup_streams("));
}

fn linux_io_uring_prerequisites_available() -> bool {
    std::path::Path::new("/proc/sys/kernel/io_uring_disabled").exists()
        || std::env::var_os("PG_KINETIC_RUN_IO_URING_TESTS").is_some()
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_simple_query_matches_tokio_default() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TestBackend::start(BackendBehavior::QueryResponse);
    let (mut proxy, proxy_addr) = spawn_proxy(backend.addr, &[]);
    let mut client = connect_client(proxy_addr);

    client
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write startup packet");
    let startup_response = read_until_ready(&mut client);
    assert!(startup_response
        .windows(5)
        .any(|frame| frame == b"R\0\0\0\x08"));
    assert!(startup_response.contains(&b'Z'));

    client
        .write_all(&query_packet("select 1"))
        .expect("write query packet");
    let query_response = read_until_ready(&mut client);
    assert!(query_response.contains(&b'D'));
    assert!(query_response.contains(&b'1'));

    drop(client);
    stop_proxy(&mut proxy);
    assert_eq!(backend.accepted(), 1, "session should reuse one backend");
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_query_timeout_preserves_client_cleanup() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TestBackend::start(BackendBehavior::HoldQueries);
    let (mut proxy, proxy_addr) =
        spawn_proxy(backend.addr, &[("PG_KINETIC_QUERY_TIMEOUT_MS", "50")]);
    let mut client = connect_client(proxy_addr);
    client
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write startup packet");
    read_until_ready(&mut client);
    client
        .write_all(&query_packet("select pg_sleep(1)"))
        .expect("write query packet");

    let response = read_with_timeout(&mut client, Duration::from_secs(2));
    assert!(
        response.windows(5).any(|field| field == b"57014")
            || response.windows(5).any(|field| field == b"53301"),
        "timeout should return a PostgreSQL timeout or checkout error: {response:?}"
    );

    drop(client);
    stop_proxy(&mut proxy);
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_discards_backend_after_ambiguous_failure() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TestBackend::start(BackendBehavior::DisconnectAfterStartup);
    let (mut proxy, proxy_addr) = spawn_proxy(backend.addr, &[]);
    let mut client = connect_client(proxy_addr);
    client
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write startup packet");
    let _ = read_with_timeout(&mut client, Duration::from_secs(2));
    assert!(
        wait_for_disconnect(&mut client, Duration::from_secs(2)),
        "client connection should close after backend disconnect"
    );
    drop(client);

    stop_proxy(&mut proxy);
    assert_eq!(backend.accepted(), 1, "failed backend must be discarded");
}

fn spawn_proxy(backend_addr: SocketAddr, extra_env: &[(&str, &str)]) -> (Child, SocketAddr) {
    let listen_addr = unused_addr();
    let mut command = Command::new(env!("CARGO_BIN_EXE_pg-kinetic"));
    command
        .env("PG_KINETIC_LISTEN_ADDR", listen_addr.to_string())
        .env("PG_KINETIC_BACKEND_ADDR", backend_addr.to_string())
        .env("PG_KINETIC_RUNTIME_ENGINE", "experimental_io_uring")
        .env("PG_KINETIC_EXPERIMENTAL_RUNTIME_ENABLED", "true")
        .env("PG_KINETIC_RUNTIME_SHARDS", "1")
        .env("PG_KINETIC_MAX_BACKENDS", "4")
        .env("PG_KINETIC_STARTUP_BACKEND_CHECKS_ENABLED", "false");
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().expect("spawn pg-kinetic io_uring process");
    wait_for_listener(listen_addr);
    (child, listen_addr)
}

fn connect_client(proxy: SocketAddr) -> TcpStream {
    TcpStream::connect(proxy).expect("connect PostgreSQL client")
}

fn stop_proxy(proxy: &mut Child) {
    let _ = Command::new("kill")
        .args(["-TERM", &proxy.id().to_string()])
        .status();
    let _ = proxy.wait();
}

fn unused_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral listener")
        .local_addr()
        .expect("ephemeral listener address")
}

fn wait_for_listener(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("pg-kinetic did not start listening on {addr}");
}

fn read_until_ready(stream: &mut TcpStream) -> Vec<u8> {
    read_with_timeout(stream, Duration::from_secs(2))
}

fn read_with_timeout(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    stream
        .set_read_timeout(Some(timeout))
        .expect("set client read timeout");
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                response.extend_from_slice(&chunk[..read]);
                if response.windows(5).any(|frame| frame == b"Z\0\0\0\x05") {
                    break;
                }
            }
        }
    }
    response
}

fn wait_for_disconnect(stream: &mut TcpStream, timeout: Duration) -> bool {
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set disconnect read timeout");
    let deadline = Instant::now() + timeout;
    let mut buffer = [0_u8; 256];
    while Instant::now() < deadline {
        match stream.read(&mut buffer) {
            Ok(0) => return true,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => return true,
        }
    }
    false
}

fn startup_packet(user: &str, database: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(196608);
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.extend_from_slice(b"\0\0");
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

#[derive(Clone, Copy)]
enum BackendBehavior {
    QueryResponse,
    HoldQueries,
    DisconnectAfterStartup,
}

struct TestBackend {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
    thread: Option<thread::JoinHandle<()>>,
}

impl TestBackend {
    fn start(behavior: BackendBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind PostgreSQL test backend");
        listener
            .set_nonblocking(true)
            .expect("make PostgreSQL test backend nonblocking");
        let addr = listener.local_addr().expect("backend address");
        let stop = Arc::new(AtomicBool::new(false));
        let accepted = Arc::new(AtomicUsize::new(0));
        let thread_stop = Arc::clone(&stop);
        let thread_accepted = Arc::clone(&accepted);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        thread_accepted.fetch_add(1, Ordering::Relaxed);
                        thread::spawn(move || handle_backend(stream, behavior));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            stop,
            accepted,
            thread: Some(thread),
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }
}

impl Drop for TestBackend {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_backend(mut stream: TcpStream, behavior: BackendBehavior) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set backend read timeout");
    let mut header = [0_u8; 4];
    if stream.read_exact(&mut header).is_err() {
        return;
    }
    let body_len = i32::from_be_bytes(header) as usize - 4;
    let mut body = vec![0_u8; body_len];
    if stream.read_exact(&mut body).is_err() {
        return;
    }
    match behavior {
        BackendBehavior::DisconnectAfterStartup => {
            let _ = stream.shutdown(Shutdown::Both);
        }
        BackendBehavior::QueryResponse | BackendBehavior::HoldQueries => {
            if stream.write_all(&auth_ok_ready()).is_err() {
                return;
            }
            let mut query_header = [0_u8; 5];
            if stream.read_exact(&mut query_header).is_err() {
                return;
            }
            let query_len = i32::from_be_bytes(query_header[1..].try_into().expect("query length"))
                as usize
                - 4;
            let mut query = vec![0_u8; query_len];
            if stream.read_exact(&mut query).is_err() {
                return;
            }
            if matches!(behavior, BackendBehavior::HoldQueries) {
                thread::sleep(Duration::from_secs(2));
            } else {
                let _ = stream.write_all(&select_one_ready());
            }
        }
    }
}
