#![cfg(all(target_os = "linux", feature = "io-uring"))]

use std::{
    fs,
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{BackendTlsMode, ClientTlsMode, Config, ReadRoutingConfig, RouteConfig, TlsConfig},
    core::runtime::RuntimeEngine,
    proxy_runtime::io_uring,
};
use pg_kinetic_core::routing::ReadRoutingMode;
use pg_kinetic_proxy::tls::{load_backend_client_config, load_server_config};
use tokio_rustls::rustls::{
    pki_types::ServerName, ClientConnection, ServerConnection, StreamOwned,
};

fn io_uring_config() -> Config {
    let mut config = Config::default();
    config.runtime.engine.runtime_engine = RuntimeEngine::IoUring;
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
fn io_uring_startup_idle_timeout_returns_postgresql_error() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TestBackend::start(BackendBehavior::QueryLoop);
    let (mut proxy, proxy_addr) =
        spawn_proxy(backend.addr, &[("PG_KINETIC_IDLE_CLIENT_TIMEOUT_MS", "50")]);
    let mut client = connect_client(proxy_addr);

    let response = read_with_timeout(&mut client, Duration::from_secs(2));
    assert!(
        response.windows(5).any(|field| field == b"57000")
            && response
                .windows(b"startup timed out".len())
                .any(|field| field == b"startup timed out"),
        "startup idle timeout should return a PostgreSQL error: {response:?}"
    );

    drop(client);
    stop_proxy(&mut proxy);
    assert_eq!(
        backend.accepted(),
        0,
        "startup timeout must not checkout backend"
    );
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_idle_client_timeout_keeps_session_usable() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TestBackend::start(BackendBehavior::QueryResponse);
    let (mut proxy, proxy_addr) =
        spawn_proxy(backend.addr, &[("PG_KINETIC_IDLE_CLIENT_TIMEOUT_MS", "50")]);
    let mut client = connect_client(proxy_addr);
    client
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write startup packet");
    read_until_ready(&mut client);

    let timeout_response = read_with_timeout(&mut client, Duration::from_secs(2));
    assert!(
        timeout_response.windows(5).any(|field| field == b"57000")
            && timeout_response
                .windows(b"idle client timed out".len())
                .any(|field| field == b"idle client timed out"),
        "idle client timeout should return a PostgreSQL error: {timeout_response:?}"
    );

    client
        .write_all(&query_packet("select 1"))
        .expect("write query packet after idle timeout");
    let query_response = read_until_data_ready(&mut client);
    assert!(
        query_response.contains(&b'D'),
        "query after idle client timeout should return data: {query_response:?}"
    );
    assert!(
        query_response.contains(&b'1'),
        "query after idle client timeout should return select value: {query_response:?}"
    );

    drop(client);
    stop_proxy(&mut proxy);
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_startup_checkout_timeout_keeps_retry_usable() {
    startup_checkout_timeout_retry_usable(&[]);
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_startup_checkout_timeout_retry_uses_private_bootstrap_after_trust_auth() {
    let auth_users_file = write_auth_users_file("postgres = trust\n");
    let auth_users_path = auth_users_file.to_string_lossy();
    startup_checkout_timeout_retry_usable(&[
        ("PG_KINETIC_AUTH_MODE", "trust"),
        ("PG_KINETIC_AUTH_USERS_FILE", auth_users_path.as_ref()),
    ]);
    let _ = fs::remove_file(auth_users_file);
}

fn startup_checkout_timeout_retry_usable(extra_env: &[(&str, &str)]) {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TestBackend::start(BackendBehavior::FirstConnectionHoldsThenQueryLoop);
    let mut env = vec![
        ("PG_KINETIC_MAX_BACKENDS", "1"),
        ("PG_KINETIC_CHECKOUT_TIMEOUT_MS", "75"),
    ];
    env.extend_from_slice(extra_env);
    let (mut proxy, proxy_addr) = spawn_proxy(backend.addr, &env);
    let mut first = connect_client(proxy_addr);
    first
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write first startup packet");
    read_until_ready(&mut first);
    first
        .write_all(&query_packet("select hold"))
        .expect("write holding query");
    thread::sleep(Duration::from_millis(100));

    let mut second = connect_client(proxy_addr);
    second
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write second startup packet");
    let checkout_response = read_with_timeout(&mut second, Duration::from_secs(2));
    assert!(
        checkout_response.windows(5).any(|field| field == b"53300")
            && checkout_response
                .windows(b"backend checkout timed out".len())
                .any(|field| field == b"backend checkout timed out"),
        "startup checkout timeout should return a PostgreSQL error: {checkout_response:?}"
    );

    thread::sleep(Duration::from_millis(500));
    second
        .write_all(&query_packet("select 1"))
        .expect("write retry query after startup checkout timeout");
    let retry_response = read_until_data_ready(&mut second);
    assert!(retry_response.contains(&b'D'));
    assert!(retry_response.contains(&b'1'));

    drop(second);
    drop(first);
    stop_proxy(&mut proxy);
    assert!(
        backend.accepted() >= 2,
        "retry should checkout a backend after the first one is released"
    );
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_idle_pinned_timeout_closes_and_discards_backend() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TestBackend::start(BackendBehavior::QueryLoop);
    let (mut proxy, proxy_addr) = spawn_proxy(
        backend.addr,
        &[
            ("PG_KINETIC_MAX_BACKENDS", "1"),
            ("PG_KINETIC_IDLE_TRANSACTION_TIMEOUT_MS", "50"),
        ],
    );
    let mut first = connect_client(proxy_addr);
    first
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write startup packet");
    read_until_ready(&mut first);
    first
        .write_all(&query_packet("create temp table t(x int)"))
        .expect("write pinned query");
    read_until_ready(&mut first);

    let timeout_response = read_with_timeout(&mut first, Duration::from_secs(2));
    assert!(
        timeout_response.windows(5).any(|field| field == b"57000")
            && timeout_response
                .windows(b"idle transaction timed out".len())
                .any(|field| field == b"idle transaction timed out"),
        "pinned idle timeout should return an idle transaction error: {timeout_response:?}"
    );
    assert!(
        wait_for_disconnect(&mut first, Duration::from_secs(2)),
        "pinned idle timeout should close the client"
    );

    let mut second = connect_client(proxy_addr);
    second
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write second startup packet");
    read_until_ready(&mut second);
    second
        .write_all(&query_packet("select 1"))
        .expect("write second query");
    let second_response = read_until_data_ready(&mut second);
    assert!(second_response.contains(&b'D'));
    assert!(second_response.contains(&b'1'));

    drop(second);
    drop(first);
    stop_proxy(&mut proxy);
    assert_eq!(
        backend.accepted(),
        2,
        "pinned idle timeout should discard the dirty backend instead of reusing it"
    );
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

#[test]
#[ignore = "requires Linux io_uring runtime and TLS fixtures"]
fn io_uring_accepts_client_tls_and_connects_backend_tls() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = TlsTestBackend::start();
    let client_cert_path = fixture_path("server-chain.pem")
        .to_string_lossy()
        .into_owned();
    let client_key_path = fixture_path("server-key.pem")
        .to_string_lossy()
        .into_owned();
    let backend_ca_path = fixture_path("ca.pem").to_string_lossy().into_owned();
    let (mut proxy, proxy_addr) = spawn_proxy(
        backend.addr,
        &[
            ("PG_KINETIC_CLIENT_TLS_MODE", "require"),
            ("PG_KINETIC_CLIENT_TLS_CERT_PATH", client_cert_path.as_str()),
            ("PG_KINETIC_CLIENT_TLS_KEY_PATH", client_key_path.as_str()),
            ("PG_KINETIC_BACKEND_TLS_MODE", "verify_full"),
            ("PG_KINETIC_BACKEND_TLS_CA_PATH", backend_ca_path.as_str()),
            ("PG_KINETIC_BACKEND_TLS_SERVER_NAME", "localhost"),
        ],
    );
    let mut client = connect_tls_client(proxy_addr);

    client
        .write_all(&startup_packet("postgres", "pgkinetic"))
        .expect("write TLS startup packet");
    client.flush().expect("flush TLS startup packet");
    let startup_response = read_tls_until_ready(&mut client);
    assert!(
        startup_response
            .windows(5)
            .any(|frame| frame == b"R\0\0\0\x08"),
        "TLS startup should receive AuthenticationOk: {startup_response:?}"
    );

    client
        .write_all(&query_packet("select 1"))
        .expect("write TLS query packet");
    client.flush().expect("flush TLS query packet");
    let query_response = read_tls_until_ready(&mut client);
    assert!(
        query_response.contains(&b'D'),
        "TLS query should receive DataRow: {query_response:?}"
    );
    assert!(
        query_response.contains(&b'1'),
        "TLS query should receive select value: {query_response:?}"
    );

    drop(client);
    stop_proxy(&mut proxy);
    assert_eq!(backend.accepted(), 1, "TLS session should use one backend");
}

fn spawn_proxy(backend_addr: SocketAddr, extra_env: &[(&str, &str)]) -> (Child, SocketAddr) {
    let listen_addr = unused_addr();
    let mut command = Command::new(env!("CARGO_BIN_EXE_pg-kinetic"));
    command
        .env("PG_KINETIC_LISTEN_ADDR", listen_addr.to_string())
        .env("PG_KINETIC_BACKEND_ADDR", backend_addr.to_string())
        .env("PG_KINETIC_RUNTIME_ENGINE", "io_uring")
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

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

fn client_tls_config() -> TlsConfig {
    TlsConfig {
        client_tls_mode: ClientTlsMode::Disable,
        client_cert_path: None,
        client_key_path: None,
        client_ca_path: None,
        backend_tls_mode: BackendTlsMode::Disable,
        backend_ca_path: Some(fixture_path("ca.pem")),
        backend_server_name: Some(String::from("localhost")),
    }
}

fn proxy_tls_config() -> TlsConfig {
    TlsConfig {
        client_tls_mode: ClientTlsMode::Require,
        client_cert_path: Some(fixture_path("server-chain.pem")),
        client_key_path: Some(fixture_path("server-key.pem")),
        client_ca_path: None,
        backend_tls_mode: BackendTlsMode::Disable,
        backend_ca_path: Some(fixture_path("ca.pem")),
        backend_server_name: Some(String::from("localhost")),
    }
}

fn connect_tls_client(proxy: SocketAddr) -> StreamOwned<ClientConnection, TcpStream> {
    let mut stream = TcpStream::connect(proxy).expect("connect TLS client");
    stream
        .write_all(&pg_kinetic::wire::tls::ssl_request_packet())
        .expect("write client SSLRequest");
    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .expect("read client SSLResponse");
    assert_eq!(response, *b"S");

    let server_name = ServerName::try_from("localhost").expect("server name");
    let connection = ClientConnection::new(
        load_backend_client_config(&client_tls_config()).expect("client TLS config"),
        server_name,
    )
    .expect("client TLS connection");
    StreamOwned::new(connection, stream)
}

fn read_tls_until_ready(stream: &mut StreamOwned<ClientConnection, TcpStream>) -> Vec<u8> {
    stream
        .sock
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set TLS client read timeout");
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

fn write_auth_users_file(contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pg-kinetic-io-uring-auth-users-{}-{}.txt",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    fs::write(&path, contents).expect("write auth users file");
    path
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

fn read_until_data_ready(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set client read timeout");
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    while Instant::now() < deadline {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                response.extend_from_slice(&chunk[..read]);
                if response.contains(&b'D')
                    && response.windows(5).any(|frame| frame == b"Z\0\0\0\x05")
                {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => break,
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
    QueryLoop,
    HoldQueries,
    DisconnectAfterStartup,
    FirstConnectionHoldsThenQueryLoop,
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
                        let connection_number = thread_accepted.fetch_add(1, Ordering::Relaxed) + 1;
                        thread::spawn(move || handle_backend(stream, behavior, connection_number));
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

struct TlsTestBackend {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
    thread: Option<thread::JoinHandle<()>>,
}

impl TlsTestBackend {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind TLS PostgreSQL test backend");
        listener
            .set_nonblocking(true)
            .expect("make TLS PostgreSQL test backend nonblocking");
        let addr = listener.local_addr().expect("TLS backend address");
        let stop = Arc::new(AtomicBool::new(false));
        let accepted = Arc::new(AtomicUsize::new(0));
        let thread_stop = Arc::clone(&stop);
        let thread_accepted = Arc::clone(&accepted);
        let server_config = load_server_config(&proxy_tls_config()).expect("backend TLS config");
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        thread_accepted.fetch_add(1, Ordering::Relaxed);
                        let server_config = Arc::clone(&server_config);
                        thread::spawn(move || handle_tls_backend(stream, server_config));
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

impl Drop for TlsTestBackend {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_tls_backend(
    mut stream: TcpStream,
    server_config: Arc<tokio_rustls::rustls::ServerConfig>,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set TLS backend read timeout");
    let mut ssl_request = [0_u8; 8];
    if stream.read_exact(&mut ssl_request).is_err() {
        return;
    }
    if ssl_request != pg_kinetic::wire::tls::ssl_request_packet()[..] {
        return;
    }
    if stream.write_all(b"S").is_err() {
        return;
    }

    let connection = match ServerConnection::new(server_config) {
        Ok(connection) => connection,
        Err(_) => return,
    };
    let mut stream = StreamOwned::new(connection, stream);
    let mut header = [0_u8; 4];
    if stream.read_exact(&mut header).is_err() {
        return;
    }
    let body_len = i32::from_be_bytes(header) as usize - 4;
    let mut body = vec![0_u8; body_len];
    if stream.read_exact(&mut body).is_err() {
        return;
    }
    if stream.write_all(&auth_ok_ready()).is_err() || stream.flush().is_err() {
        return;
    }

    let mut query_header = [0_u8; 5];
    if stream.read_exact(&mut query_header).is_err() {
        return;
    }
    let query_len =
        i32::from_be_bytes(query_header[1..].try_into().expect("query length")) as usize - 4;
    let mut query = vec![0_u8; query_len];
    if stream.read_exact(&mut query).is_err() {
        return;
    }
    let _ = stream.write_all(&select_one_ready());
    let _ = stream.flush();
}

fn handle_backend(mut stream: TcpStream, behavior: BackendBehavior, connection_number: usize) {
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
        BackendBehavior::QueryResponse
        | BackendBehavior::QueryLoop
        | BackendBehavior::HoldQueries
        | BackendBehavior::FirstConnectionHoldsThenQueryLoop => {
            if stream.write_all(&auth_ok_ready()).is_err() {
                return;
            }
            let mut query_count = 0;
            loop {
                let mut query_header = [0_u8; 5];
                if stream.read_exact(&mut query_header).is_err() {
                    return;
                }
                let query_len =
                    i32::from_be_bytes(query_header[1..].try_into().expect("query length"))
                        as usize
                        - 4;
                let mut query = vec![0_u8; query_len];
                if stream.read_exact(&mut query).is_err() {
                    return;
                }
                query_count += 1;
                if matches!(behavior, BackendBehavior::HoldQueries)
                    || (matches!(behavior, BackendBehavior::FirstConnectionHoldsThenQueryLoop)
                        && connection_number == 1
                        && query_count == 1)
                {
                    thread::sleep(Duration::from_millis(300));
                    return;
                }
                let _ = stream.write_all(&select_one_ready());
                if !matches!(
                    behavior,
                    BackendBehavior::QueryLoop | BackendBehavior::FirstConnectionHoldsThenQueryLoop
                ) {
                    return;
                }
            }
        }
    }
}
