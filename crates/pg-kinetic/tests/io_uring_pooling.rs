#![cfg(all(target_os = "linux", feature = "io-uring"))]

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{config::Config, core::runtime::RuntimeEngine, proxy_runtime::io_uring};

fn io_uring_config() -> Config {
    let mut config = Config::default();
    config.runtime.engine.runtime_engine = RuntimeEngine::ExperimentalIoUring;
    config.runtime.engine.experimental_runtime_enabled = true;
    config
}

#[test]
fn io_uring_accepts_pool_configs_after_shared_pool_checkout() {
    let mut config = io_uring_config();
    config.pools = vec![pg_kinetic::config::PoolConfig {
        database: "app".to_string(),
        user: "app".to_string(),
        backend_addr: "127.0.0.1:6544".parse().expect("pool addr"),
        max_backends: Some(2),
    }];

    io_uring::validate_supported_config_for_test(&config)
        .expect("io_uring supports pool configs after shared checkout");
}

fn linux_io_uring_prerequisites_available() -> bool {
    std::path::Path::new("/proc/sys/kernel/io_uring_disabled").exists()
        || std::env::var_os("PG_KINETIC_RUN_IO_URING_TESTS").is_some()
}

#[test]
#[ignore = "requires Linux io_uring runtime and PostgreSQL test service"]
fn io_uring_enforces_global_backend_capacity() {
    assert!(
        linux_io_uring_prerequisites_available(),
        "set PG_KINETIC_RUN_IO_URING_TESTS=1 on supported Linux"
    );

    let backend = HoldingBackend::start();
    let (mut proxy, proxy_addr) = spawn_proxy(backend.addr);
    let mut first = TcpStream::connect(proxy_addr).expect("connect first client");
    first
        .write_all(&startup_packet())
        .expect("write first startup");
    read_until_ready(&mut first);
    first
        .write_all(&query_packet("select hold"))
        .expect("write first query");
    backend.query_received();

    let mut second = TcpStream::connect(proxy_addr).expect("connect second client");
    second
        .write_all(&startup_packet())
        .expect("write second startup");
    let response = read_with_timeout(&mut second, Duration::from_secs(2));
    assert!(
        response.windows(5).any(|field| field == b"53300")
            && response
                .windows(b"backend checkout timed out".len())
                .any(|field| field == b"backend checkout timed out"),
        "second checkout should observe global capacity timeout: {response:?}"
    );

    drop(second);
    drop(first);
    stop_proxy(&mut proxy);
}

fn spawn_proxy(backend_addr: SocketAddr) -> (Child, SocketAddr) {
    let listen_addr = TcpListener::bind("127.0.0.1:0")
        .expect("bind proxy address")
        .local_addr()
        .expect("proxy address");
    let mut command = Command::new(env!("CARGO_BIN_EXE_pg-kinetic"));
    command
        .env("PG_KINETIC_LISTEN_ADDR", listen_addr.to_string())
        .env("PG_KINETIC_BACKEND_ADDR", backend_addr.to_string())
        .env("PG_KINETIC_RUNTIME_ENGINE", "experimental_io_uring")
        .env("PG_KINETIC_EXPERIMENTAL_RUNTIME_ENABLED", "true")
        .env("PG_KINETIC_RUNTIME_SHARDS", "1")
        .env("PG_KINETIC_MAX_BACKENDS", "1")
        .env("PG_KINETIC_CHECKOUT_TIMEOUT_MS", "75")
        .env("PG_KINETIC_STARTUP_BACKEND_CHECKS_ENABLED", "false");
    let child = command.spawn().expect("spawn pg-kinetic io_uring process");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if TcpStream::connect(listen_addr).is_ok() {
            return (child, listen_addr);
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("pg-kinetic did not start listening on {listen_addr}");
}

fn stop_proxy(proxy: &mut Child) {
    let _ = Command::new("kill")
        .args(["-TERM", &proxy.id().to_string()])
        .status();
    let _ = proxy.wait();
}

fn startup_packet() -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(196608);
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

struct HoldingBackend {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    query_seen: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl HoldingBackend {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind PostgreSQL test backend");
        listener
            .set_nonblocking(true)
            .expect("make PostgreSQL test backend nonblocking");
        let addr = listener.local_addr().expect("backend address");
        let stop = Arc::new(AtomicBool::new(false));
        let query_seen = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_query_seen = Arc::clone(&query_seen);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let query_seen = Arc::clone(&thread_query_seen);
                        thread::spawn(move || handle_backend(stream, query_seen));
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
            query_seen,
            thread: Some(thread),
        }
    }

    fn query_received(&self) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !self.query_seen.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            self.query_seen.load(Ordering::Acquire),
            "backend query received"
        );
    }
}

impl Drop for HoldingBackend {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_backend(mut stream: TcpStream, query_seen: Arc<AtomicBool>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set backend read timeout");
    let mut header = [0_u8; 4];
    if stream.read_exact(&mut header).is_err() {
        return;
    }
    let body_len = i32::from_be_bytes(header) as usize - 4;
    let mut body = vec![0_u8; body_len];
    if stream.read_exact(&mut body).is_err() || stream.write_all(&auth_ok_ready()).is_err() {
        return;
    }
    let mut query_header = [0_u8; 5];
    if stream.read_exact(&mut query_header).is_ok() {
        query_seen.store(true, Ordering::Release);
        thread::sleep(Duration::from_secs(2));
    }
}
