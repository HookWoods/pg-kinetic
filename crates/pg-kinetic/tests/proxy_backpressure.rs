use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    backpressure::BackpressureError,
    config::{
        CapacityConfig, Config, ConnectionConfig, ObservabilityConfig, PerformanceConfig,
        QosConfig, TlsConfig,
    },
    pool::{BackendPool, PoolError},
    proxy::Proxy,
    recovery::RecoveryMode,
    route::{QueryClass, RouteKey},
    wire::{
        backend::{build_error_response, parse_backend_frame, BackendFrame, ReadyStatus},
        frame::parse_frontend_frame,
        message::parse_simple_query,
        protocol::{FrontendTag, ProtocolVersion},
        sqlstate::SqlState,
        startup::{parse_startup_packet, StartupPacket},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Notify},
    time,
};

#[tokio::test]
async fn startup_route_components_are_forwarded_to_the_backend() {
    let (proxy_addr, mut seen, _) = spawn_proxy_with_backend(HoldRule::Never).await;

    let mut client = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;
    send_query(&mut client, "select 1").await;
    read_response(&mut client).await;

    let events = collect_events(&mut seen).await;
    assert!(events
        .iter()
        .any(|event| event.contains("startup db=pgkinetic user=postgres app=api")));
}

#[test]
fn overload_error_responses_include_clear_fields() {
    let cases = [
        ("53301", "backend checkout queue is full"),
        ("53301", "backend checkout timed out"),
        ("57014", "query timed out"),
        ("57000", "idle client timed out"),
        ("57000", "backend response buffer limit exceeded"),
    ];

    for (sqlstate, message) in cases {
        let frame = parse_error_frame(&build_error_response(sqlstate, message));
        assert_eq!(frame.tag, b'E');
        assert_error_field(&frame, b'S', "ERROR");
        assert_error_field(&frame, b'C', sqlstate);
        assert_error_field(&frame, b'M', message);
    }
}

#[tokio::test]
async fn backpressure_checkout_queue_full_and_timeout_errors_are_distinct() {
    let route = RouteKey::new(
        "pgkinetic",
        "postgres",
        Some("api"),
        None,
        QueryClass::Default,
    );

    let queue_full = BackendPool::new(
        "127.0.0.1:1".parse().expect("backend addr"),
        TlsConfig::default(),
        1,
        1,
        0,
        0,
        Duration::from_millis(5),
        "DISCARD ALL",
    );
    let queue_full_error = queue_full
        .checkout(route.clone())
        .await
        .expect_err("queue full");
    assert!(matches!(
        queue_full_error,
        PoolError::Backpressure(BackpressureError::QueueFull)
    ));

    let timeout = BackendPool::new(
        "127.0.0.1:1".parse().expect("backend addr"),
        TlsConfig::default(),
        1,
        1,
        0,
        1,
        Duration::from_millis(5),
        "DISCARD ALL",
    );
    let timeout_error = timeout.checkout(route).await.expect_err("timeout");
    assert!(matches!(
        timeout_error,
        PoolError::Backpressure(BackpressureError::Timeout)
    ));
}

#[tokio::test]
async fn database_startup_value_participates_in_route_selection() {
    let (proxy_addr, mut seen, release_hold) =
        spawn_proxy_with_backend(HoldRule::Database("pgkinetic-a")).await;

    let mut held = open_client(proxy_addr, "pgkinetic-a", "postgres", Some("api-a")).await;
    send_query(&mut held, "select hold").await;
    let hold_event = wait_for_event(&mut seen, "hold-started").await;

    let mut other = open_client(proxy_addr, "pgkinetic-b", "postgres", Some("api-b")).await;
    send_query(&mut other, "select 1").await;
    read_response(&mut other).await;

    release_hold.notify_waiters();
    read_response(&mut held).await;

    assert!(hold_event.contains("db=pgkinetic-a"));
    assert!(hold_event.contains("user=postgres"));

    let events = collect_events(&mut seen).await;
    assert!(events
        .iter()
        .any(|event| event.contains("startup db=pgkinetic-b user=postgres")));
}

#[tokio::test]
async fn user_startup_value_participates_in_route_selection() {
    let (proxy_addr, mut seen, release_hold) =
        spawn_proxy_with_backend(HoldRule::User("alice")).await;

    let mut held = open_client(proxy_addr, "pgkinetic", "alice", Some("api-a")).await;
    send_query(&mut held, "select hold").await;
    let hold_event = wait_for_event(&mut seen, "hold-started").await;

    let mut other = open_client(proxy_addr, "pgkinetic", "bob", Some("api-b")).await;
    send_query(&mut other, "select 1").await;
    read_response(&mut other).await;

    release_hold.notify_waiters();
    read_response(&mut held).await;

    assert!(hold_event.contains("db=pgkinetic"));
    assert!(hold_event.contains("user=alice"));

    let events = collect_events(&mut seen).await;
    assert!(events
        .iter()
        .any(|event| event.contains("startup db=pgkinetic user=bob")));
}

#[tokio::test]
async fn application_name_change_is_forwarded_before_the_next_query() {
    let (proxy_addr, mut seen, _) = spawn_proxy_with_backend(HoldRule::Never).await;

    let mut client = open_client(proxy_addr, "pgkinetic", "postgres", Some("api-a")).await;

    send_query(&mut client, "set application_name = 'api-b'").await;
    read_response(&mut client).await;

    send_query(&mut client, "select 1").await;
    read_response(&mut client).await;

    let events = collect_events(&mut seen).await;
    let set_index = events
        .iter()
        .position(|event| event.contains("sql=set application_name = 'api-b'"))
        .expect("set application_name query recorded");
    let select_index = events
        .iter()
        .position(|event| event.contains("sql=select 1"))
        .expect("select query recorded");

    assert!(set_index < select_index);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimeoutScenario {
    Normal,
    QueryTimeoutThenNormal,
    IdleTransactionThenNormal,
}

async fn spawn_timeout_proxy(scenario: TimeoutScenario) -> (SocketAddr, mpsc::Receiver<String>) {
    let (sender, receiver) = mpsc::channel(128);
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");
    let connection_ids = Arc::new(AtomicU64::new(0));

    tokio::spawn({
        let connection_ids = connection_ids.clone();
        async move {
            loop {
                let (mut stream, _) = backend.accept().await.expect("accept backend");
                let sender = sender.clone();
                let connection_ids = connection_ids.clone();
                tokio::spawn(async move {
                    let connection_id = connection_ids.fetch_add(1, Ordering::Relaxed) + 1;
                    sender
                        .send(format!("conn={connection_id} open"))
                        .await
                        .expect("record connection open");

                    let mut startup = [0_u8; 2048];
                    let read = stream.read(&mut startup).await.expect("read startup");
                    if read == 0 {
                        sender
                            .send(format!("conn={connection_id} closed before startup"))
                            .await
                            .expect("record startup close");
                        return;
                    }

                    let startup_packet =
                        parse_startup_packet(&startup[..read]).expect("parse startup");
                    let (database, user, application_name) = startup_fields(&startup_packet);
                    sender
                        .send(format!(
                            "startup db={database} user={user} app={}",
                            application_name.as_deref().unwrap_or("<none>")
                        ))
                        .await
                        .expect("record startup");

                    stream
                        .write_all(&auth_ok_ready())
                        .await
                        .expect("auth ready");

                    let mut buffer = BytesMut::with_capacity(4096);
                    let mut hung = false;
                    loop {
                        let read = stream.read_buf(&mut buffer).await.expect("read frontend");
                        if read == 0 {
                            sender
                                .send(format!("conn={connection_id} closed"))
                                .await
                                .expect("record close");
                            return;
                        }

                        while let Some(frame) =
                            parse_frontend_frame(&mut buffer).expect("parse frontend frame")
                        {
                            if hung {
                                continue;
                            }

                            if let Some(query) = parse_simple_query(&frame).expect("parse query") {
                                sender
                                    .send(format!("conn={connection_id} query={query}"))
                                    .await
                                    .expect("record query");

                                match scenario {
                                    TimeoutScenario::Normal => {
                                        if query == "begin" {
                                            stream
                                                .write_all(&ready_in_transaction())
                                                .await
                                                .expect("write begin ready");
                                        } else {
                                            stream
                                                .write_all(&ready_idle())
                                                .await
                                                .expect("write ready");
                                        }
                                    }
                                    TimeoutScenario::QueryTimeoutThenNormal => {
                                        if connection_id == 1 {
                                            hung = true;
                                        } else if query == "begin" {
                                            stream
                                                .write_all(&ready_in_transaction())
                                                .await
                                                .expect("write begin ready");
                                        } else {
                                            stream
                                                .write_all(&ready_idle())
                                                .await
                                                .expect("write ready");
                                        }
                                    }
                                    TimeoutScenario::IdleTransactionThenNormal => {
                                        if connection_id == 1 && query == "begin" {
                                            stream
                                                .write_all(&ready_in_transaction())
                                                .await
                                                .expect("write begin ready");
                                            hung = true;
                                        } else if connection_id == 1 {
                                            hung = true;
                                        } else if query == "begin" {
                                            stream
                                                .write_all(&ready_in_transaction())
                                                .await
                                                .expect("write begin ready");
                                        } else {
                                            stream
                                                .write_all(&ready_idle())
                                                .await
                                                .expect("write ready");
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
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
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 2,
            max_checkout_waiters: 4,
        },
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            recovery_mode: RecoveryMode::Recover,
            recovery_timeout_ms: 50,
            backend_reset_query: "DISCARD ALL".to_string(),
        },
        qos: QosConfig {
            max_route_in_flight: 1,
            max_route_waiters: 1,
            query_timeout_ms: 50,
            idle_client_timeout_ms: 50,
            idle_transaction_timeout_ms: 50,
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
    time::sleep(Duration::from_millis(50)).await;

    (listen_addr, receiver)
}

#[tokio::test]
async fn query_timeout_returns_error_and_discards_uncertain_backend() {
    let (proxy_addr, mut seen) = spawn_timeout_proxy(TimeoutScenario::QueryTimeoutThenNormal).await;

    let mut client = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;
    send_query(&mut client, "select slow").await;

    let timeout_frames = read_frames(&mut client).await;
    assert!(!timeout_frames.is_empty());
    assert_eq!(timeout_frames[0].tag, b'E');
    assert_eq!(timeout_frames[0].sqlstate(), Some(SqlState::QueryCanceled));
    assert_error_field(&timeout_frames[0], b'M', "query timed out");
    assert!(timeout_frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));

    let mut second = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;
    send_query(&mut second, "select 1").await;
    let second_frames = read_frames(&mut second).await;
    assert!(second_frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event.contains("conn=1")));
    assert!(events.iter().any(|event| event.contains("conn=2")));
}

#[tokio::test]
async fn idle_client_timeout_can_continue_with_the_same_connection() {
    let (proxy_addr, _seen) = spawn_timeout_proxy(TimeoutScenario::Normal).await;

    let mut client = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;

    let timeout_frames = read_frames(&mut client).await;
    assert!(!timeout_frames.is_empty());
    assert_eq!(timeout_frames[0].tag, b'E');
    assert_eq!(
        timeout_frames[0].sqlstate(),
        Some(SqlState::OperatorIntervention)
    );
    assert_error_field(&timeout_frames[0], b'M', "idle client timed out");
    assert!(timeout_frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));

    send_query(&mut client, "select 1").await;
    let recovery_frames = read_frames(&mut client).await;
    assert!(recovery_frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));
}

#[tokio::test]
async fn idle_transaction_timeout_closes_the_client_and_discards_the_backend() {
    let (proxy_addr, mut seen) =
        spawn_timeout_proxy(TimeoutScenario::IdleTransactionThenNormal).await;

    let mut client = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;
    send_query(&mut client, "begin").await;
    let begin_frames = read_until_ready(&mut client).await;
    assert!(begin_frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::InTransaction)));

    let timeout_frames = read_frames(&mut client).await;
    assert!(!timeout_frames.is_empty());
    assert_eq!(timeout_frames[0].tag, b'E');
    assert_eq!(
        timeout_frames[0].sqlstate(),
        Some(SqlState::OperatorIntervention)
    );
    assert_error_field(&timeout_frames[0], b'M', "idle transaction timed out");

    let mut eof_probe = [0_u8; 1];
    let read = time::timeout(Duration::from_millis(200), client.read(&mut eof_probe))
        .await
        .expect("wait for timeout close")
        .expect("read after timeout");
    assert_eq!(read, 0);

    let mut second = open_client(proxy_addr, "pgkinetic", "postgres", Some("api")).await;
    send_query(&mut second, "select 1").await;
    let second_frames = read_frames(&mut second).await;
    assert!(second_frames
        .iter()
        .any(|frame| frame.ready_status() == Some(ReadyStatus::Idle)));

    let events = collect_events(&mut seen).await;
    assert!(events.iter().any(|event| event.contains("conn=1")));
    assert!(events.iter().any(|event| event.contains("conn=2")));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HoldRule {
    Never,
    Database(&'static str),
    User(&'static str),
}

impl HoldRule {
    fn matches(self, database: &str, user: &str) -> bool {
        match self {
            Self::Never => false,
            Self::Database(expected) => database == expected,
            Self::User(expected) => user == expected,
        }
    }
}

async fn spawn_proxy_with_backend(
    hold_rule: HoldRule,
) -> (SocketAddr, mpsc::Receiver<String>, Arc<Notify>) {
    spawn_proxy_with_backend_and_qos(
        hold_rule,
        QosConfig {
            max_route_in_flight: 1,
            max_route_waiters: 1,
            query_timeout_ms: 30_000,
            idle_client_timeout_ms: 300_000,
            idle_transaction_timeout_ms: 60_000,
            max_client_buffer_bytes: 1_048_576,
            max_backend_buffer_bytes: 4_194_304,
            overload_error_code: "53300".to_string(),
        },
    )
    .await
}

async fn spawn_proxy_with_backend_and_qos(
    hold_rule: HoldRule,
    qos: QosConfig,
) -> (SocketAddr, mpsc::Receiver<String>, Arc<Notify>) {
    let (sender, receiver) = mpsc::channel(128);
    let release_hold = Arc::new(Notify::new());
    let backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = backend.local_addr().expect("backend addr");

    tokio::spawn({
        let release_hold = release_hold.clone();
        async move {
            loop {
                let (mut stream, _) = backend.accept().await.expect("accept backend");
                let sender = sender.clone();
                let release_hold = release_hold.clone();
                tokio::spawn(async move {
                    let mut startup = [0_u8; 2048];
                    let read = stream.read(&mut startup).await.expect("read startup");
                    if read == 0 {
                        return;
                    }

                    let startup_packet =
                        parse_startup_packet(&startup[..read]).expect("parse startup");
                    let (database, user, application_name) = startup_fields(&startup_packet);
                    sender
                        .send(format!(
                            "startup db={database} user={user} app={}",
                            application_name.as_deref().unwrap_or("<none>")
                        ))
                        .await
                        .expect("record startup");

                    stream
                        .write_all(&auth_ok_ready())
                        .await
                        .expect("auth ready");

                    let mut buffer = BytesMut::with_capacity(4096);
                    let mut held = false;

                    loop {
                        let read = stream.read_buf(&mut buffer).await.expect("read frontend");
                        if read == 0 {
                            sender
                                .send("closed".to_string())
                                .await
                                .expect("record close");
                            return;
                        }

                        while let Some(frame) =
                            parse_frontend_frame(&mut buffer).expect("parse frontend frame")
                        {
                            if let Some(query) = parse_simple_query(&frame).expect("parse query") {
                                sender
                                    .send(format!(
                                        "query db={database} user={user} app={} sql={query}",
                                        application_name.as_deref().unwrap_or("<none>")
                                    ))
                                    .await
                                    .expect("record query");

                                if !held && hold_rule.matches(&database, &user) {
                                    held = true;
                                    sender
                                        .send(format!(
                                            "hold-started db={database} user={user} app={}",
                                            application_name.as_deref().unwrap_or("<none>")
                                        ))
                                        .await
                                        .expect("record hold");
                                    release_hold.notified().await;
                                }

                                stream.write_all(&ready_idle()).await.expect("write ready");
                            }
                        }
                    }
                });
            }
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
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 2,
            max_checkout_waiters: 4,
        },
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            recovery_mode: RecoveryMode::Recover,
            recovery_timeout_ms: 1_000,
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

    (listen_addr, receiver, release_hold)
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

async fn read_response(stream: &mut TcpStream) {
    let mut response = [0_u8; 256];
    let _ = stream.read(&mut response).await.expect("response");
}

async fn read_frames(stream: &mut TcpStream) -> Vec<BackendFrame> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];

    loop {
        match time::timeout(Duration::from_millis(250), stream.read(&mut buffer)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(read)) => bytes.extend_from_slice(&buffer[..read]),
            Ok(Err(error)) => panic!("read response: {error}"),
            Err(_) => break,
        }
    }

    let mut buffer = BytesMut::from(bytes.as_slice());
    let mut frames = Vec::new();
    while let Some(frame) = parse_backend_frame(&mut buffer).expect("parse backend frame") {
        frames.push(frame);
    }

    frames
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

fn parse_error_frame(bytes: &[u8]) -> BackendFrame {
    let mut buffer = BytesMut::from(bytes);
    parse_backend_frame(&mut buffer)
        .expect("parse backend frame")
        .expect("backend frame")
}

fn assert_error_field(frame: &BackendFrame, kind: u8, expected: &str) {
    let mut offset = 0;
    while offset < frame.payload.len() {
        let field_kind = frame.payload[offset];
        offset += 1;

        if field_kind == 0 {
            break;
        }

        let remaining = &frame.payload[offset..];
        let terminator = remaining
            .iter()
            .position(|byte| *byte == 0)
            .expect("terminated");
        let value = std::str::from_utf8(&remaining[..terminator]).expect("utf8");
        if field_kind == kind {
            assert_eq!(value, expected);
            return;
        }

        offset += terminator + 1;
    }

    panic!("missing error field {kind}");
}

async fn wait_for_event(receiver: &mut mpsc::Receiver<String>, expected: &str) -> String {
    while let Ok(Some(event)) = time::timeout(Duration::from_millis(500), receiver.recv()).await {
        if event.contains(expected) {
            return event;
        }
    }

    panic!("expected event {expected}");
}

async fn collect_events(receiver: &mut mpsc::Receiver<String>) -> Vec<String> {
    let mut events = Vec::new();
    while let Ok(Some(event)) = time::timeout(Duration::from_millis(100), receiver.recv()).await {
        events.push(event);
    }
    events
}

fn startup_fields(startup: &StartupPacket) -> (String, String, Option<String>) {
    let StartupPacket::Startup { parameters, .. } = startup else {
        panic!("expected startup packet");
    };

    let database = startup_parameter(parameters, "database")
        .expect("database")
        .to_string();
    let user = startup_parameter(parameters, "user")
        .expect("user")
        .to_string();
    let application_name = startup_parameter(parameters, "application_name").map(str::to_string);

    (database, user, application_name)
}

fn startup_parameter<'a>(parameters: &'a [(String, String)], key: &str) -> Option<&'a str> {
    parameters
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
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

fn ready_in_transaction() -> Vec<u8> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'C');
    bytes.put_i32(10);
    bytes.extend_from_slice(b"BEGIN\0");
    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'T');
    bytes.to_vec()
}
