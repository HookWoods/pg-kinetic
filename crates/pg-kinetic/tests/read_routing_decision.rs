use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::{BufMut, BytesMut};
use pg_kinetic::{
    config::{
        BackendEndpointConfig, CapacityConfig, Config, ConnectionConfig, FreshnessConfig, HaConfig,
        ObservabilityConfig, PerformanceConfig, QosConfig, ReadRoutingConfig, ReplicaConfig,
        RouteConfig,
    },
    proxy::Proxy,
    proxy_runtime::snapshot::{RouteCheckoutSnapshot, SnapshotStore},
    wire::{
        frame::parse_frontend_frame,
        message::{parse_bind_statement_name, parse_parse_message, parse_simple_query},
        protocol::{FrontendTag, ProtocolVersion},
    },
};
use pg_kinetic_core::{
    lsn::PgLsn,
    routing::{FallbackPolicy, FreshnessPolicy, ReadRoutingMode},
    session::TransactionState,
    virtual_session::ReadAfterWriteState,
};
use pg_kinetic_proxy::routing::{
    choose_routing_target, ReadRoutingPlanner, ReplicaCandidate, RouteHealthSnapshot,
    RoutingContext, RoutingReason, RoutingTarget,
};
use pg_kinetic_wire::backend::parse_backend_frame;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::sleep,
};

fn planner(
    read_routing_mode: ReadRoutingMode,
    fallback_policy: FallbackPolicy,
    freshness_policy: FreshnessPolicy,
    max_replica_lag_ms: u64,
) -> ReadRoutingPlanner {
    ReadRoutingPlanner::new(
        read_routing_mode,
        fallback_policy,
        freshness_policy,
        max_replica_lag_ms,
    )
}

fn replica(
    replica_id: u64,
    healthy: bool,
    replay_lsn: Option<PgLsn>,
    lag_ms: Option<u64>,
) -> ReplicaCandidate {
    ReplicaCandidate::new(replica_id, healthy, replay_lsn, lag_ms)
}

fn snapshot(replicas: Vec<ReplicaCandidate>) -> RouteHealthSnapshot {
    RouteHealthSnapshot::new(replicas)
}

fn context<'a>(
    sql: &'a str,
    transaction_state: TransactionState,
    session_write_lsn: Option<PgLsn>,
    health: &'a RouteHealthSnapshot,
) -> RoutingContext<'a> {
    RoutingContext::new(
        sql,
        transaction_state,
        match session_write_lsn {
            Some(session_write_lsn) => ReadAfterWriteState::Required(session_write_lsn),
            None => ReadAfterWriteState::Disabled,
        },
        health,
    )
}

fn assert_primary(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Primary { reason } => assert_eq!(reason, expected_reason),
        other => panic!("expected primary target, got {other:?}"),
    }
}

fn assert_replica(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Replica { reason, .. } => assert_eq!(reason, expected_reason),
        other => panic!("expected replica target, got {other:?}"),
    }
}

fn assert_wait(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Wait { reason } => assert_eq!(reason, expected_reason),
        other => panic!("expected wait target, got {other:?}"),
    }
}

fn assert_reject(target: RoutingTarget, expected_reason: RoutingReason) {
    match target {
        RoutingTarget::Reject { reason } => assert_eq!(reason, expected_reason),
        other => panic!("expected reject target, got {other:?}"),
    }
}

#[test]
fn routing_mode_off_sends_all_traffic_to_primary() {
    let planner = planner(
        ReadRoutingMode::Off,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::Off);
}

#[test]
fn writes_always_go_to_primary() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "INSERT INTO accounts VALUES (1)",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::WriteQuery);
}

#[test]
fn unknown_sql_always_goes_to_primary() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "???",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::UnknownQuery);
}

#[test]
fn explicit_primary_hint_sends_query_to_primary() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: primary */ SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::PrimaryHint);
}

#[test]
fn explicit_primary_hint_overrides_require_replica_mode() {
    let planner = planner(
        ReadRoutingMode::RequireReplica,
        FallbackPolicy::Reject,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: primary */ SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_primary(target, RoutingReason::PrimaryHint);
}

#[test]
fn explicit_replica_hint_routes_eligible_query_to_replica_when_freshness_permits() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        7,
        true,
        Some(PgLsn::from_parts(1, 20)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: replica */ SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(1, 1)),
            &health,
        ),
    );

    assert_replica(target, RoutingReason::ReplicaHint);
}

#[test]
fn stale_ok_bypasses_session_lsn_freshness_but_still_requires_healthy_replica() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(3, true, None, Some(5))]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: stale-ok */ SELECT 1",
            TransactionState::Idle,
            None,
            &health,
        ),
    );

    assert_replica(target, RoutingReason::StaleOkHint);
}

#[test]
fn strict_fresh_requires_session_lsn_and_lag_freshness() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        10,
    );
    let health = snapshot(vec![replica(
        9,
        true,
        Some(PgLsn::from_parts(2, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: strict-fresh */ SELECT 1",
            TransactionState::Idle,
            None,
            &health,
        ),
    );

    assert_primary(target, RoutingReason::FallbackPrimary);
}

#[test]
fn no_healthy_replica_with_primary_routes_to_primary() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        false,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_primary(target, RoutingReason::FallbackPrimary);
}

#[test]
fn no_healthy_replica_with_reject_returns_reject() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Reject,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        false,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_reject(target, RoutingReason::FallbackReject);
}

#[test]
fn stale_replica_with_strict_freshness_follows_fallback_policy() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Reject,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        10,
    );
    let health = snapshot(vec![replica(
        9,
        true,
        Some(PgLsn::from_parts(2, 10)),
        Some(100),
    )]);

    let target = choose_routing_target(
        &planner,
        context(
            "/* pg-kinetic: strict-fresh */ SELECT 1",
            TransactionState::Idle,
            Some(PgLsn::from_parts(2, 20)),
            &health,
        ),
    );

    assert_reject(target, RoutingReason::ReplicaStale);
}

#[test]
fn no_healthy_replica_follows_fallback_policy() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Wait,
        FreshnessPolicy::SessionWriteLsn,
        1_000,
    );
    let health = snapshot(vec![replica(
        1,
        false,
        Some(PgLsn::from_parts(1, 10)),
        Some(5),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_wait(target, RoutingReason::FallbackWait);
}

#[test]
fn replica_lag_beyond_limit_follows_fallback_policy() {
    let planner = planner(
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Reject,
        FreshnessPolicy::MaxReplicaLag,
        50,
    );
    let health = snapshot(vec![replica(
        1,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(500),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_reject(target, RoutingReason::FallbackReject);
}

#[test]
fn require_replica_rejects_when_no_replica_is_safe() {
    let planner = planner(
        ReadRoutingMode::RequireReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::SessionWriteLsnAndMaxLag,
        10,
    );
    let health = snapshot(vec![replica(
        2,
        true,
        Some(PgLsn::from_parts(1, 10)),
        Some(500),
    )]);

    let target = choose_routing_target(
        &planner,
        context("SELECT 1", TransactionState::Idle, None, &health),
    );

    assert_reject(target, RoutingReason::RequireReplicaMode);
}

#[tokio::test]
async fn simple_read_query_checks_out_a_replica_when_enabled_and_safe() {
    let (primary_addr, primary_events) = spawn_backend("primary").await;
    let (replica_addr, replica_events) = spawn_backend("replica").await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        Some(replica_addr),
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::None,
    )
    .await;

    run_simple_query(proxy_addr, "select 1").await;
    sleep(Duration::from_millis(50)).await;
    let checkout = checkout_snapshot(&snapshot_store).await;
    assert_replica(checkout.decision, RoutingReason::ReadCandidateQuery);

    assert!(!collect_events(&primary_events)
        .await
        .iter()
        .any(|event| event.starts_with("primary:query:")));
    let replica_events = collect_events(&replica_events).await;
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:connect"),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:query:select 1"),
        "events: {replica_events:?}"
    );
}

#[tokio::test]
async fn write_query_checks_out_primary() {
    let (primary_addr, primary_events) = spawn_backend("primary").await;
    let (replica_addr, replica_events) = spawn_backend("replica").await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        Some(replica_addr),
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::None,
    )
    .await;

    run_simple_query(proxy_addr, "insert into accounts values (1)").await;
    sleep(Duration::from_millis(50)).await;

    let primary_events = collect_events(&primary_events).await;
    assert!(primary_events
        .iter()
        .any(|event| event == "primary:connect"));
    assert!(primary_events
        .iter()
        .any(|event| event == "primary:query:insert into accounts values (1)"));
    assert!(collect_events(&replica_events).await.is_empty());

    let checkout = checkout_snapshot(&snapshot_store).await;
    assert_primary(checkout.decision, RoutingReason::WriteQuery);
}

#[tokio::test]
async fn prepared_statement_parse_bind_execute_routing_stays_consistent() {
    let (primary_addr, primary_events) = spawn_backend("primary").await;
    let (replica_addr, replica_events) = spawn_backend("replica").await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        Some(replica_addr),
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::None,
    )
    .await;

    run_extended_query(proxy_addr, "select 1").await;
    sleep(Duration::from_millis(50)).await;
    let checkout = checkout_snapshot(&snapshot_store).await;
    assert_replica(checkout.decision, RoutingReason::ReadCandidateQuery);

    assert!(!collect_events(&primary_events)
        .await
        .iter()
        .any(|event| event.starts_with("primary:query:")));
    let replica_events = collect_events(&replica_events).await;
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:connect"),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:parse:select 1"),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event.starts_with("replica:bind:")),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:execute"),
        "events: {replica_events:?}"
    );
}

#[tokio::test]
async fn read_only_transaction_uses_one_backend_role_for_the_transaction_lifetime() {
    let (primary_addr, primary_events) = spawn_backend("primary").await;
    let (replica_addr, replica_events) = spawn_backend("replica").await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        Some(replica_addr),
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::None,
    )
    .await;

    run_transaction(
        proxy_addr,
        &["begin read only", "select 1", "select 2", "commit"],
    )
    .await;
    sleep(Duration::from_millis(50)).await;
    let checkout = checkout_snapshot(&snapshot_store).await;
    assert_replica(checkout.decision, RoutingReason::ReadOnlyQuery);

    assert!(!collect_events(&primary_events)
        .await
        .iter()
        .any(|event| event.starts_with("primary:query:")));
    let replica_events = collect_events(&replica_events).await;
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:connect"),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:query:begin read only"),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:query:select 1"),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:query:select 2"),
        "events: {replica_events:?}"
    );
    assert!(
        replica_events
            .iter()
            .any(|event| event == "replica:query:commit"),
        "events: {replica_events:?}"
    );
}

#[tokio::test]
async fn fallback_to_primary_records_fallback_reason() {
    let (primary_addr, primary_events) = spawn_backend("primary").await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        None,
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Primary,
        FreshnessPolicy::None,
    )
    .await;

    run_simple_query(proxy_addr, "select 1").await;
    sleep(Duration::from_millis(50)).await;

    let primary_events = collect_events(&primary_events).await;
    assert!(primary_events
        .iter()
        .any(|event| event == "primary:connect"));
    assert!(primary_events
        .iter()
        .any(|event| event == "primary:query:select 1"));

    let checkout = checkout_snapshot(&snapshot_store).await;
    assert_primary(checkout.decision, RoutingReason::FallbackPrimary);
}

#[tokio::test]
async fn reject_fallback_returns_a_postgresql_error_response() {
    let (primary_addr, primary_events) = spawn_backend("primary").await;
    let (proxy_addr, snapshot_store) = spawn_proxy(
        primary_addr,
        None,
        ReadRoutingMode::PreferReplica,
        FallbackPolicy::Reject,
        FreshnessPolicy::None,
    )
    .await;

    let response = run_simple_query(proxy_addr, "select 1").await;
    sleep(Duration::from_millis(50)).await;

    assert!(!collect_events(&primary_events)
        .await
        .iter()
        .any(|event| event.starts_with("primary:query:")));
    assert_eq!(response.first().copied(), Some(b'E'));
    assert_eq!(error_sqlstate(&response), Some("57P03"));

    let checkout = checkout_snapshot(&snapshot_store).await;
    assert_reject(checkout.decision, RoutingReason::FallbackReject);
}

async fn spawn_proxy(
    primary_addr: SocketAddr,
    replica_addr: Option<SocketAddr>,
    read_routing_mode: ReadRoutingMode,
    fallback_policy: FallbackPolicy,
    freshness_policy: FreshnessPolicy,
) -> (SocketAddr, SnapshotStore) {
    let mut route = RouteConfig {
        primary: BackendEndpointConfig {
            address: primary_addr,
            connect_timeout_ms: 100,
            tls_mode: pg_kinetic::config::BackendTlsMode::Disable,
        },
        replicas: Vec::new(),
        read_routing: ReadRoutingConfig {
            read_routing_mode,
            fallback_policy,
        },
        freshness: FreshnessConfig {
            freshness_policy,
            max_replica_lag_ms: 1_000,
            read_after_write_timeout_ms: 500,
        },
        ha: HaConfig::default(),
    };

    if let Some(replica_addr) = replica_addr {
        route.replicas.push(ReplicaConfig {
            address: replica_addr,
            connect_timeout_ms: 100,
            tls_mode: pg_kinetic::config::BackendTlsMode::Disable,
            weight: 1,
        });
    }

    let listen = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let listen_addr = listen.local_addr().expect("listen addr");
    drop(listen);

    let config = Config {
        connection: ConnectionConfig {
            listen_addr,
            backend_addr: primary_addr,
        },
        routes: vec![route],
        runtime: Default::default(),
        capacity: CapacityConfig {
            max_clients: 10,
            max_backends: 8,
            max_checkout_waiters: 4,
        },
        pool_lifecycle: Default::default(),
        performance: PerformanceConfig {
            checkout_timeout_ms: 100,
            pool_mode: Default::default(),
            recovery_mode: pg_kinetic::recovery::RecoveryMode::Recover,
            recovery_timeout_ms: 1_000,
            backend_reset_query: String::from("DISCARD ALL"),
        },
        qos: QosConfig {
            max_route_in_flight: 100,
            max_route_waiters: 100,
            query_timeout_ms: 5_000,
            idle_client_timeout_ms: 5_000,
            idle_transaction_timeout_ms: 5_000,
            max_client_buffer_bytes: 1_048_576,
            max_backend_buffer_bytes: 4_194_304,
            overload_error_code: String::from("53300"),
        },
        admin: Default::default(),
        observability: ObservabilityConfig::default(),
        tls: Default::default(),
        auth: Default::default(),
        reload: Default::default(),
        drain: Default::default(),
        health: Default::default(),
        socket: Default::default(),
    };

    let proxy = Proxy::new(config);
    let snapshot_store = proxy.snapshot_store();
    tokio::spawn(async move {
        let _ = proxy.run().await;
    });
    sleep(Duration::from_millis(100)).await;

    (listen_addr, snapshot_store)
}

async fn spawn_backend(role: &'static str) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend_addr = listener.local_addr().expect("backend addr");

    tokio::spawn({
        let events = Arc::clone(&events);
        async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept backend");
                let events = Arc::clone(&events);
                tokio::spawn(async move {
                    handle_backend_connection(role, stream, events).await;
                });
            }
        }
    });

    (backend_addr, events)
}

async fn handle_backend_connection(
    role: &'static str,
    mut stream: TcpStream,
    events: Arc<Mutex<Vec<String>>>,
) {
    let mut startup = [0_u8; 2048];
    let read = stream.read(&mut startup).await.expect("read startup");
    if read == 0 {
        return;
    }

    events.lock().await.push(format!("{role}:connect"));
    stream
        .write_all(&auth_ok_ready())
        .await
        .expect("auth ready");

    let mut buffer = BytesMut::with_capacity(4096);
    let mut in_transaction = false;
    let mut pending_extended = false;

    loop {
        let read = stream.read_buf(&mut buffer).await.expect("read frontend");
        if read == 0 {
            return;
        }
        while let Some(frame) = parse_frontend_frame(&mut buffer).expect("parse frontend frame") {
            if let Some(query) = parse_simple_query(&frame).expect("simple query") {
                let normalized = normalize_sql(query);
                events
                    .lock()
                    .await
                    .push(format!("{role}:query:{normalized}"));
                if normalized.starts_with("begin") || normalized.starts_with("start transaction") {
                    in_transaction = true;
                    stream
                        .write_all(&ready_in_transaction())
                        .await
                        .expect("begin response");
                } else if normalized.starts_with("commit") || normalized.starts_with("rollback") {
                    in_transaction = false;
                    stream
                        .write_all(&ready_idle())
                        .await
                        .expect("commit response");
                } else if in_transaction {
                    stream
                        .write_all(&ready_in_transaction())
                        .await
                        .expect("transaction response");
                } else {
                    stream
                        .write_all(&ready_idle())
                        .await
                        .expect("query response");
                }
                continue;
            }

            if let Some(parse) = parse_parse_message(&frame).expect("parse message") {
                events
                    .lock()
                    .await
                    .push(format!("{role}:parse:{}", normalize_sql(&parse.query)));
                pending_extended = true;
                continue;
            }

            if let Some(statement_name) = parse_bind_statement_name(&frame).expect("bind message") {
                events
                    .lock()
                    .await
                    .push(format!("{role}:bind:{statement_name}"));
                pending_extended = true;
                continue;
            }

            if frame.tag == u8::from(FrontendTag::Execute) {
                events.lock().await.push(format!("{role}:execute"));
                pending_extended = true;
                continue;
            }

            if frame.tag == u8::from(FrontendTag::Sync) && pending_extended {
                pending_extended = false;
                if in_transaction {
                    stream
                        .write_all(&ready_in_transaction())
                        .await
                        .expect("extended transaction response");
                } else {
                    stream
                        .write_all(&ready_idle())
                        .await
                        .expect("extended response");
                }
            }
        }
    }
}

async fn run_simple_query(addr: SocketAddr, sql: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    read_until_ready_for_query(&mut stream, "startup response").await;

    stream.write_all(&query_packet(sql)).await.expect("query");

    read_response_bytes(&mut stream, "query response").await
}

async fn run_extended_query(addr: SocketAddr, sql: &str) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    read_until_ready_for_query(&mut stream, "startup response").await;

    stream
        .write_all(&extended_query_cycle(sql))
        .await
        .expect("extended query");

    sleep(Duration::from_millis(100)).await;
}

async fn run_transaction(addr: SocketAddr, queries: &[&str]) {
    let mut stream = TcpStream::connect(addr).await.expect("connect proxy");
    stream.write_all(&startup_packet()).await.expect("startup");

    read_until_ready_for_query(&mut stream, "startup response").await;

    for query in queries {
        stream.write_all(&query_packet(query)).await.expect("query");
        sleep(Duration::from_millis(50)).await;
    }
}

async fn checkout_snapshot(snapshot_store: &SnapshotStore) -> RouteCheckoutSnapshot {
    sleep(Duration::from_millis(50)).await;
    snapshot_store
        .route_checkout_snapshots()
        .into_iter()
        .next()
        .expect("checkout snapshot")
}

async fn collect_events(events: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    events.lock().await.clone()
}

async fn read_until_ready_for_query(stream: &mut TcpStream, context: &str) {
    let mut buffer = BytesMut::with_capacity(1024);
    loop {
        if parse_backend_frame(&mut buffer)
            .expect(context)
            .and_then(|frame| frame.ready_status())
            .is_some()
        {
            return;
        }

        let read = stream.read_buf(&mut buffer).await.expect(context);
        assert!(read > 0, "{context}: client disconnected");
    }
}

async fn read_response_bytes(stream: &mut TcpStream, context: &str) -> Vec<u8> {
    let mut response = Vec::new();
    let mut buffer = BytesMut::with_capacity(1024);
    loop {
        let before = buffer.len();
        let read = stream.read_buf(&mut buffer).await.expect(context);
        assert!(read > 0, "{context}: client disconnected");
        response.extend_from_slice(&buffer[before..]);

        while let Some(frame) = parse_backend_frame(&mut buffer).expect(context) {
            if frame.ready_status().is_some() {
                return response;
            }
        }
    }
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
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

fn extended_query_cycle(sql: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(parse_frame("stmt", sql));
    bytes.extend(bind_frame("portal", "stmt"));
    bytes.extend(execute_frame("portal"));
    bytes.extend(sync_packet());
    bytes
}

fn parse_frame(statement_name: &str, sql: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(statement_name.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(sql.as_bytes());
    payload.put_u8(0);
    payload.put_i16(0);
    frontend_frame(b'P', payload)
}

fn bind_frame(portal_name: &str, statement_name: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(statement_name.as_bytes());
    payload.put_u8(0);
    payload.put_i16(0);
    payload.put_i16(0);
    payload.put_i16(0);
    frontend_frame(b'B', payload)
}

fn execute_frame(portal_name: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.put_u8(0);
    payload.put_i32(0);
    frontend_frame(b'E', payload)
}

fn sync_packet() -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_u8(b'S');
    packet.put_i32(4);
    packet.to_vec()
}

fn frontend_frame(tag: u8, payload: BytesMut) -> Vec<u8> {
    let mut packet = BytesMut::new();
    packet.put_u8(tag);
    packet.put_i32((payload.len() + 4) as i32);
    packet.extend_from_slice(&payload);
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

fn error_sqlstate(response: &[u8]) -> Option<&'static str> {
    let mut buffer = BytesMut::from(response);
    while let Some(frame) = parse_backend_frame(&mut buffer).ok().flatten() {
        if let Some(sqlstate) = frame.sqlstate() {
            return Some(sqlstate.as_str());
        }
    }

    None
}
