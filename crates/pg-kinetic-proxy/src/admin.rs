use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::Context;
use bytes::{BufMut, BytesMut};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use tokio_rustls::rustls::ServerConfig;

use crate::{
    config::{Config, MultiShardPolicyConfig, ShardScopeConfig, ShardTargetConfig, ShardingConfig},
    drain::DrainController,
    proxy::{read_startup_packet, ClientConnection, StartupRead},
    reload,
    snapshot::{
        AdaptiveOutcomeSnapshot, AdaptiveRecommendationSnapshot, BackpressureSnapshot,
        BenchmarkRunSnapshot, ClientSnapshot, LimitsSnapshot, MirrorSummarySnapshot,
        NodeSummaryRole, NodeSummarySnapshot, PerformanceSnapshot, PinningSnapshot,
        PolicyReloadSnapshot, PolicyStatusSnapshot, PoolSnapshot, PreparedSnapshot,
        RecoverySnapshot, ReplicaHealthSnapshot, RouteCheckoutSnapshot, RouteMapReloadSnapshot,
        RoutePolicySnapshot, RouteSnapshot, RuntimeSnapshot, ServerSnapshot, SettingsSnapshot,
        ShardLifecycleSnapshot, ShardMigrationSafetySnapshot, SnapshotStore,
    },
    socket, telemetry,
};
use pg_kinetic_core::{
    admin::{
        parse_admin_command, AdminColumn, AdminColumnType, AdminCommand, AdminRow, AdminTable,
        AdminView,
    },
    lsn::PgLsn,
    performance::{PerformanceBudget, PerformanceRegressionThreshold, ProcessMetricKind},
    recovery::{RecoveryAction, RecoveryTrigger},
    route::RouteKey,
    runtime::{ReadinessState, RuntimeLifecycleState},
    session::PinReason,
    sharding::ShardLifecycleState,
};
use pg_kinetic_wire::{
    admin::{build_admin_table_response, AdminWireColumn, AdminWireType},
    backend::build_error_response,
    frame::parse_frontend_frame,
    message::parse_simple_query,
    protocol::{BackendTag, FrontendTag, ReadyStatusByte},
    sqlstate::SqlState,
    startup::{parse_startup_packet, StartupPacket},
};

const ADMIN_AUTH_SQLSTATE: &str = "28000";
const ADMIN_UNSUPPORTED_SQLSTATE: &str = "0A000";

#[derive(Debug)]
struct AdminState {
    config: Config,
    client_tls_server_config: Option<Arc<ServerConfig>>,
    drain: Arc<DrainController>,
    snapshot_store: SnapshotStore,
    client_slots: Arc<Semaphore>,
}

#[derive(Debug)]
enum AdminRequest {
    Query(pg_kinetic_wire::frame::FrontendFrame),
    Terminate,
    Unsupported,
    BufferLimitExceeded,
    TimedOut,
}

pub async fn spawn(
    listen_addr: SocketAddr,
    config: Config,
    drain: Arc<DrainController>,
    snapshot_store: SnapshotStore,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("bind admin listener {listen_addr}"))?;
    tracing::info!(%listen_addr, "admin listener enabled");

    let client_tls_server_config = reload::load_client_tls_server_config(&config)?;
    if config.admin.admin_require_tls && client_tls_server_config.is_none() {
        anyhow::bail!("admin TLS is required but client TLS is disabled");
    }

    let state = Arc::new(AdminState {
        client_slots: Arc::new(Semaphore::new(config.admin.admin_max_clients)),
        config,
        client_tls_server_config,
        drain,
        snapshot_store,
    });

    Ok(tokio::spawn(async move {
        run_server(listener, state).await;
    }))
}

async fn run_server(listener: TcpListener, state: Arc<AdminState>) {
    loop {
        let (stream, client_addr) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(error = %error, "admin listener accept failed");
                continue;
            }
        };

        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, client_addr, state).await {
                tracing::warn!(%client_addr, error = %error, "admin connection closed with error");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    client_addr: SocketAddr,
    state: Arc<AdminState>,
) -> anyhow::Result<()> {
    let socket_options = socket::SocketOptions::from(&state.config.socket);
    socket::apply_socket_options(&stream, &socket_options, "admin")
        .context("apply admin socket options")?;

    let mut client = ClientConnection::new(stream);
    crate::metrics::increment_client_connections();

    let Some(drain_guard) = state.drain.try_enter_client() else {
        reject_during_drain(&mut client).await?;
        tracing::info!(%client_addr, "rejected admin client during drain");
        return Ok(());
    };

    let permit: OwnedSemaphorePermit = state.client_slots.clone().acquire_owned().await?;
    let _permit = permit;
    let _drain_guard = drain_guard;

    handle_session(&mut client, client_addr, &state).await
}

async fn handle_session(
    client: &mut ClientConnection,
    client_addr: SocketAddr,
    state: &AdminState,
) -> anyhow::Result<()> {
    let phase_recorder = telemetry::shared_phase_timing_recorder();
    let admin_timeout = Duration::from_millis(state.config.admin.admin_query_timeout_ms);
    let startup_packet = match read_startup_packet(
        client,
        state.config.tls.client_tls_mode,
        state.client_tls_server_config.as_ref(),
        admin_timeout,
        state.config.qos.max_client_buffer_bytes,
        phase_recorder.as_ref(),
    )
    .await
    .with_context(|| format!("admin client {client_addr}"))?
    {
        StartupRead::Packet(packet) => packet,
        StartupRead::ClientClosed => return Ok(()),
        StartupRead::TimedOut => {
            error_response_and_ready(
                client,
                SqlState::QueryCanceled.as_str(),
                "admin startup timed out",
                ReadyStatusByte::Idle,
            )
            .await?;
            return Ok(());
        }
        StartupRead::BufferLimitExceeded => return Ok(()),
    };

    let startup_user = startup_user(&startup_packet)?;
    if let Some(allowed_user) = state.config.admin.admin_allowed_user.as_deref() {
        if startup_user != allowed_user {
            reject_admin_user(client, allowed_user).await?;
            return Ok(());
        }
    }

    if state.config.admin.admin_require_tls && !client.is_tls() {
        error_response_and_ready(
            client,
            ADMIN_AUTH_SQLSTATE,
            "admin endpoint requires TLS",
            ReadyStatusByte::Idle,
        )
        .await?;
        return Ok(());
    }

    client
        .write_all(&startup_ok_response())
        .await
        .context("write admin startup response")?;

    let mut buffer = BytesMut::with_capacity(8 * 1024);
    loop {
        match read_request(
            client,
            &mut buffer,
            admin_timeout,
            state.config.qos.max_client_buffer_bytes,
        )
        .await?
        {
            AdminRequest::Query(frame) => {
                let Some(sql) = parse_simple_query(&frame)? else {
                    error_response_and_ready(
                        client,
                        ADMIN_UNSUPPORTED_SQLSTATE,
                        "admin endpoint only supports simple query protocol",
                        ReadyStatusByte::Idle,
                    )
                    .await?;
                    continue;
                };

                match parse_admin_command(sql) {
                    AdminCommand::Show(view) => {
                        if let Some(response) = render_admin_view(state, view) {
                            client
                                .write_all(&response)
                                .await
                                .context("write admin view response")?;
                        } else {
                            error_response_and_ready(
                                client,
                                ADMIN_UNSUPPORTED_SQLSTATE,
                                &format!("admin view {} is not implemented", view.as_str()),
                                ReadyStatusByte::Idle,
                            )
                            .await?;
                        }
                    }
                    AdminCommand::Unknown(sql) => {
                        error_response_and_ready(
                            client,
                            ADMIN_UNSUPPORTED_SQLSTATE,
                            &format!("unsupported admin command: {sql}"),
                            ReadyStatusByte::Idle,
                        )
                        .await?;
                    }
                }
            }
            AdminRequest::Terminate => return Ok(()),
            AdminRequest::Unsupported => {
                error_response_and_ready(
                    client,
                    ADMIN_UNSUPPORTED_SQLSTATE,
                    "admin endpoint only supports simple query protocol",
                    ReadyStatusByte::Idle,
                )
                .await?;
            }
            AdminRequest::BufferLimitExceeded => return Ok(()),
            AdminRequest::TimedOut => {
                error_response_and_ready(
                    client,
                    SqlState::QueryCanceled.as_str(),
                    "admin query timed out",
                    ReadyStatusByte::Idle,
                )
                .await?;
                return Ok(());
            }
        }
    }
}

async fn read_request(
    client: &mut ClientConnection,
    buffer: &mut BytesMut,
    idle_timeout: Duration,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<AdminRequest> {
    loop {
        if let Some(frame) = parse_frontend_frame(buffer)? {
            return Ok(match frame.tag {
                tag if tag == u8::from(FrontendTag::Query) => AdminRequest::Query(frame),
                tag if tag == u8::from(FrontendTag::Terminate) => AdminRequest::Terminate,
                _ => AdminRequest::Unsupported,
            });
        }

        if buffer.len() >= max_client_buffer_bytes {
            return Ok(AdminRequest::BufferLimitExceeded);
        }

        match timeout(idle_timeout, client.read_buf(buffer)).await {
            Ok(Ok(0)) => return Ok(AdminRequest::Terminate),
            Ok(Ok(_)) => {
                if buffer.len() > max_client_buffer_bytes {
                    return Ok(AdminRequest::BufferLimitExceeded);
                }
            }
            Ok(Err(error)) => return Err(error).context("read admin client"),
            Err(_) => return Ok(AdminRequest::TimedOut),
        }
    }
}

fn startup_user(startup_packet: &[u8]) -> anyhow::Result<String> {
    let startup = parse_startup_packet(startup_packet).context("parse admin startup packet")?;
    let StartupPacket::Startup { parameters, .. } = startup else {
        anyhow::bail!("unexpected startup packet kind");
    };

    parameters
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("user"))
        .map(|(_, value)| value.clone())
        .context("admin startup packet missing user")
}

async fn reject_admin_user(
    client: &mut ClientConnection,
    allowed_user: &str,
) -> anyhow::Result<()> {
    error_response_and_ready(
        client,
        ADMIN_AUTH_SQLSTATE,
        &format!("admin access restricted to user {allowed_user}"),
        ReadyStatusByte::Idle,
    )
    .await
}

async fn reject_during_drain(client: &mut ClientConnection) -> anyhow::Result<()> {
    error_response_and_ready(
        client,
        SqlState::OperatorIntervention.as_str(),
        "proxy is draining",
        ReadyStatusByte::Idle,
    )
    .await?;
    client
        .shutdown()
        .await
        .context("shutdown admin client during drain")
}

async fn error_response_and_ready(
    client: &mut ClientConnection,
    sqlstate: &str,
    message: &str,
    ready_status: ReadyStatusByte,
) -> anyhow::Result<()> {
    let error = build_error_response(sqlstate, message);
    client
        .write_all(&error)
        .await
        .context("write admin error response")?;
    client
        .write_all(&ready_for_query(ready_status))
        .await
        .context("write admin ready response")
}

fn startup_ok_response() -> BytesMut {
    let mut response = BytesMut::new();
    response.put_u8(u8::from(BackendTag::Authentication));
    response.put_i32(8);
    response.put_i32(0);
    response.extend_from_slice(&ready_for_query(ReadyStatusByte::Idle));
    response
}

fn ready_for_query(status: ReadyStatusByte) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(u8::from(status));
    bytes
}

fn render_admin_view(state: &AdminState, view: AdminView) -> Option<BytesMut> {
    let sharding_snapshot = state.snapshot_store.sharding_snapshot();
    let table = match view {
        AdminView::Clients => clients_table(
            &state.snapshot_store.client_snapshots(),
            &state.snapshot_store.route_checkout_snapshots(),
        ),
        AdminView::Pools => pools_table(
            &state.snapshot_store.pool_snapshot(),
            &state.snapshot_store.performance_snapshot(),
        ),
        AdminView::Servers => servers_table(
            &state.snapshot_store.server_snapshots(),
            &state.snapshot_store.replica_health_snapshots(),
        ),
        AdminView::Runtime => runtime_table(state.snapshot_store.runtime_snapshot(), &state.config),
        AdminView::Nodes => nodes_table(
            state.snapshot_store.node_snapshots(),
            state.snapshot_store.runtime_snapshot(),
        ),
        AdminView::Mirroring => {
            mirroring_table(state.snapshot_store.mirror_snapshot(), &state.config)
        }
        AdminView::Adaptive => adaptive_table(
            state
                .config
                .runtime
                .production
                .adaptive
                .adaptive_mode
                .as_str(),
            &state.config.runtime.production.adaptive.apply,
            &state.config.runtime.production.adaptive.guardrail,
            &state.snapshot_store.adaptive_recommendation_snapshots(),
            &state.snapshot_store.adaptive_outcome_snapshots(),
        ),
        AdminView::Benchmarks => benchmarks_table(
            &state.snapshot_store.benchmark_run_snapshots(),
            &state.snapshot_store.performance_snapshot(),
        ),
        AdminView::Performance => {
            state.snapshot_store.refresh_performance_snapshot();
            performance_table(&state.snapshot_store.performance_snapshot())
        }
        AdminView::Prepared => prepared_table(
            &state.snapshot_store.prepared_snapshot(),
            &state.snapshot_store.performance_snapshot(),
        ),
        AdminView::Pinning => pinning_table(&state.snapshot_store.pinning_snapshots()),
        AdminView::Recovery => recovery_table(&state.snapshot_store.recovery_snapshots()),
        AdminView::Backpressure => {
            backpressure_table(&state.snapshot_store.backpressure_snapshots())
        }
        AdminView::Policies => policies_table(
            state.snapshot_store.policy_status_snapshot(),
            &state.snapshot_store.policy_reload_snapshots(),
        ),
        AdminView::PolicyDecisions => {
            policy_audit_table(&state.snapshot_store.policy_decision_events(), false)
        }
        AdminView::PolicyAudit => {
            policy_audit_table(&state.snapshot_store.policy_audit_events(), true)
        }
        AdminView::Routes => routes_table(
            latest_route_map_generation(&state.snapshot_store.route_map_reload_snapshots()),
            sharding_snapshot.sharding_enabled,
            &state.snapshot_store.route_snapshots(),
            &state.snapshot_store.route_policy_snapshots(),
        ),
        AdminView::RouteMaps => route_maps_table(&sharding_snapshot),
        AdminView::Shards => shards_table(
            &sharding_snapshot,
            &state.snapshot_store.shard_lifecycle_snapshots(),
        ),
        AdminView::Migrations => {
            migrations_table(&state.snapshot_store.shard_migration_safety_snapshots())
        }
        AdminView::Settings => settings_table(&state.snapshot_store.settings_snapshot()),
        AdminView::Limits => limits_table(&state.snapshot_store.limits_snapshot(), &state.config),
    };

    Some(admin_table_response(table))
}

fn clients_table(
    clients: &[ClientSnapshot],
    route_checkouts: &[RouteCheckoutSnapshot],
) -> AdminTable {
    let route_checkouts = route_checkouts
        .iter()
        .map(|snapshot| (snapshot.route_key.clone(), snapshot))
        .collect::<HashMap<_, _>>();

    admin_table(
        AdminView::Clients,
        &[
            ("client_id", AdminColumnType::Int8),
            ("user", AdminColumnType::Text),
            ("database", AdminColumnType::Text),
            ("application_name", AdminColumnType::Text),
            ("route_key", AdminColumnType::Text),
            ("state", AdminColumnType::Text),
            ("connected_duration_ms", AdminColumnType::Int8),
            ("current_target_role", AdminColumnType::Text),
            ("required_session_write_lsn", AdminColumnType::Text),
        ],
        clients
            .iter()
            .map(|snapshot| {
                let checkout = snapshot
                    .route_key
                    .as_ref()
                    .and_then(|route_key| route_checkouts.get(route_key).copied());

                AdminRow::new(vec![
                    snapshot.client_id.to_string(),
                    optional_text(snapshot.user.as_deref()),
                    optional_text(snapshot.database.as_deref()),
                    optional_text(snapshot.application_name.as_deref()),
                    optional_route_key(snapshot.route_key.as_ref()),
                    snapshot.state.clone(),
                    duration_millis(snapshot.connected_duration),
                    optional_text(
                        checkout
                            .and_then(|checkout| checkout.decision.clone().target_role())
                            .map(|role| role.as_str()),
                    ),
                    optional_pglsn(
                        checkout.and_then(|checkout| checkout.required_session_write_lsn),
                    ),
                ])
            })
            .collect(),
    )
}

fn pools_table(pool: &PoolSnapshot, performance: &PerformanceSnapshot) -> AdminTable {
    admin_table(
        AdminView::Pools,
        &[
            ("route_key", AdminColumnType::Text),
            ("max_backends", AdminColumnType::Int8),
            ("active_backends", AdminColumnType::Int8),
            ("idle_backends", AdminColumnType::Int8),
            ("waiting_clients", AdminColumnType::Int8),
            ("checkout_lock_wait_ms", AdminColumnType::Float8),
        ],
        vec![AdminRow::new(vec![
            String::from("global"),
            pool.configured_backends.to_string(),
            pool.active_backends.to_string(),
            pool.idle_backends.to_string(),
            pool.waiting_clients.to_string(),
            optional_metric_value(performance.pool_checkout_lock_wait_ms),
        ])],
    )
}

fn servers_table(
    servers: &[ServerSnapshot],
    replica_health: &[ReplicaHealthSnapshot],
) -> AdminTable {
    let replica_health = replica_health
        .iter()
        .map(|snapshot| (snapshot.endpoint_id, snapshot))
        .collect::<HashMap<_, _>>();

    admin_table(
        AdminView::Servers,
        &[
            ("backend_id", AdminColumnType::Int8),
            ("route_key", AdminColumnType::Text),
            ("state", AdminColumnType::Text),
            ("last_checkout_age_ms", AdminColumnType::Int8),
            ("in_transaction", AdminColumnType::Bool),
            ("endpoint_role", AdminColumnType::Text),
            ("detected_role", AdminColumnType::Text),
            ("health", AdminColumnType::Text),
            ("lag_ms", AdminColumnType::Text),
            ("replay_lsn", AdminColumnType::Text),
            ("last_probe_age_ms", AdminColumnType::Text),
        ],
        servers
            .iter()
            .map(|snapshot| {
                let health = replica_health.get(&snapshot.backend_id).copied();

                AdminRow::new(vec![
                    snapshot.backend_id.to_string(),
                    optional_route_key(snapshot.route_key.as_ref()),
                    snapshot.state.clone(),
                    duration_millis(snapshot.age),
                    snapshot.in_transaction.to_string(),
                    optional_text(health.map(|snapshot| snapshot.expected_role.as_str())),
                    optional_text(health.map(|snapshot| snapshot.role.state.as_str())),
                    optional_text(health.map(|snapshot| snapshot.health.state.as_str())),
                    optional_duration(health.and_then(|snapshot| snapshot.lag_duration)),
                    optional_pglsn(health.and_then(|snapshot| snapshot.replay_lsn)),
                    optional_probe_age(
                        health.and_then(|snapshot| snapshot.last_successful_probe_at),
                    ),
                ])
            })
            .collect(),
    )
}

fn runtime_table(runtime: Option<RuntimeSnapshot>, config: &Config) -> AdminTable {
    let runtime = runtime.unwrap_or_else(|| {
        RuntimeSnapshot::new(
            config.runtime.node.node_id.clone(),
            RuntimeLifecycleState::Starting,
            ReadinessState::NotReady,
            config.runtime.engine.runtime_engine,
            Duration::ZERO,
        )
    });

    admin_table(
        AdminView::Runtime,
        &[
            ("node_id", AdminColumnType::Text),
            ("lifecycle_state", AdminColumnType::Text),
            ("readiness_state", AdminColumnType::Text),
            ("runtime_engine", AdminColumnType::Text),
            ("uptime_ms", AdminColumnType::Int8),
        ],
        vec![AdminRow::new(vec![
            runtime.node_id.as_str().to_string(),
            runtime.lifecycle_state.as_str().to_string(),
            runtime.readiness_state.as_str().to_string(),
            runtime.runtime_engine.as_str().to_string(),
            duration_millis(runtime.uptime),
        ])],
    )
}

fn nodes_table(nodes: Vec<NodeSummarySnapshot>, runtime: Option<RuntimeSnapshot>) -> AdminTable {
    let rows = if nodes.is_empty() {
        runtime
            .map(|runtime| {
                vec![AdminRow::new(vec![
                    NodeSummaryRole::Local.as_str().to_string(),
                    runtime.node_id.as_str().to_string(),
                    runtime.lifecycle_state.as_str().to_string(),
                    runtime.readiness_state.as_str().to_string(),
                    String::from("healthy"),
                    String::from("0"),
                    String::from("0"),
                    duration_millis(runtime.uptime),
                    String::from("false"),
                ])]
            })
            .unwrap_or_default()
    } else {
        nodes
            .into_iter()
            .map(|snapshot| {
                AdminRow::new(vec![
                    snapshot.role.as_str().to_string(),
                    snapshot.node_id.as_str().to_string(),
                    snapshot.lifecycle_state.as_str().to_string(),
                    snapshot.readiness_state.as_str().to_string(),
                    snapshot.health.as_str().to_string(),
                    snapshot.route_map_generation.to_string(),
                    snapshot.policy_generation.to_string(),
                    duration_millis(snapshot.heartbeat_age),
                    snapshot.overloaded.to_string(),
                ])
            })
            .collect()
    };

    admin_table(
        AdminView::Nodes,
        &[
            ("role", AdminColumnType::Text),
            ("node_id", AdminColumnType::Text),
            ("lifecycle_state", AdminColumnType::Text),
            ("readiness_state", AdminColumnType::Text),
            ("health", AdminColumnType::Text),
            ("route_map_generation_id", AdminColumnType::Int8),
            ("policy_generation_id", AdminColumnType::Int8),
            ("heartbeat_age_ms", AdminColumnType::Int8),
            ("overloaded", AdminColumnType::Bool),
        ],
        rows,
    )
}

fn mirroring_table(snapshot: Option<MirrorSummarySnapshot>, _config: &Config) -> AdminTable {
    let snapshot = snapshot.unwrap_or_else(|| {
        MirrorSummarySnapshot::new(pg_kinetic_core::mirror::MirrorMode::Off, 0.0)
    });
    admin_table(
        AdminView::Mirroring,
        &[
            ("mode", AdminColumnType::Text),
            ("sample_rate", AdminColumnType::Float8),
            ("in_flight", AdminColumnType::Int8),
            ("dropped", AdminColumnType::Int8),
            ("timeout_total", AdminColumnType::Int8),
            ("decisions_total", AdminColumnType::Int8),
            ("mirrored_total", AdminColumnType::Int8),
            ("skipped_total", AdminColumnType::Int8),
            ("rejected_total", AdminColumnType::Int8),
        ],
        vec![AdminRow::new(vec![
            snapshot.mode.as_str().to_string(),
            format!("{:.3}", snapshot.sample_rate),
            snapshot.in_flight.to_string(),
            snapshot.dropped_total.to_string(),
            snapshot.timed_out_total.to_string(),
            mirror_decisions_total(&snapshot).to_string(),
            snapshot.mirrored_total.to_string(),
            snapshot.skipped_total.to_string(),
            snapshot.rejected_total.to_string(),
        ])],
    )
}

fn adaptive_table(
    mode: &'static str,
    apply: &crate::config::AdaptiveApplyConfig,
    guardrail: &crate::config::AdaptiveGuardrailConfig,
    recommendations: &[AdaptiveRecommendationSnapshot],
    outcomes: &[AdaptiveOutcomeSnapshot],
) -> AdminTable {
    let guardrails = adaptive_guardrails_label(mode, apply, guardrail);
    let recommendation_rows = recommendations
        .iter()
        .rev()
        .take(5)
        .cloned()
        .collect::<Vec<_>>();
    let outcome_rows = outcomes.iter().rev().take(5).cloned().collect::<Vec<_>>();
    let max_rows = recommendation_rows.len().max(outcome_rows.len()).max(1);

    let rows = (0..max_rows)
        .map(|index| {
            let recommendation = recommendation_rows.get(index);
            let outcome = outcome_rows.get(index);
            AdminRow::new(vec![
                mode.to_string(),
                recommendation_summary(recommendation),
                apply_status_summary(outcome),
                guardrails.clone(),
            ])
        })
        .collect();

    admin_table(
        AdminView::Adaptive,
        &[
            ("mode", AdminColumnType::Text),
            ("latest_recommendation", AdminColumnType::Text),
            ("apply_status", AdminColumnType::Text),
            ("guardrails", AdminColumnType::Text),
        ],
        rows,
    )
}

fn benchmarks_table(
    runs: &[BenchmarkRunSnapshot],
    performance: &PerformanceSnapshot,
) -> AdminTable {
    let rows = runs
        .iter()
        .rev()
        .take(8)
        .flat_map(|run| {
            run.results.iter().map(move |result| {
                AdminRow::new(vec![
                    run.scenario.metric_label().to_string(),
                    result.target().metric_label().to_string(),
                    result.target().comparison().as_str().to_string(),
                    result.driver().as_str().to_string(),
                    result.duration_ms().to_string(),
                    benchmark_metric_value(result.metrics().p50_ms()),
                    benchmark_metric_value(result.metrics().p95_ms()),
                    benchmark_metric_value(result.metrics().p99_ms()),
                    benchmark_metric_value(result.metrics().throughput_qps()),
                    benchmark_metric_value(result.metrics().error_rate()),
                    String::from(pg_kinetic_core::benchmark::REDACTED_BENCHMARK_DETAIL_LABEL),
                    String::from(pg_kinetic_core::benchmark::REDACTED_BENCHMARK_DETAIL_LABEL),
                    run.scenario.workload().as_str().to_string(),
                    benchmark_matrix_targets(&run.scenario),
                    performance
                        .comparison_outcome(run.scenario.name(), result.target().comparison())
                        .map_or_else(|| String::from("unknown"), |outcome| outcome.to_string()),
                ])
            })
        })
        .collect();

    admin_table(
        AdminView::Benchmarks,
        &[
            ("scenario", AdminColumnType::Text),
            ("target", AdminColumnType::Text),
            ("comparison", AdminColumnType::Text),
            ("driver", AdminColumnType::Text),
            ("duration_ms", AdminColumnType::Int8),
            ("p50_ms", AdminColumnType::Float8),
            ("p95_ms", AdminColumnType::Float8),
            ("p99_ms", AdminColumnType::Float8),
            ("throughput_qps", AdminColumnType::Float8),
            ("error_rate", AdminColumnType::Float8),
            ("cpu_label", AdminColumnType::Text),
            ("memory_label", AdminColumnType::Text),
            ("workload", AdminColumnType::Text),
            ("matrix_targets", AdminColumnType::Text),
            ("comparison_outcome", AdminColumnType::Text),
        ],
        rows,
    )
}

fn performance_table(performance: &PerformanceSnapshot) -> AdminTable {
    let rows = if performance.budgets.is_empty() {
        vec![performance_row(None, performance)]
    } else {
        performance
            .budgets
            .iter()
            .map(|budget| performance_row(Some(budget), performance))
            .collect()
    };

    admin_table(
        AdminView::Performance,
        &[
            ("metric", AdminColumnType::Text),
            ("warning_threshold", AdminColumnType::Text),
            ("failure_threshold", AdminColumnType::Text),
            ("observed_value", AdminColumnType::Float8),
            ("baseline_value", AdminColumnType::Float8),
            ("regression_outcome", AdminColumnType::Text),
            ("profile_status", AdminColumnType::Text),
            ("process_status", AdminColumnType::Text),
            ("process_cpu_seconds", AdminColumnType::Float8),
            ("process_resident_memory_bytes", AdminColumnType::Float8),
            ("cpu_per_query", AdminColumnType::Float8),
            ("memory_per_client_bytes", AdminColumnType::Float8),
            ("protocol_buffer_copies", AdminColumnType::Int8),
            ("pool_checkout_lock_wait_ms", AdminColumnType::Float8),
            ("prepared_cache_hits", AdminColumnType::Int8),
            ("prepared_cache_misses", AdminColumnType::Int8),
            ("observability_hot_path_allocations", AdminColumnType::Int8),
            ("idle_clients", AdminColumnType::Int8),
        ],
        rows,
    )
}

fn performance_row(
    budget: Option<&PerformanceBudget>,
    performance: &PerformanceSnapshot,
) -> AdminRow {
    let regression = budget.and_then(|budget| performance.latest_regression_for(budget.metric()));
    let process_sample = performance.process_sample.as_ref();
    AdminRow::new(vec![
        budget.map_or_else(
            || String::from("none"),
            |budget| budget.metric().to_string(),
        ),
        budget.map_or_else(
            || String::from("none"),
            |budget| performance_threshold_label(budget.warning_threshold()),
        ),
        budget.map_or_else(
            || String::from("none"),
            |budget| performance_threshold_label(budget.failure_threshold()),
        ),
        regression.map_or_else(
            || benchmark_metric_value(0.0),
            |result| benchmark_metric_value(result.observed_value()),
        ),
        regression.map_or_else(
            || benchmark_metric_value(0.0),
            |result| {
                result
                    .baseline_value()
                    .map_or_else(|| benchmark_metric_value(0.0), benchmark_metric_value)
            },
        ),
        regression.map_or_else(
            || String::from("unknown"),
            |result| result.outcome().to_string(),
        ),
        performance.profile_status.to_string(),
        performance.process_status.to_string(),
        process_metric_value(process_sample, ProcessMetricKind::CpuTime),
        process_metric_value(process_sample, ProcessMetricKind::ResidentMemory),
        optional_metric_value(performance.cpu_per_query),
        optional_metric_value(performance.memory_per_client_bytes),
        performance.protocol_buffer_copies.to_string(),
        optional_metric_value(performance.pool_checkout_lock_wait_ms),
        performance.prepared_cache_hits.to_string(),
        performance.prepared_cache_misses.to_string(),
        performance.observability_hot_path_allocations.to_string(),
        performance.idle_clients.to_string(),
    ])
}

fn benchmark_matrix_targets(scenario: &pg_kinetic_core::benchmark::BenchmarkScenario) -> String {
    scenario
        .targets()
        .iter()
        .map(|target| target.comparison().as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn performance_threshold_label(threshold: PerformanceRegressionThreshold) -> String {
    format!("{}:{:.3}", threshold.as_str(), threshold.value())
}

fn process_metric_value(
    sample: Option<&pg_kinetic_core::performance::ProcessMetricSample>,
    metric: ProcessMetricKind,
) -> String {
    sample
        .and_then(|sample| sample.metric(metric).as_f64())
        .map_or_else(|| benchmark_metric_value(0.0), benchmark_metric_value)
}

fn optional_metric_value(value: Option<f64>) -> String {
    value.map_or_else(|| benchmark_metric_value(0.0), benchmark_metric_value)
}

fn prepared_table(prepared: &PreparedSnapshot, performance: &PerformanceSnapshot) -> AdminTable {
    admin_table(
        AdminView::Prepared,
        &[
            ("session_id", AdminColumnType::Int8),
            ("client_statement_name", AdminColumnType::Text),
            ("backend_statement_name", AdminColumnType::Text),
            ("materialized_backend_count", AdminColumnType::Int8),
            ("invalidation_count", AdminColumnType::Int8),
            ("prepared_cache_hits", AdminColumnType::Int8),
            ("prepared_cache_misses", AdminColumnType::Int8),
        ],
        prepared
            .statements
            .iter()
            .map(|statement| {
                AdminRow::new(vec![
                    statement.session_id.to_string(),
                    statement.client_statement_name.clone(),
                    statement.backend_statement_name.clone(),
                    statement.materialized_backend_count.to_string(),
                    statement.invalidation_count.to_string(),
                    performance.prepared_cache_hits.to_string(),
                    performance.prepared_cache_misses.to_string(),
                ])
            })
            .collect(),
    )
}

fn pinning_table(pinnings: &[PinningSnapshot]) -> AdminTable {
    admin_table(
        AdminView::Pinning,
        &[
            ("client_id", AdminColumnType::Int8),
            ("backend_id", AdminColumnType::Int8),
            ("route_key", AdminColumnType::Text),
            ("reason", AdminColumnType::Text),
            ("duration_ms", AdminColumnType::Int8),
        ],
        pinnings
            .iter()
            .map(|snapshot| {
                AdminRow::new(vec![
                    snapshot.client_id.to_string(),
                    optional_u64(snapshot.backend_id),
                    optional_route_key(snapshot.route_key.as_ref()),
                    pin_reason_label(snapshot.reason).to_string(),
                    duration_millis(snapshot.duration),
                ])
            })
            .collect(),
    )
}

fn recovery_table(recoveries: &[RecoverySnapshot]) -> AdminTable {
    admin_table(
        AdminView::Recovery,
        &[
            ("trigger", AdminColumnType::Text),
            ("action", AdminColumnType::Text),
            ("outcome", AdminColumnType::Text),
            ("count", AdminColumnType::Int8),
            ("last_error", AdminColumnType::Text),
        ],
        recoveries
            .iter()
            .map(|snapshot| {
                AdminRow::new(vec![
                    recovery_trigger_label(snapshot.trigger).to_string(),
                    recovery_action_label(snapshot.action).to_string(),
                    snapshot.outcome.as_str().to_string(),
                    snapshot.count.to_string(),
                    optional_text(snapshot.last_error.as_deref()),
                ])
            })
            .collect(),
    )
}

fn backpressure_table(backpressure: &[BackpressureSnapshot]) -> AdminTable {
    admin_table(
        AdminView::Backpressure,
        &[
            ("route_key", AdminColumnType::Text),
            ("waiting", AdminColumnType::Int8),
            ("in_flight", AdminColumnType::Int8),
            ("rejected", AdminColumnType::Int8),
            ("timed_out", AdminColumnType::Int8),
            ("canceled", AdminColumnType::Int8),
        ],
        backpressure
            .iter()
            .map(|snapshot| {
                AdminRow::new(vec![
                    optional_route_key(Some(&snapshot.route_key)),
                    snapshot.waiting.to_string(),
                    snapshot.in_flight.to_string(),
                    snapshot.rejected.to_string(),
                    snapshot.timed_out.to_string(),
                    snapshot.canceled.to_string(),
                ])
            })
            .collect(),
    )
}

fn policies_table(
    status: Option<PolicyStatusSnapshot>,
    reloads: &[PolicyReloadSnapshot],
) -> AdminTable {
    let columns = &[
        ("policy_id", AdminColumnType::Text),
        ("policy_version", AdminColumnType::Int8),
        ("policy_mode", AdminColumnType::Text),
        ("source", AdminColumnType::Text),
        ("enabled", AdminColumnType::Bool),
        ("last_reload_outcome", AdminColumnType::Text),
        ("error_code", AdminColumnType::Text),
    ];

    let rows = status
        .map(|status| {
            let last_reload = reloads.last();
            vec![AdminRow::new(vec![
                status.policy_id,
                status.policy_version.to_string(),
                status.policy_mode.as_str().to_string(),
                status.source,
                status.enabled.to_string(),
                policy_reload_outcome_label(last_reload),
                optional_text(
                    last_reload.and_then(|snapshot| snapshot.error_code.map(|code| code.as_str())),
                ),
            ])]
        })
        .unwrap_or_default();

    admin_table(AdminView::Policies, columns, rows)
}

fn policy_audit_table(
    events: &[pg_kinetic_core::policy::PolicyAuditEvent],
    include_kind: bool,
) -> AdminTable {
    let mut column_specs = Vec::new();
    if include_kind {
        column_specs.push(("kind", AdminColumnType::Text));
    }
    column_specs.extend_from_slice(&[
        ("policy_id", AdminColumnType::Text),
        ("policy_version", AdminColumnType::Int8),
        ("hook_point", AdminColumnType::Text),
        ("action", AdminColumnType::Text),
        ("outcome", AdminColumnType::Text),
        ("reason", AdminColumnType::Text),
        ("route", AdminColumnType::Text),
        ("shard", AdminColumnType::Text),
        ("target_role", AdminColumnType::Text),
        ("context", AdminColumnType::Text),
    ]);

    let rows = events
        .iter()
        .map(|event| {
            let mut values = Vec::new();
            if include_kind {
                values.push(event.kind.as_str().to_string());
            }
            values.extend_from_slice(&[
                event.policy_id.as_str().to_string(),
                event.policy_version.as_u64().to_string(),
                event.hook_point.as_str().to_string(),
                event.action.as_str().to_string(),
                event.outcome.as_str().to_string(),
                optional_text(event.reason.as_deref()),
                optional_text(event.route.as_deref()),
                optional_text(event.shard.as_deref()),
                optional_text(event.target_role.as_deref()),
                event.context.to_string(),
            ]);
            AdminRow::new(values)
        })
        .collect();

    admin_table(
        if include_kind {
            AdminView::PolicyAudit
        } else {
            AdminView::PolicyDecisions
        },
        column_specs.as_slice(),
        rows,
    )
}

fn routes_table(
    route_map_generation_id: u64,
    sharding_enabled: bool,
    routes: &[RouteSnapshot],
    route_policies: &[RoutePolicySnapshot],
) -> AdminTable {
    let route_policies = route_policies
        .iter()
        .cloned()
        .map(|snapshot| (snapshot.route_key.clone(), snapshot))
        .collect::<HashMap<_, _>>();

    admin_table(
        AdminView::Routes,
        &[
            ("database", AdminColumnType::Text),
            ("user", AdminColumnType::Text),
            ("application_name", AdminColumnType::Text),
            ("query_class", AdminColumnType::Text),
            ("client_count", AdminColumnType::Int8),
            ("backend_count", AdminColumnType::Int8),
            ("primary_count", AdminColumnType::Int8),
            ("replica_count", AdminColumnType::Int8),
            ("read_routing_mode", AdminColumnType::Text),
            ("fallback_policy", AdminColumnType::Text),
            ("freshness_policy", AdminColumnType::Text),
            ("read_after_write_timeout_ms", AdminColumnType::Int8),
            ("route_map_generation_id", AdminColumnType::Int8),
            ("sharding_enabled", AdminColumnType::Bool),
        ],
        routes
            .iter()
            .map(|snapshot| {
                let policy = route_policies
                    .get(&snapshot.route_key)
                    .cloned()
                    .unwrap_or_else(|| RoutePolicySnapshot::new(snapshot.route_key.clone()));

                AdminRow::new(vec![
                    snapshot.route_key.database().to_string(),
                    snapshot.route_key.user().to_string(),
                    optional_text(snapshot.route_key.application_name()),
                    snapshot.route_key.query_class().to_string(),
                    snapshot.client_count.to_string(),
                    snapshot.backend_count.to_string(),
                    policy.primary_count.to_string(),
                    policy.replica_count.to_string(),
                    policy.read_routing_mode.as_str().to_string(),
                    policy.fallback_policy.as_str().to_string(),
                    policy.freshness_policy.as_str().to_string(),
                    policy.read_after_write_timeout_ms.to_string(),
                    route_map_generation_id.to_string(),
                    sharding_enabled.to_string(),
                ])
            })
            .collect(),
    )
}

fn route_maps_table(sharding: &ShardingConfig) -> AdminTable {
    admin_table(
        AdminView::RouteMaps,
        &[
            ("scope", AdminColumnType::Text),
            ("strategy", AdminColumnType::Text),
            ("priority", AdminColumnType::Text),
            ("multi_shard_policy", AdminColumnType::Text),
        ],
        sharding
            .route_maps
            .iter()
            .map(|route_map| {
                AdminRow::new(vec![
                    route_map_scope_label(&route_map.scope),
                    shard_strategy_label(&route_map.strategy).to_string(),
                    route_map
                        .priority
                        .map(|priority| priority.0.to_string())
                        .unwrap_or_else(|| String::from("<none>")),
                    multi_shard_policy_label(sharding.multi_shard_policy).to_string(),
                ])
            })
            .collect(),
    )
}

fn shards_table(
    sharding: &ShardingConfig,
    lifecycle_snapshots: &[ShardLifecycleSnapshot],
) -> AdminTable {
    #[derive(Default)]
    struct ShardSummary {
        route_key: Option<String>,
        primary_backend_count: usize,
        replica_backend_count: usize,
        lifecycle_state: Option<ShardLifecycleState>,
    }

    let mut shards = BTreeMap::<String, ShardSummary>::new();

    for route_map in &sharding.route_maps {
        let route_key = route_map_scope_label(&route_map.scope);
        for target in &route_map.targets {
            let shard_id = match target {
                ShardTargetConfig::Primary { shard_id }
                | ShardTargetConfig::Replicas { shard_id } => shard_id,
            };
            let summary = shards.entry(shard_id.clone()).or_default();
            if summary.route_key.is_none() {
                summary.route_key = Some(route_key.clone());
            }

            match target {
                ShardTargetConfig::Primary { .. } => summary.primary_backend_count += 1,
                ShardTargetConfig::Replicas { .. } => summary.replica_backend_count += 1,
            }
        }
    }

    for snapshot in lifecycle_snapshots {
        shards
            .entry(snapshot.shard_id.as_str().to_owned())
            .or_default()
            .lifecycle_state = Some(snapshot.lifecycle_state);
    }

    admin_table(
        AdminView::Shards,
        &[
            ("shard_id", AdminColumnType::Text),
            ("route_key", AdminColumnType::Text),
            ("lifecycle_state", AdminColumnType::Text),
            ("primary_backend_count", AdminColumnType::Int8),
            ("replica_backend_count", AdminColumnType::Int8),
            ("health_summary", AdminColumnType::Text),
        ],
        shards
            .into_iter()
            .map(|(shard_id, summary)| {
                let lifecycle_state = summary.lifecycle_state.unwrap_or_default();
                let route_key = summary.route_key.unwrap_or_else(|| String::from("<none>"));
                let health_summary = shard_health_summary(
                    lifecycle_state,
                    summary.primary_backend_count,
                    summary.replica_backend_count,
                );

                AdminRow::new(vec![
                    shard_id,
                    route_key,
                    lifecycle_state.as_str().to_string(),
                    summary.primary_backend_count.to_string(),
                    summary.replica_backend_count.to_string(),
                    health_summary.to_string(),
                ])
            })
            .collect(),
    )
}

fn migrations_table(migrations: &[ShardMigrationSafetySnapshot]) -> AdminTable {
    admin_table(
        AdminView::Migrations,
        &[
            ("migration_state", AdminColumnType::Text),
            ("migration_override_explicit", AdminColumnType::Bool),
            ("source_shard_ids", AdminColumnType::Text),
            ("target_shard_ids", AdminColumnType::Text),
            ("active_client_count", AdminColumnType::Int8),
            ("prepared_statement_count", AdminColumnType::Int8),
            ("open_transaction_count", AdminColumnType::Int8),
            ("last_required_lsn", AdminColumnType::Text),
        ],
        migrations
            .iter()
            .map(|snapshot| {
                let plan = &snapshot.rebalance_plan;
                let report = plan.safety_report();

                AdminRow::new(vec![
                    plan.migration_state().as_str().to_string(),
                    plan.migration_override_explicit().to_string(),
                    join_shard_ids(plan.source_shard_ids()),
                    join_shard_ids(plan.target_shard_ids()),
                    report
                        .map(|report| report.active_client_ids().len())
                        .unwrap_or(0)
                        .to_string(),
                    report
                        .map(|report| report.prepared_statements().len())
                        .unwrap_or(0)
                        .to_string(),
                    report
                        .map(|report| report.open_transaction_ids().len())
                        .unwrap_or(0)
                        .to_string(),
                    optional_pglsn(report.and_then(|report| report.last_required_lsn())),
                ])
            })
            .collect(),
    )
}

fn settings_table(settings: &SettingsSnapshot) -> AdminTable {
    admin_table(
        AdminView::Settings,
        &[
            ("listen_addr", AdminColumnType::Text),
            ("backend_addr", AdminColumnType::Text),
            ("client_tls_mode", AdminColumnType::Text),
            ("backend_tls_mode", AdminColumnType::Text),
            ("auth_mode", AdminColumnType::Text),
            ("auth_failure_message_mode", AdminColumnType::Text),
            ("backend_user", AdminColumnType::Text),
            ("backend_reset_query", AdminColumnType::Text),
            ("recovery_mode", AdminColumnType::Text),
            ("reload_enabled", AdminColumnType::Bool),
            ("config_reload_interval_ms", AdminColumnType::Int8),
            ("drain_timeout_ms", AdminColumnType::Int8),
            ("reject_new_clients_during_drain", AdminColumnType::Bool),
            ("health_addr", AdminColumnType::Text),
            ("readiness_backend_check_interval_ms", AdminColumnType::Int8),
            ("readiness_timeout_ms", AdminColumnType::Int8),
            ("metrics_addr", AdminColumnType::Text),
            ("tcp_nodelay", AdminColumnType::Bool),
            ("tcp_keepalive", AdminColumnType::Bool),
            ("tcp_keepalive_idle_ms", AdminColumnType::Int8),
            ("tcp_keepalive_interval_ms", AdminColumnType::Int8),
            ("tcp_keepalive_retries", AdminColumnType::Int8),
            ("tcp_user_timeout_ms", AdminColumnType::Int8),
            ("tcp_send_buffer_bytes", AdminColumnType::Int8),
            ("tcp_recv_buffer_bytes", AdminColumnType::Int8),
            ("strict_socket_option_mode", AdminColumnType::Bool),
        ],
        vec![AdminRow::new(vec![
            settings.listen_addr.to_string(),
            settings.backend_addr.to_string(),
            settings.client_tls_mode.as_str().to_string(),
            settings.backend_tls_mode.as_str().to_string(),
            settings.auth_mode.as_str().to_string(),
            settings.auth_failure_message_mode.as_str().to_string(),
            optional_text(settings.backend_user.as_deref()),
            settings.backend_reset_query.clone(),
            recovery_mode_label(settings.recovery_mode),
            settings.reload_enabled.to_string(),
            duration_millis(settings.config_reload_interval),
            duration_millis(settings.drain_timeout),
            settings.reject_new_clients_during_drain.to_string(),
            optional_socket_addr(settings.health_addr),
            duration_millis(settings.readiness_backend_check_interval),
            duration_millis(settings.readiness_timeout),
            optional_socket_addr(settings.metrics_addr),
            settings.tcp_nodelay.to_string(),
            settings.tcp_keepalive.to_string(),
            optional_duration(settings.tcp_keepalive_idle),
            optional_duration(settings.tcp_keepalive_interval),
            optional_u32(settings.tcp_keepalive_retries),
            optional_duration(settings.tcp_user_timeout),
            optional_usize(settings.tcp_send_buffer_bytes),
            optional_usize(settings.tcp_recv_buffer_bytes),
            settings.strict_socket_option_mode.to_string(),
        ])],
    )
}

fn limits_table(limits: &LimitsSnapshot, config: &Config) -> AdminTable {
    admin_table(
        AdminView::Limits,
        &[
            ("max_clients", AdminColumnType::Int8),
            ("max_backends", AdminColumnType::Int8),
            ("max_checkout_waiters", AdminColumnType::Int8),
            ("max_route_in_flight", AdminColumnType::Int8),
            ("max_route_waiters", AdminColumnType::Int8),
            ("checkout_timeout_ms", AdminColumnType::Int8),
            ("query_timeout_ms", AdminColumnType::Int8),
            ("idle_client_timeout_ms", AdminColumnType::Int8),
            ("idle_transaction_timeout_ms", AdminColumnType::Int8),
            ("max_client_buffer_bytes", AdminColumnType::Int8),
            ("max_backend_buffer_bytes", AdminColumnType::Int8),
            ("recovery_timeout_ms", AdminColumnType::Int8),
            ("drain_timeout_ms", AdminColumnType::Int8),
            ("readiness_backend_check_interval_ms", AdminColumnType::Int8),
            ("readiness_timeout_ms", AdminColumnType::Int8),
            ("config_reload_interval_ms", AdminColumnType::Int8),
            ("admin_query_timeout_ms", AdminColumnType::Int8),
            ("admin_max_clients", AdminColumnType::Int8),
            ("overload_error_code", AdminColumnType::Text),
        ],
        vec![AdminRow::new(vec![
            limits.max_clients.to_string(),
            limits.max_backends.to_string(),
            limits.max_checkout_waiters.to_string(),
            limits.max_route_in_flight.to_string(),
            limits.max_route_waiters.to_string(),
            duration_millis(limits.checkout_timeout),
            duration_millis(limits.query_timeout),
            duration_millis(limits.idle_client_timeout),
            duration_millis(limits.idle_transaction_timeout),
            limits.max_client_buffer_bytes.to_string(),
            limits.max_backend_buffer_bytes.to_string(),
            duration_millis(limits.recovery_timeout),
            duration_millis(limits.drain_timeout),
            duration_millis(limits.readiness_backend_check_interval),
            duration_millis(limits.readiness_timeout),
            duration_millis(limits.config_reload_interval),
            config.admin.admin_query_timeout_ms.to_string(),
            config.admin.admin_max_clients.to_string(),
            limits.overload_error_code.clone(),
        ])],
    )
}

fn admin_table(
    view: AdminView,
    column_specs: &[(&'static str, AdminColumnType)],
    rows: Vec<AdminRow>,
) -> AdminTable {
    AdminTable::new(
        view,
        column_specs
            .iter()
            .map(|(name, column_type)| AdminColumn::new(name, *column_type))
            .collect(),
        rows,
    )
}

fn admin_table_response(table: AdminTable) -> BytesMut {
    let columns = table
        .columns()
        .iter()
        .map(|column| AdminWireColumn::new(column.name(), admin_wire_type(column.column_type())))
        .collect::<Vec<_>>();
    let rows = table
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    build_admin_table_response(&columns, &rows)
}

fn admin_wire_type(column_type: AdminColumnType) -> AdminWireType {
    match column_type {
        AdminColumnType::Text => AdminWireType::Text,
        AdminColumnType::Int8 => AdminWireType::Int8,
        AdminColumnType::Float8 => AdminWireType::Float8,
        AdminColumnType::Bool => AdminWireType::Bool,
        AdminColumnType::Timestamp => AdminWireType::Timestamp,
    }
}

fn optional_text(value: Option<&str>) -> String {
    value.map_or_else(|| String::from("<none>"), str::to_owned)
}

fn policy_reload_outcome_label(snapshot: Option<&PolicyReloadSnapshot>) -> String {
    match snapshot {
        Some(snapshot) if snapshot.success => String::from("success"),
        Some(_) => String::from("failure"),
        None => String::from("<none>"),
    }
}

fn optional_route_key(value: Option<&RouteKey>) -> String {
    value
        .map(route_key_value)
        .unwrap_or_else(|| String::from("<none>"))
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| String::from("<none>"), |number| number.to_string())
}

fn latest_route_map_generation(route_map_reloads: &[RouteMapReloadSnapshot]) -> u64 {
    route_map_reloads
        .last()
        .map_or(0, |snapshot| snapshot.route_map_generation_id)
}

fn route_map_scope_label(scope: &ShardScopeConfig) -> String {
    match scope {
        ShardScopeConfig::DatabaseUser { database, user } => format!("{database}/{user}"),
        ShardScopeConfig::ApplicationName { application_name } => application_name.clone(),
        ShardScopeConfig::SchemaTable { schema, table } => format!("{schema}.{table}"),
        ShardScopeConfig::TenantKey { tenant_key } => tenant_key.clone(),
    }
}

fn shard_strategy_label(strategy: &crate::config::ShardStrategyConfig) -> &'static str {
    match strategy {
        crate::config::ShardStrategyConfig::Hash => "hash",
        crate::config::ShardStrategyConfig::Range => "range",
        crate::config::ShardStrategyConfig::List => "list",
    }
}

fn multi_shard_policy_label(policy: MultiShardPolicyConfig) -> &'static str {
    match policy {
        MultiShardPolicyConfig::Reject => "reject",
        MultiShardPolicyConfig::FirstMatch => "first_match",
        MultiShardPolicyConfig::FanOut => "fan_out",
    }
}

fn join_shard_ids(shard_ids: &[pg_kinetic_core::sharding::ShardId]) -> String {
    if shard_ids.is_empty() {
        return String::from("<none>");
    }

    shard_ids
        .iter()
        .map(|shard_id| shard_id.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn shard_health_summary(
    lifecycle_state: ShardLifecycleState,
    primary_backend_count: usize,
    replica_backend_count: usize,
) -> &'static str {
    match lifecycle_state {
        ShardLifecycleState::Active => {
            if primary_backend_count + replica_backend_count == 0 {
                "unassigned"
            } else {
                "healthy"
            }
        }
        ShardLifecycleState::Draining => "draining",
        ShardLifecycleState::Readonly => "readonly",
        ShardLifecycleState::Disabled => "disabled",
    }
}

fn route_key_value(route_key: &RouteKey) -> String {
    route_key.metric_label().to_string()
}

fn duration_millis(duration: std::time::Duration) -> String {
    duration.as_millis().to_string()
}

fn optional_duration(value: Option<std::time::Duration>) -> String {
    value.map_or_else(|| String::from("<none>"), duration_millis)
}

fn optional_pglsn(value: Option<PgLsn>) -> String {
    value.map_or_else(|| String::from("<none>"), |lsn| lsn.to_string())
}

fn optional_socket_addr(value: Option<std::net::SocketAddr>) -> String {
    value.map_or_else(|| String::from("<none>"), |address| address.to_string())
}

fn optional_u32(value: Option<u32>) -> String {
    value.map_or_else(|| String::from("<none>"), |number| number.to_string())
}

fn optional_usize(value: Option<usize>) -> String {
    value.map_or_else(|| String::from("<none>"), |number| number.to_string())
}

fn optional_probe_age(value: Option<SystemTime>) -> String {
    value.map_or_else(
        || String::from("<none>"),
        |probe_time| {
            SystemTime::now()
                .duration_since(probe_time)
                .map_or_else(|_| String::from("0"), duration_millis)
        },
    )
}

fn recovery_mode_label(mode: pg_kinetic_core::recovery::RecoveryMode) -> String {
    match mode {
        pg_kinetic_core::recovery::RecoveryMode::Recover => String::from("recover"),
        pg_kinetic_core::recovery::RecoveryMode::RollbackOnly => String::from("rollback_only"),
        pg_kinetic_core::recovery::RecoveryMode::Drop => String::from("drop"),
    }
}

fn pin_reason_label(reason: PinReason) -> &'static str {
    match reason {
        PinReason::OpenTransaction => "open_transaction",
        PinReason::FailedTransaction => "failed_transaction",
        PinReason::SessionState => "session_state",
        PinReason::Copy => "copy",
        PinReason::ListenNotify => "listen_notify",
        PinReason::ExtendedQueryCycle => "extended_query_cycle",
        PinReason::UnknownProtocolState => "unknown_protocol_state",
    }
}

fn recovery_trigger_label(trigger: RecoveryTrigger) -> &'static str {
    match trigger {
        RecoveryTrigger::FailedTransaction => "failed_transaction",
        RecoveryTrigger::AbandonedTransaction => "abandoned_transaction",
        RecoveryTrigger::AbandonedResponse => "abandoned_response",
        RecoveryTrigger::UnknownProtocolState => "unknown_protocol_state",
    }
}

fn recovery_action_label(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::None => "none",
        RecoveryAction::Rollback => "rollback",
        RecoveryAction::DrainAndSync => "drain_and_sync",
        RecoveryAction::RollbackAndDrain => "rollback_and_drain",
        RecoveryAction::Discard => "discard",
    }
}

fn mirror_decisions_total(snapshot: &MirrorSummarySnapshot) -> u64 {
    snapshot.dropped_total
        + snapshot.timed_out_total
        + snapshot.mirrored_total
        + snapshot.skipped_total
        + snapshot.rejected_total
}

fn recommendation_summary(snapshot: Option<&AdaptiveRecommendationSnapshot>) -> String {
    let Some(snapshot) = snapshot else {
        return String::from("<none>");
    };

    format!(
        "{}:{}:{:.3}",
        snapshot.signal.as_str(),
        snapshot.knob.as_str(),
        snapshot.confidence
    )
}

fn apply_status_summary(snapshot: Option<&AdaptiveOutcomeSnapshot>) -> String {
    let Some(snapshot) = snapshot else {
        return String::from("<none>");
    };

    format!(
        "{}{}",
        snapshot.outcome.as_str(),
        if snapshot.disabled_by_reload {
            ":disabled"
        } else {
            ""
        }
    )
}

fn adaptive_guardrails_label(
    mode: &str,
    apply: &crate::config::AdaptiveApplyConfig,
    guardrail: &crate::config::AdaptiveGuardrailConfig,
) -> String {
    let allowlist = if apply.adaptive_apply_allowlist.is_empty() {
        String::from("<none>")
    } else {
        apply
            .adaptive_apply_allowlist
            .iter()
            .map(|knob| knob.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };

    format!(
        "mode={mode};apply={};allowlist={allowlist};max_change={}%",
        apply.adaptive_apply_enabled, guardrail.adaptive_max_change_percent
    )
}

fn benchmark_metric_value(value: f64) -> String {
    format!("{value:.3}")
}
