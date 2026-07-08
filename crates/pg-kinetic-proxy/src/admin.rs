use std::{
    collections::HashMap,
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
    config::Config,
    drain::DrainController,
    proxy::{read_startup_packet, ClientConnection, StartupRead},
    reload,
    snapshot::{
        BackpressureSnapshot, ClientSnapshot, LimitsSnapshot, PinningSnapshot, PoolSnapshot,
        PreparedSnapshot, RecoverySnapshot, ReplicaHealthSnapshot, RouteCheckoutSnapshot,
        RoutePolicySnapshot, RouteSnapshot, ServerSnapshot, SettingsSnapshot, SnapshotStore,
    },
    socket, telemetry,
};
use pg_kinetic_core::{
    admin::{
        parse_admin_command, AdminColumn, AdminColumnType, AdminCommand, AdminRow, AdminTable,
        AdminView,
    },
    lsn::PgLsn,
    recovery::{RecoveryAction, RecoveryTrigger},
    route::RouteKey,
    session::PinReason,
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
    let table = match view {
        AdminView::Clients => clients_table(
            &state.snapshot_store.client_snapshots(),
            &state.snapshot_store.route_checkout_snapshots(),
        ),
        AdminView::Pools => pools_table(&state.snapshot_store.pool_snapshot()),
        AdminView::Servers => servers_table(
            &state.snapshot_store.server_snapshots(),
            &state.snapshot_store.replica_health_snapshots(),
        ),
        AdminView::Prepared => prepared_table(&state.snapshot_store.prepared_snapshot()),
        AdminView::Pinning => pinning_table(&state.snapshot_store.pinning_snapshots()),
        AdminView::Recovery => recovery_table(&state.snapshot_store.recovery_snapshots()),
        AdminView::Backpressure => {
            backpressure_table(&state.snapshot_store.backpressure_snapshots())
        }
        AdminView::Routes => routes_table(
            &state.snapshot_store.route_snapshots(),
            &state.snapshot_store.route_policy_snapshots(),
        ),
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

fn pools_table(pool: &PoolSnapshot) -> AdminTable {
    admin_table(
        AdminView::Pools,
        &[
            ("route_key", AdminColumnType::Text),
            ("max_backends", AdminColumnType::Int8),
            ("active_backends", AdminColumnType::Int8),
            ("idle_backends", AdminColumnType::Int8),
            ("waiting_clients", AdminColumnType::Int8),
        ],
        vec![AdminRow::new(vec![
            String::from("global"),
            pool.configured_backends.to_string(),
            pool.active_backends.to_string(),
            pool.idle_backends.to_string(),
            pool.waiting_clients.to_string(),
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

fn prepared_table(prepared: &PreparedSnapshot) -> AdminTable {
    admin_table(
        AdminView::Prepared,
        &[
            ("session_id", AdminColumnType::Int8),
            ("client_statement_name", AdminColumnType::Text),
            ("backend_statement_name", AdminColumnType::Text),
            ("materialized_backend_count", AdminColumnType::Int8),
            ("invalidation_count", AdminColumnType::Int8),
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

fn routes_table(routes: &[RouteSnapshot], route_policies: &[RoutePolicySnapshot]) -> AdminTable {
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

fn optional_route_key(value: Option<&RouteKey>) -> String {
    value
        .map(route_key_value)
        .unwrap_or_else(|| String::from("<none>"))
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| String::from("<none>"), |number| number.to_string())
}

fn route_key_value(route_key: &RouteKey) -> String {
    route_key.metric_label()
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
