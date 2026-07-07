use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
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
    snapshot::SnapshotStore,
    socket,
};
use pg_kinetic_core::admin::{parse_admin_command, AdminCommand};
use pg_kinetic_wire::{
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
    _snapshot_store: SnapshotStore,
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
        _snapshot_store: snapshot_store,
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
    let admin_timeout = Duration::from_millis(state.config.admin.admin_query_timeout_ms);
    let startup_packet = match read_startup_packet(
        client,
        state.config.tls.client_tls_mode,
        state.client_tls_server_config.as_ref(),
        admin_timeout,
        state.config.qos.max_client_buffer_bytes,
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
                        error_response_and_ready(
                            client,
                            ADMIN_UNSUPPORTED_SQLSTATE,
                            &format!("admin view {} is not implemented", view.as_str()),
                            ReadyStatusByte::Idle,
                        )
                        .await?;
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

async fn reject_admin_user(client: &mut ClientConnection, allowed_user: &str) -> anyhow::Result<()> {
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
    client.shutdown().await.context("shutdown admin client during drain")
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
