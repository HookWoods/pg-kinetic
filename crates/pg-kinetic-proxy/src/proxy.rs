use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::Context;
use bytes::{BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};

use crate::{
    config::Config,
    metrics,
    pool::{BackendPool, PooledBackend},
};
use pg_kinetic_core::{
    cleanup::{cleanup_action, CleanupAction},
    constants::PreparedEvent,
    pin::PinnedBackend,
    prepare::{InvalidationScope, PreparedCatalog},
    recovery::{recovery_action, RecoveryAction, RecoveryTrigger},
    sql::classify,
    virtual_session::{PinReason, VirtualSession},
};
use pg_kinetic_wire::{
    backend::{parse_backend_frame, BackendFrame, ReadyStatus},
    error::WireError,
    frame::{parse_frontend_frame, FrontendFrame},
    message::{
        parse_bind_statement_name, parse_close_target, parse_describe_target, parse_parse_message,
        parse_simple_query, CloseTarget, DescribeTarget,
    },
    protocol::{BackendTag, FrontendTag, ReadyStatusByte},
    rewrite::{
        build_parse_frame, encode_frontend_frame, rewrite_bind_statement_name,
        rewrite_close_statement_name, rewrite_describe_statement_name,
        rewrite_parse_statement_name,
    },
    startup::{parse_startup_packet, StartupPacket},
};

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Proxy {
    config: Config,
    client_slots: Arc<Semaphore>,
    pool: Arc<BackendPool>,
}

impl Proxy {
    #[must_use]
    pub fn new(config: Config) -> Self {
        let client_slots = Arc::new(Semaphore::new(config.capacity.max_clients));
        let pool = BackendPool::new(
            config.connection.backend_addr,
            config.capacity.max_backends,
            config.capacity.max_checkout_waiters,
            config.performance.checkout_timeout(),
            config.performance.backend_reset_query.clone(),
        );

        Self {
            config,
            client_slots,
            pool,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.config.connection.listen_addr)
            .await
            .with_context(|| format!("bind listener {}", self.config.connection.listen_addr))?;

        tracing::info!(listen_addr = %self.config.connection.listen_addr, "listening");

        loop {
            let (client, client_addr) = listener.accept().await.context("accept client")?;
            client.set_nodelay(true).context("set client TCP_NODELAY")?;
            metrics::increment_client_connections();

            let permit = self.client_slots.clone().acquire_owned().await?;
            let pool = self.pool.clone();
            let performance = self.config.performance.clone();

            tokio::spawn(async move {
                let result = handle_client(client, client_addr, pool, performance).await;
                drop(permit);

                if let Err(error) = result {
                    let error_chain = format!("{error:#}");
                    tracing::warn!(%client_addr, error = %error_chain, "client connection closed with error");
                }
            });
        }
    }
}

async fn handle_client(
    mut client: TcpStream,
    client_addr: SocketAddr,
    pool: Arc<BackendPool>,
    performance: crate::config::PerformanceConfig,
) -> anyhow::Result<()> {
    let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
    let mut session = VirtualSession::default();
    let mut pinned_backend = PinnedBackend::default();
    let mut prepared = PreparedCatalog::new(session_id);
    let mut held_backend: Option<PooledBackend> = None;

    let startup_packet = match read_startup_packet(&mut client)
        .await
        .with_context(|| format!("proxy client {client_addr}"))?
    {
        StartupRead::Packet(packet) => packet,
        StartupRead::ClientClosed => return Ok(()),
    };

    let mut backend = checkout_backend(&pool, "checkout backend for startup").await?;
    if let Err(error) = proxy_startup(&mut client, &mut backend, &startup_packet).await {
        backend.discard();
        return Err(error).with_context(|| format!("proxy client {client_addr}"));
    }
    backend.release().await;

    let mut client_buffer = BytesMut::with_capacity(16 * 1024);

    loop {
        if client
            .read_buf(&mut client_buffer)
            .await
            .context("read client")?
            == 0
        {
            if let Some(backend) = held_backend.take() {
                finalize_backend_on_disconnect(
                    backend,
                    &pool,
                    &performance,
                    &mut session,
                    &mut pinned_backend,
                )
                .await?;
            }

            return Ok(());
        }

        while let Some(cycle) = next_client_cycle(&mut client, &mut client_buffer).await? {
            let ClientCycle::Frames(frames) = cycle else {
                if let Some(backend) = held_backend.take() {
                    finalize_backend_on_disconnect(
                        backend,
                        &pool,
                        &performance,
                        &mut session,
                        &mut pinned_backend,
                    )
                    .await?;
                }

                return Ok(());
            };

            let mut backend = if let Some(backend) = held_backend.take() {
                backend
            } else {
                checkout_backend(&pool, "checkout backend for cycle").await?
            };

            if should_replay_session(&session, &pinned_backend, backend.backend_id()) {
                let replay = replay_frames(&session);
                let status = execute_backend_batch(&mut backend, &replay)
                    .await
                    .context("replay virtual session")?;
                anyhow::ensure!(
                    status == ReadyStatus::Idle,
                    "unexpected replay status: {status:?}"
                );
            }

            let result = forward_message_cycle(
                &mut client,
                &mut backend,
                &mut session,
                &mut prepared,
                frames,
            )
            .await;

            let client_disconnected_after_ready =
                matches!(&result, Ok(ForwardOutcome::ClientDisconnectedAfterReady(_)));

            match result {
                Ok(ForwardOutcome::Ready(status))
                | Ok(ForwardOutcome::ClientDisconnectedAfterReady(status)) => {
                    session.mark_ready_after_copy();
                    let action = cleanup_action(&session, status);
                    metrics::increment_cleanup(action);

                    match action {
                        CleanupAction::Reuse => {
                            pinned_backend.clear();
                            backend.release().await;
                        }
                        CleanupAction::ResetThenReuse => {
                            execute_simple_query(&mut backend, pool.reset_query())
                                .await
                                .context("reset backend before reuse")?;
                            pinned_backend.clear();
                            backend.release().await;
                        }
                        CleanupAction::KeepPinned => {
                            if let Some(reason) = session.pin_reason() {
                                metrics::increment_pin(reason);
                            }
                            pinned_backend.mark_pinned(backend.backend_id());
                            held_backend = Some(backend);
                        }
                        CleanupAction::RollbackThenReuse => {
                            execute_simple_query(&mut backend, "ROLLBACK")
                                .await
                                .context("rollback failed transaction")?;
                            session.apply_sql(classify("rollback"));
                            pinned_backend.clear();
                            backend.release().await;
                        }
                        CleanupAction::Discard => {
                            pinned_backend.clear();
                            backend.discard();
                        }
                    }

                    if client_disconnected_after_ready {
                        return Ok(());
                    }
                }
                Ok(ForwardOutcome::AbandonedResponse { needs_sync }) => {
                    let reused = recover_backend(
                        &mut backend,
                        RecoveryTrigger::AbandonedResponse,
                        &performance,
                        needs_sync,
                        &mut session,
                    )
                    .await
                    .context("recover abandoned response")?;
                    pinned_backend.clear();
                    if reused {
                        backend.release().await;
                    } else {
                        backend.discard();
                    }
                    return Ok(());
                }
                Err(error) => {
                    backend.discard();
                    return Err(error).with_context(|| format!("proxy client {client_addr}"));
                }
            }
        }
    }
}

async fn checkout_backend(
    pool: &Arc<BackendPool>,
    context: &'static str,
) -> anyhow::Result<PooledBackend> {
    let started = Instant::now();
    let backend = pool.checkout().await.context(context)?;
    metrics::record_pool_checkout(started.elapsed().as_secs_f64() * 1000.0, "ok");
    Ok(backend)
}

async fn next_client_cycle(
    client: &mut TcpStream,
    client_buffer: &mut BytesMut,
) -> anyhow::Result<Option<ClientCycle>> {
    let Some(first) = parse_frontend_frame(client_buffer)? else {
        return Ok(None);
    };

    if first.tag == u8::from(FrontendTag::Terminate) {
        return Ok(Some(ClientCycle::Terminate));
    }

    if first.tag == u8::from(FrontendTag::Query) {
        return Ok(Some(ClientCycle::Frames(vec![first])));
    }

    let mut frames = vec![first];
    while !frames
        .iter()
        .any(|frame| frame.tag == u8::from(FrontendTag::Sync))
    {
        if let Some(frame) = parse_frontend_frame(client_buffer)? {
            frames.push(frame);
            continue;
        }

        if client
            .read_buf(client_buffer)
            .await
            .context("read extended query frame")?
            == 0
        {
            anyhow::bail!("client disconnected during extended query cycle");
        }
    }

    Ok(Some(ClientCycle::Frames(frames)))
}

#[derive(Debug)]
enum ClientCycle {
    Frames(Vec<FrontendFrame>),
    Terminate,
}

async fn read_startup_packet(client: &mut TcpStream) -> anyhow::Result<StartupRead> {
    let mut buffer = BytesMut::with_capacity(8192);
    loop {
        while let Some(packet) = next_startup_packet(&mut buffer)? {
            match parse_startup_packet(&packet) {
                Ok(StartupPacket::SslRequest | StartupPacket::GssEncRequest) => {
                    reject_startup_encryption_request(client).await?;
                    continue;
                }
                Ok(StartupPacket::CancelRequest { .. }) => {
                    anyhow::bail!("cancel requests are not supported during startup");
                }
                Ok(StartupPacket::Startup { .. }) => return Ok(StartupRead::Packet(packet)),
                Err(error) => return Err(error).context("parse startup packet"),
            }
        }

        if client.read_buf(&mut buffer).await.context("read startup")? == 0 {
            return Ok(StartupRead::ClientClosed);
        }
    }
}

fn next_startup_packet(buffer: &mut BytesMut) -> anyhow::Result<Option<BytesMut>> {
    if buffer.len() < 4 {
        return Ok(None);
    }

    let len = i32::from_be_bytes(
        buffer[..4]
            .try_into()
            .expect("four startup length bytes are present"),
    );
    if len < 8 {
        return Err(WireError::InvalidStartupLength(len)).context("parse startup packet");
    }

    let len = len as usize;
    if buffer.len() < len {
        return Ok(None);
    }

    Ok(Some(buffer.split_to(len)))
}

async fn reject_startup_encryption_request(client: &mut TcpStream) -> anyhow::Result<()> {
    client
        .write_all(b"N")
        .await
        .context("reject startup encryption request")
}

#[derive(Debug)]
enum StartupRead {
    Packet(BytesMut),
    ClientClosed,
}

async fn proxy_startup(
    client: &mut TcpStream,
    backend: &mut PooledBackend,
    startup_packet: &[u8],
) -> anyhow::Result<()> {
    if !backend.requires_startup() {
        client
            .write_all(&synthetic_startup_ready())
            .await
            .context("write synthetic startup response")?;
        return Ok(());
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(startup_packet)
        .await
        .context("forward startup")?;

    let mut client_buffer = BytesMut::with_capacity(8192);
    let mut backend_buffer = BytesMut::with_capacity(8192);
    loop {
        backend
            .backend_mut()
            .stream_mut()
            .read_buf(&mut backend_buffer)
            .await
            .context("read startup response")?;
        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            client
                .write_all(&encode_backend_frame(&frame))
                .await
                .context("forward startup response")?;

            if frame.tag == u8::from(BackendTag::Authentication)
                && auth_request_expects_client_response(&frame.payload)?
            {
                client_buffer.clear();
                let read = client
                    .read_buf(&mut client_buffer)
                    .await
                    .context("read startup auth response")?;
                anyhow::ensure!(read > 0, "client disconnected during startup auth");
                backend
                    .backend_mut()
                    .stream_mut()
                    .write_all(&client_buffer)
                    .await
                    .context("forward startup auth response")?;
            }

            if frame.ready_status() == Some(ReadyStatus::Idle) {
                return Ok(());
            }
        }
    }
}

fn encode_backend_frame(frame: &BackendFrame) -> BytesMut {
    let mut encoded = BytesMut::with_capacity(frame.payload.len() + 5);
    encoded.put_u8(frame.tag);
    encoded.put_i32((frame.payload.len() + 4) as i32);
    encoded.extend_from_slice(&frame.payload);
    encoded
}

fn synthetic_startup_ready() -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(u8::from(BackendTag::Authentication));
    bytes.put_i32(8);
    bytes.put_i32(0);
    bytes.put_u8(u8::from(BackendTag::ReadyForQuery));
    bytes.put_i32(5);
    bytes.put_u8(u8::from(ReadyStatusByte::Idle));
    bytes
}

fn auth_request_expects_client_response(payload: &[u8]) -> anyhow::Result<bool> {
    anyhow::ensure!(payload.len() >= 4, "authentication request missing code");
    let code = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    Ok(matches!(code, 3 | 5 | 6 | 7 | 8 | 9 | 10 | 11))
}

async fn forward_message_cycle(
    client: &mut TcpStream,
    backend: &mut PooledBackend,
    session: &mut VirtualSession,
    prepared: &mut PreparedCatalog,
    frames: Vec<FrontendFrame>,
) -> anyhow::Result<ForwardOutcome> {
    let needs_sync = should_sync_for_frames(&frames);
    let mut outbound = BytesMut::new();
    for frame in frames {
        let plan = prepare_frame_for_backend(backend.backend_id(), prepared, frame)?;
        update_virtual_session_from_frame(session, &plan.frame)?;

        for prelude in &plan.prelude {
            outbound.extend_from_slice(&encode_frontend_frame(prelude));
        }
        outbound.extend_from_slice(&encode_frontend_frame(&plan.frame));
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(&outbound)
        .await
        .context("write frontend cycle to backend")?;

    let mut backend_buffer = BytesMut::with_capacity(16 * 1024);
    loop {
        let read = backend
            .backend_mut()
            .stream_mut()
            .read_buf(&mut backend_buffer)
            .await
            .context("read backend frame")?;
        if read == 0 {
            anyhow::bail!("backend disconnected during response cycle");
        }

        let mut forward = BytesMut::new();
        let mut ready = None;
        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            if let Some(sqlstate) = frame.sqlstate() {
                metrics::increment_sqlstate(sqlstate);
                let scope = prepared.invalidate_for_sqlstate(sqlstate, backend.backend_id());
                if scope != InvalidationScope::None {
                    metrics::increment_prepared_event(PreparedEvent::Invalidate);
                }
            }

            if frame.tag == u8::from(BackendTag::ErrorResponse)
                && matches!(session.pin_reason(), Some(PinReason::OpenTransaction))
            {
                session.mark_failed_transaction();
            }

            forward.extend_from_slice(&encode_backend_frame(&frame));
            if let Some(status) = frame.ready_status() {
                ready = Some(status);
            }
        }

        if !forward.is_empty() && client.write_all(&forward).await.is_err() {
            if let Some(status) = ready {
                return Ok(ForwardOutcome::ClientDisconnectedAfterReady(status));
            }

            return Ok(ForwardOutcome::AbandonedResponse { needs_sync });
        }

        if let Some(status) = ready {
            return Ok(ForwardOutcome::Ready(status));
        }
    }
}

fn prepare_frame_for_backend(
    backend_id: u64,
    prepared: &mut PreparedCatalog,
    frame: FrontendFrame,
) -> anyhow::Result<PreparedForwardPlan> {
    if let Some(parse) = parse_parse_message(&frame)? {
        let statement = prepared
            .upsert(parse.statement_name, parse.query, parse.parameter_type_oids)
            .clone();
        metrics::increment_prepared_event(PreparedEvent::Parse);
        prepared.mark_materialized(backend_id, &statement);
        return Ok(PreparedForwardPlan::single(rewrite_parse_statement_name(
            &frame,
            &statement.backend_name,
        )?));
    }

    if let Some(statement_name) = parse_bind_statement_name(&frame)? {
        if let Some(statement) = prepared.get(&statement_name).cloned() {
            metrics::increment_prepared_event(PreparedEvent::Bind);
            let mut prelude = Vec::new();
            if !prepared.is_materialized(backend_id, &statement) {
                prelude.push(build_parse_frame(
                    &statement.backend_name,
                    &statement.query,
                    &statement.parameter_type_oids,
                ));
                prepared.mark_materialized(backend_id, &statement);
                metrics::increment_prepared_event(PreparedEvent::Materialize);
            }

            return Ok(PreparedForwardPlan {
                prelude,
                frame: rewrite_bind_statement_name(&frame, &statement.backend_name)?,
            });
        }
    }

    if let Some(DescribeTarget::Statement(statement_name)) = parse_describe_target(&frame)? {
        if let Some(statement) = prepared.get(&statement_name).cloned() {
            let mut prelude = Vec::new();
            if !prepared.is_materialized(backend_id, &statement) {
                prelude.push(build_parse_frame(
                    &statement.backend_name,
                    &statement.query,
                    &statement.parameter_type_oids,
                ));
                prepared.mark_materialized(backend_id, &statement);
                metrics::increment_prepared_event(PreparedEvent::Materialize);
            }

            return Ok(PreparedForwardPlan {
                prelude,
                frame: rewrite_describe_statement_name(&frame, &statement.backend_name)?,
            });
        }
    }

    if let Some(CloseTarget::Statement(statement_name)) = parse_close_target(&frame)? {
        if let Some(statement) = prepared.remove(&statement_name) {
            metrics::increment_prepared_event(PreparedEvent::Close);
            return Ok(PreparedForwardPlan::single(rewrite_close_statement_name(
                &frame,
                &statement.backend_name,
            )?));
        }
    }

    Ok(PreparedForwardPlan::single(frame))
}

#[derive(Debug)]
struct PreparedForwardPlan {
    prelude: Vec<FrontendFrame>,
    frame: FrontendFrame,
}

impl PreparedForwardPlan {
    fn single(frame: FrontendFrame) -> Self {
        Self {
            prelude: Vec::new(),
            frame,
        }
    }
}

#[derive(Debug)]
enum ForwardOutcome {
    Ready(ReadyStatus),
    ClientDisconnectedAfterReady(ReadyStatus),
    AbandonedResponse { needs_sync: bool },
}

fn should_sync_for_frames(frames: &[FrontendFrame]) -> bool {
    frames
        .iter()
        .any(|frame| frame.tag != u8::from(FrontendTag::Query))
}

fn simple_query_frame(sql: &str) -> FrontendFrame {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.put_u8(0);
    FrontendFrame {
        tag: u8::from(FrontendTag::Query),
        payload: payload.freeze(),
    }
}

fn replay_frames(session: &VirtualSession) -> Vec<FrontendFrame> {
    session
        .replay_sql()
        .into_iter()
        .map(|sql| simple_query_frame(&sql))
        .collect()
}

fn sync_frame() -> FrontendFrame {
    FrontendFrame {
        tag: u8::from(FrontendTag::Sync),
        payload: BytesMut::new().freeze(),
    }
}

fn update_virtual_session_from_frame(
    session: &mut VirtualSession,
    frame: &FrontendFrame,
) -> anyhow::Result<()> {
    if let Some(query) = parse_simple_query(frame)? {
        session.apply_sql(classify(query));
    } else if [
        FrontendTag::Parse,
        FrontendTag::Bind,
        FrontendTag::Describe,
        FrontendTag::Execute,
        FrontendTag::Close,
        FrontendTag::Sync,
    ]
    .iter()
    .any(|tag| frame.tag == u8::from(*tag))
    {
        return Ok(());
    }

    Ok(())
}

async fn execute_backend_batch(
    backend: &mut PooledBackend,
    frames: &[FrontendFrame],
) -> anyhow::Result<ReadyStatus> {
    let mut outbound = BytesMut::new();
    for frame in frames {
        outbound.extend_from_slice(&encode_frontend_frame(frame));
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(&outbound)
        .await
        .context("write backend batch")?;
    await_ready_status(backend).await
}

async fn execute_simple_query(backend: &mut PooledBackend, sql: &str) -> anyhow::Result<()> {
    let status = execute_backend_batch(backend, &[simple_query_frame(sql)])
        .await
        .with_context(|| format!("execute backend query {sql}"))?;
    anyhow::ensure!(
        status == ReadyStatus::Idle,
        "unexpected backend status after {sql}: {status:?}"
    );
    Ok(())
}

async fn await_ready_status(backend: &mut PooledBackend) -> anyhow::Result<ReadyStatus> {
    let mut backend_buffer = BytesMut::with_capacity(16 * 1024);
    loop {
        let read = backend
            .backend_mut()
            .stream_mut()
            .read_buf(&mut backend_buffer)
            .await
            .context("read backend response")?;
        if read == 0 {
            anyhow::bail!("backend disconnected during response drain");
        }

        let mut ready = None;
        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            if let Some(status) = frame.ready_status() {
                ready = Some(status);
            }
        }

        if let Some(status) = ready {
            return Ok(status);
        }
    }
}

async fn recover_backend(
    backend: &mut PooledBackend,
    trigger: RecoveryTrigger,
    performance: &crate::config::PerformanceConfig,
    needs_sync: bool,
    session: &mut VirtualSession,
) -> anyhow::Result<bool> {
    let action = recovery_action(trigger, performance.recovery_mode);
    let recovered = timeout(performance.recovery_timeout(), async {
        match action {
            RecoveryAction::None => Ok(true),
            RecoveryAction::Rollback => {
                execute_simple_query(backend, "ROLLBACK").await?;
                session.apply_sql(classify("rollback"));
                Ok(true)
            }
            RecoveryAction::DrainAndSync => {
                let status = if needs_sync {
                    execute_backend_batch(backend, &[sync_frame()]).await?
                } else {
                    await_ready_status(backend).await?
                };
                anyhow::ensure!(
                    status == ReadyStatus::Idle,
                    "unexpected recovery status: {status:?}"
                );
                Ok(true)
            }
            RecoveryAction::RollbackAndDrain => {
                let status = if needs_sync {
                    execute_backend_batch(backend, &[sync_frame()]).await?
                } else {
                    await_ready_status(backend).await?
                };
                anyhow::ensure!(
                    status == ReadyStatus::Idle,
                    "unexpected recovery status: {status:?}"
                );
                execute_simple_query(backend, "ROLLBACK").await?;
                session.apply_sql(classify("rollback"));
                Ok(true)
            }
            RecoveryAction::Discard => Ok(false),
        }
    })
    .await;

    match recovered {
        Ok(Ok(reuse)) => {
            metrics::increment_recovery(trigger, action, "ok");
            Ok(reuse)
        }
        Ok(Err(error)) => {
            metrics::increment_recovery(trigger, action, "error");
            Err(error)
        }
        Err(_) => {
            metrics::increment_recovery(trigger, action, "timeout");
            Ok(false)
        }
    }
}

async fn finalize_backend_on_disconnect(
    mut backend: PooledBackend,
    pool: &Arc<BackendPool>,
    performance: &crate::config::PerformanceConfig,
    session: &mut VirtualSession,
    pinned_backend: &mut PinnedBackend,
) -> anyhow::Result<()> {
    match session.pin_reason() {
        Some(PinReason::OpenTransaction) | Some(PinReason::FailedTransaction) => {
            let reused = recover_backend(
                &mut backend,
                RecoveryTrigger::AbandonedTransaction,
                performance,
                false,
                session,
            )
            .await
            .context("recover abandoned transaction")?;
            pinned_backend.clear();
            if reused {
                backend.release().await;
            } else {
                backend.discard();
            }
        }
        Some(PinReason::UnknownProtocolState) => {
            pinned_backend.clear();
            backend.discard();
        }
        Some(PinReason::Copy)
        | Some(PinReason::TempTable)
        | Some(PinReason::AdvisoryLock)
        | Some(PinReason::ListenNotify)
        | Some(PinReason::SessionState) => {
            pinned_backend.clear();
            backend.discard();
        }
        None => {
            if session.has_replayable_settings() {
                execute_simple_query(&mut backend, pool.reset_query())
                    .await
                    .context("reset backend during disconnect cleanup")?;
            }
            pinned_backend.clear();
            backend.release().await;
        }
    }

    Ok(())
}

fn should_replay_session(
    session: &VirtualSession,
    pinned_backend: &PinnedBackend,
    backend_id: u64,
) -> bool {
    session.has_replayable_settings() && pinned_backend.backend_id() != Some(backend_id)
}

#[cfg(test)]
mod tests {
    use super::auth_request_expects_client_response;

    fn auth_payload(code: i32) -> [u8; 4] {
        code.to_be_bytes()
    }

    #[test]
    fn sasl_start_and_continue_expect_client_responses() {
        assert!(auth_request_expects_client_response(&auth_payload(10)).unwrap());
        assert!(auth_request_expects_client_response(&auth_payload(11)).unwrap());
    }

    #[test]
    fn sasl_final_and_ok_do_not_expect_client_responses() {
        assert!(!auth_request_expects_client_response(&auth_payload(12)).unwrap());
        assert!(!auth_request_expects_client_response(&auth_payload(0)).unwrap());
    }
}
