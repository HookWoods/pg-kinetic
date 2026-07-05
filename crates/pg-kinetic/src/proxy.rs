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
};

use crate::{
    config::Config,
    metrics,
    pool::{BackendPool, PooledBackend},
    prepare::PreparedCatalog,
    session::{ClientEvent, SessionState},
    wire::{
        backend::{parse_backend_frame, ReadyStatus},
        frame::{parse_frontend_frame, FrontendFrame},
        message::{
            parse_bind_statement_name, parse_close_target, parse_describe_target,
            parse_parse_message, parse_simple_query, CloseTarget, DescribeTarget,
        },
        rewrite::{
            build_parse_frame, encode_frontend_frame, rewrite_bind_statement_name,
            rewrite_close_statement_name, rewrite_describe_statement_name,
            rewrite_parse_statement_name,
        },
        startup::{parse_startup_packet, StartupPacket},
    },
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

            tokio::spawn(async move {
                let result = handle_client(client, client_addr, pool).await;
                drop(permit);

                if let Err(error) = result {
                    tracing::warn!(%client_addr, error = %error, "client connection closed with error");
                }
            });
        }
    }
}

async fn handle_client(
    mut client: TcpStream,
    client_addr: SocketAddr,
    pool: Arc<BackendPool>,
) -> anyhow::Result<()> {
    let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
    let mut session = SessionState::default();
    let mut prepared = PreparedCatalog::new(session_id);

    let mut backend = checkout_backend(&pool, "checkout backend for startup").await?;
    if let Err(error) = proxy_startup(&mut client, &mut backend).await {
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
            return Ok(());
        }

        while let Some(frames) = next_client_cycle(&mut client, &mut client_buffer).await? {
            let mut backend = checkout_backend(&pool, "checkout backend for cycle").await?;

            let result = forward_message_cycle(
                &mut client,
                &mut backend,
                &mut session,
                &mut prepared,
                frames,
            )
            .await;

            match result {
                Ok(
                    ReadyStatus::Idle | ReadyStatus::InTransaction | ReadyStatus::FailedTransaction,
                ) => {
                    backend.release().await;
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
) -> anyhow::Result<Option<Vec<FrontendFrame>>> {
    let Some(first) = parse_frontend_frame(client_buffer)? else {
        return Ok(None);
    };

    if first.tag == b'Q' {
        return Ok(Some(vec![first]));
    }

    let mut frames = vec![first];
    while !frames.iter().any(|frame| frame.tag == b'S') {
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

    Ok(Some(frames))
}

async fn proxy_startup(client: &mut TcpStream, backend: &mut PooledBackend) -> anyhow::Result<()> {
    let mut client_buffer = BytesMut::with_capacity(8192);
    let mut buffer = BytesMut::with_capacity(8192);
    loop {
        buffer.clear();
        client.read_buf(&mut buffer).await.context("read startup")?;

        match parse_startup_packet(&buffer) {
            Ok(StartupPacket::SslRequest) => {
                client
                    .write_all(b"N")
                    .await
                    .context("reject startup ssl request")?;
                continue;
            }
            Ok(StartupPacket::CancelRequest { .. }) => {
                anyhow::bail!("cancel requests are not supported during startup");
            }
            Ok(StartupPacket::Startup { .. }) => break,
            Err(error) => return Err(error).context("parse startup packet"),
        }
    }

    backend
        .backend_mut()
        .stream_mut()
        .write_all(&buffer)
        .await
        .context("forward startup")?;

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

            if frame.tag == b'R' && !frame.payload.starts_with(&[0, 0, 0, 0]) {
                client_buffer.clear();
                client
                    .read_buf(&mut client_buffer)
                    .await
                    .context("read startup auth response")?;
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

fn encode_backend_frame(frame: &crate::wire::backend::BackendFrame) -> BytesMut {
    let mut encoded = BytesMut::with_capacity(frame.payload.len() + 5);
    encoded.put_u8(frame.tag);
    encoded.put_i32((frame.payload.len() + 4) as i32);
    encoded.extend_from_slice(&frame.payload);
    encoded
}

async fn forward_message_cycle(
    client: &mut TcpStream,
    backend: &mut PooledBackend,
    session: &mut SessionState,
    prepared: &mut PreparedCatalog,
    frames: Vec<FrontendFrame>,
) -> anyhow::Result<ReadyStatus> {
    let mut outbound = BytesMut::new();
    for frame in frames {
        let plan = prepare_frame_for_backend(backend.backend_id(), prepared, frame)?;
        update_session_from_frame(session, &plan.frame)?;

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
        backend
            .backend_mut()
            .stream_mut()
            .read_buf(&mut backend_buffer)
            .await
            .context("read backend frame")?;

        let mut forward = BytesMut::new();
        let mut ready = None;
        while let Some(frame) = parse_backend_frame(&mut backend_buffer)? {
            forward.put_u8(frame.tag);
            forward.put_i32((frame.payload.len() + 4) as i32);
            forward.extend_from_slice(&frame.payload);
            if let Some(status) = frame.ready_status() {
                ready = Some(status);
            }
        }

        if !forward.is_empty() {
            client
                .write_all(&forward)
                .await
                .context("write backend frames to client")?;
        }

        if let Some(status) = ready {
            return Ok(status);
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
        metrics::increment_prepared_event("parse");
        prepared.mark_materialized(backend_id, &statement);
        return Ok(PreparedForwardPlan::single(rewrite_parse_statement_name(
            &frame,
            &statement.backend_name,
        )?));
    }

    if let Some(statement_name) = parse_bind_statement_name(&frame)? {
        if let Some(statement) = prepared.get(&statement_name).cloned() {
            metrics::increment_prepared_event("bind");
            let mut prelude = Vec::new();
            if !prepared.is_materialized(backend_id, &statement) {
                prelude.push(build_parse_frame(
                    &statement.backend_name,
                    &statement.query,
                    &statement.parameter_type_oids,
                ));
                prepared.mark_materialized(backend_id, &statement);
                metrics::increment_prepared_event("materialize");
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
                metrics::increment_prepared_event("materialize");
            }

            return Ok(PreparedForwardPlan {
                prelude,
                frame: rewrite_describe_statement_name(&frame, &statement.backend_name)?,
            });
        }
    }

    if let Some(CloseTarget::Statement(statement_name)) = parse_close_target(&frame)? {
        if let Some(statement) = prepared.remove(&statement_name) {
            metrics::increment_prepared_event("close");
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

fn update_session_from_frame(
    session: &mut SessionState,
    frame: &FrontendFrame,
) -> anyhow::Result<()> {
    if let Some(query) = parse_simple_query(frame)? {
        session.apply(ClientEvent::SimpleQuery(query.to_string()));
    } else if matches!(frame.tag, b'P' | b'B' | b'D' | b'E' | b'C') {
        session.apply(ClientEvent::ExtendedQuery);
    } else if frame.tag == b'S' {
        session.apply(ClientEvent::Sync);
    }

    Ok(())
}
