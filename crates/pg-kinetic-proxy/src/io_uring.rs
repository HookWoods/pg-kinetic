use std::net::SocketAddr;

use crate::{config::Config, metrics};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod linux {
    use super::*;

    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc::{self, RecvTimeoutError},
            Arc,
        },
        time::{Duration, Instant},
    };

    use anyhow::{bail, Context};
    use bytes::BytesMut;
    use monoio::{
        net::{ListenerOpts, TcpListener, TcpStream},
        RuntimeBuilder,
    };
    use pg_kinetic_core::runtime::ShutdownReason;

    use crate::{
        drain::DrainController,
        lifecycle::{wait_for_shutdown_signal, LifecycleController},
    };

    pub fn run(config: Config) -> anyhow::Result<()> {
        validate_supported_config(&config)?;

        let backend_addr = direct_backend_addr(&config)?;
        let shard_count = config
            .runtime
            .engine
            .runtime_shards
            .unwrap_or_else(default_shard_count);
        let stop = Arc::new(AtomicBool::new(false));
        let start_accepting = Arc::new(AtomicBool::new(false));
        let client_capacity = Arc::new(AtomicUsize::new(0));
        let backend_capacity = Arc::new(AtomicUsize::new(0));
        let lifecycle = LifecycleController::new(
            Arc::new(DrainController::default()),
            config.drain.drain_timeout(),
            config.runtime.lifecycle.shutdown_grace(),
            config.runtime.lifecycle.readiness_fail_during_drain,
        );
        let mut shard_threads = Vec::with_capacity(shard_count);
        let (startup_tx, startup_rx) = mpsc::channel();

        for shard_id in 0..shard_count {
            let stop = Arc::clone(&stop);
            let start_accepting = Arc::clone(&start_accepting);
            let client_capacity = Arc::clone(&client_capacity);
            let backend_capacity = Arc::clone(&backend_capacity);
            let lifecycle = lifecycle.clone();
            let startup_tx = startup_tx.clone();
            let listen_addr = config.connection.listen_addr;
            let max_clients = config.capacity.max_clients;
            let max_backends = config.capacity.max_backends;
            let drain_timeout = config.drain.drain_timeout();
            let max_client_buffer_bytes = config.qos.max_client_buffer_bytes;
            let max_backend_buffer_bytes = config.qos.max_backend_buffer_bytes;
            let thread = std::thread::Builder::new()
                .name(format!("pg-kinetic-iouring-shard-{shard_id}"))
                .spawn(move || {
                    let mut runtime = match RuntimeBuilder::<monoio::IoUringDriver>::new()
                        .enable_all()
                        .with_entries(4096)
                        .build()
                        .context("build monoio io_uring runtime")
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = startup_tx.send(Err(format!("{error:#}")));
                            return Err(error);
                        }
                    };
                    runtime.block_on(run_shard(
                        shard_id,
                        listen_addr,
                        backend_addr,
                        stop,
                        start_accepting,
                        client_capacity,
                        backend_capacity,
                        lifecycle.drain_token(),
                        lifecycle.drain_controller(),
                        max_clients,
                        max_backends,
                        drain_timeout,
                        max_client_buffer_bytes,
                        max_backend_buffer_bytes,
                        startup_tx,
                    ))
                })
                .with_context(|| format!("spawn io_uring shard thread {shard_id}"))?;
            shard_threads.push(thread);
        }
        drop(startup_tx);

        if let Err(error) = wait_for_shard_startup(
            &startup_rx,
            shard_count,
            config.runtime.lifecycle.startup_grace(),
        ) {
            stop.store(true, Ordering::Release);
            lifecycle.begin_drain(ShutdownReason::StartupFailure);
            wake_accept_loops(config.connection.listen_addr, shard_count);
            if let Err(join_error) =
                join_shards(shard_threads, config.connection.listen_addr, shard_count)
            {
                tracing::debug!(error = %join_error, "io_uring startup cleanup failed");
            }
            return Err(error);
        }

        lifecycle.mark_listeners_initialized();
        lifecycle.mark_backend_pools_initialized();
        start_accepting.store(true, Ordering::Release);

        tracing::info!(
            listen_addr = %config.connection.listen_addr,
            backend_addr = %backend_addr,
            shards = shard_count,
            "experimental io_uring plaintext pass-through runtime listening"
        );

        wait_for_shutdown_blocking()?;
        stop.store(true, Ordering::Release);
        lifecycle.begin_drain(ShutdownReason::Signal);
        join_shards(shard_threads, config.connection.listen_addr, shard_count)?;

        Ok(())
    }

    fn wait_for_shard_startup(
        startup_rx: &mpsc::Receiver<Result<usize, String>>,
        shard_count: usize,
        startup_grace: Duration,
    ) -> anyhow::Result<()> {
        for _ in 0..shard_count {
            match startup_rx.recv_timeout(startup_grace) {
                Ok(Ok(_shard_id)) => {}
                Ok(Err(error)) => bail!("io_uring shard startup failed: {error}"),
                Err(RecvTimeoutError::Timeout) => {
                    bail!("io_uring shard startup timed out after {startup_grace:?}");
                }
                Err(RecvTimeoutError::Disconnected) => {
                    bail!("io_uring shard startup channel closed before all shards were ready");
                }
            }
        }
        Ok(())
    }

    fn join_shards(
        shard_threads: Vec<std::thread::JoinHandle<anyhow::Result<()>>>,
        listen_addr: SocketAddr,
        shard_count: usize,
    ) -> anyhow::Result<()> {
        wake_accept_loops(listen_addr, shard_count);
        for thread in shard_threads {
            match thread.join() {
                Ok(result) => result?,
                Err(_) => bail!("io_uring shard thread panicked"),
            }
        }
        Ok(())
    }

    fn wake_accept_loops(listen_addr: SocketAddr, shard_count: usize) {
        for _ in 0..shard_count.saturating_mul(16) {
            let _ = std::net::TcpStream::connect_timeout(&listen_addr, Duration::from_millis(10));
        }
    }

    async fn run_shard(
        shard_id: usize,
        listen_addr: SocketAddr,
        backend_addr: SocketAddr,
        stop: Arc<AtomicBool>,
        start_accepting: Arc<AtomicBool>,
        client_capacity: Arc<AtomicUsize>,
        backend_capacity: Arc<AtomicUsize>,
        drain: crate::lifecycle::DrainToken,
        drain_controller: Arc<DrainController>,
        max_clients: usize,
        max_backends: usize,
        drain_timeout: Duration,
        max_client_buffer_bytes: usize,
        max_backend_buffer_bytes: usize,
        startup_tx: mpsc::Sender<Result<usize, String>>,
    ) -> anyhow::Result<()> {
        let listener = match bind_reuseport_listener(listen_addr)
            .with_context(|| format!("bind io_uring shard listener {shard_id}"))
        {
            Ok(listener) => listener,
            Err(error) => {
                let _ = startup_tx.send(Err(format!("{error:#}")));
                return Err(error);
            }
        };
        let _ = startup_tx.send(Ok(shard_id));

        wait_for_start_gate(&start_accepting, &stop).await;
        while !stop.load(Ordering::Acquire) && drain.is_accepting() {
            let (client, _client_addr) =
                listener.accept().await.context("accept io_uring client")?;
            if stop.load(Ordering::Acquire) {
                break;
            }
            let Some(session_guard) = drain.try_enter() else {
                continue;
            };
            let Some(client_capacity_guard) =
                crate::io_runtime::try_enter_client_capacity(&client_capacity, max_clients)
            else {
                drop(session_guard);
                continue;
            };
            let backend_capacity = Arc::clone(&backend_capacity);
            monoio::spawn(async move {
                if let Err(error) = proxy_connection(
                    client,
                    backend_addr,
                    backend_capacity,
                    max_backends,
                    max_client_buffer_bytes,
                    max_backend_buffer_bytes,
                )
                .await
                {
                    tracing::debug!(shard_id, error = %error, "io_uring connection ended");
                }
                drop(client_capacity_guard);
                drop(session_guard);
            });
        }

        wait_for_active_sessions(&drain_controller, drain_timeout).await;
        Ok(())
    }

    async fn wait_for_start_gate(start_accepting: &AtomicBool, stop: &AtomicBool) {
        while !start_accepting.load(Ordering::Acquire) && !stop.load(Ordering::Acquire) {
            monoio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn wait_for_active_sessions(drain_controller: &DrainController, drain_timeout: Duration) {
        let deadline = Instant::now() + drain_timeout;
        while drain_controller.active_clients() > 0 {
            if Instant::now() >= deadline {
                tracing::warn!(
                    active_sessions = drain_controller.active_clients(),
                    "io_uring shutdown reached drain timeout with active sessions"
                );
                break;
            }
            monoio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn proxy_connection(
        client: TcpStream,
        backend_addr: SocketAddr,
        backend_capacity: Arc<AtomicUsize>,
        max_backends: usize,
        max_client_buffer_bytes: usize,
        max_backend_buffer_bytes: usize,
    ) -> anyhow::Result<()> {
        let mut client = crate::io_uring_transport::MonoioTransport::new(client);
        let mut client_buffer = BytesMut::with_capacity(16 * 1024);
        let mut backend_buffer = BytesMut::with_capacity(16 * 1024);
        let mut backend_scan_buffer = BytesMut::with_capacity(16 * 1024);

        let startup_packet = loop {
            match crate::io_runtime::take_startup_packet_bytes(
                &mut client_buffer,
                max_client_buffer_bytes,
            )? {
                crate::io_runtime::StartupPacketRead::Packet(bytes) => break bytes,
                crate::io_runtime::StartupPacketRead::Cancel { bytes } => {
                    let Some(backend_capacity_guard) =
                        crate::io_runtime::try_enter_backend_capacity(
                            &backend_capacity,
                            max_backends,
                        )
                    else {
                        anyhow::bail!("backend capacity exceeded");
                    };
                    let backend = TcpStream::connect_addr(backend_addr)
                        .await
                        .with_context(|| format!("connect io_uring backend {backend_addr}"))?;
                    let mut backend = crate::io_uring_transport::MonoioTransport::new(backend);
                    backend
                        .write_all(&bytes)
                        .await
                        .context("forward cancel request")?;
                    let _ = backend.shutdown().await;
                    drop(backend_capacity_guard);
                    return Ok(());
                }
                crate::io_runtime::StartupPacketRead::EncryptionRequest => {
                    client
                        .write_all(b"N")
                        .await
                        .context("reject startup encryption request")?;
                }
                crate::io_runtime::StartupPacketRead::BufferLimitExceeded => {
                    anyhow::bail!("client startup packet exceeded configured buffer limit");
                }
                crate::io_runtime::StartupPacketRead::NeedMoreBytes => {
                    let read = client
                        .read_into(&mut client_buffer)
                        .await
                        .context("read startup")?;
                    if read == 0 {
                        return Ok(());
                    }
                }
            }
        };

        let Some(_backend_capacity_guard) =
            crate::io_runtime::try_enter_backend_capacity(&backend_capacity, max_backends)
        else {
            anyhow::bail!("backend capacity exceeded");
        };
        let backend = TcpStream::connect_addr(backend_addr)
            .await
            .with_context(|| format!("connect io_uring backend {backend_addr}"))?;
        let mut backend = crate::io_uring_transport::MonoioTransport::new(backend);
        backend
            .write_all(&startup_packet)
            .await
            .context("forward startup")?;

        let mut startup_drain = crate::io_runtime::BackendResponseDrain::new(1, 0);
        loop {
            let read = backend
                .read_into(&mut backend_buffer)
                .await
                .context("read startup response")?;
            if read == 0 {
                anyhow::bail!("backend closed during startup");
            }
            if backend_scan_buffer.len() + backend_buffer.len() > max_backend_buffer_bytes {
                anyhow::bail!("backend response exceeded configured buffer limit");
            }
            client
                .write_all(&backend_buffer)
                .await
                .context("write startup response")?;
            backend_scan_buffer.extend_from_slice(&backend_buffer);
            backend_buffer.clear();
            if ready_seen(
                &mut backend_scan_buffer,
                &mut startup_drain,
                max_backend_buffer_bytes,
            )? {
                break;
            }
        }

        loop {
            let (client_cycle, expected_ready_count) = loop {
                match crate::io_runtime::take_frontend_cycle_bytes(
                    &mut client_buffer,
                    max_client_buffer_bytes,
                )? {
                    crate::io_runtime::FrontendCycleRead::Complete { bytes, shape } => {
                        break (bytes, shape.expected_ready_count());
                    }
                    crate::io_runtime::FrontendCycleRead::Terminate { bytes } => {
                        let _ = backend.write_all(&bytes).await;
                        let _ = backend.shutdown().await;
                        return Ok(());
                    }
                    crate::io_runtime::FrontendCycleRead::BufferLimitExceeded => {
                        anyhow::bail!("client request exceeded configured buffer limit");
                    }
                    crate::io_runtime::FrontendCycleRead::NeedMoreBytes => {
                        let read = client
                            .read_into(&mut client_buffer)
                            .await
                            .context("read client query")?;
                        if read == 0 {
                            let _ = backend.shutdown().await;
                            return Ok(());
                        }
                    }
                }
            };
            backend
                .write_all(&client_cycle)
                .await
                .context("write query")?;
            let mut response_drain =
                crate::io_runtime::BackendResponseDrain::new(expected_ready_count, 0);

            loop {
                let read = backend
                    .read_into(&mut backend_buffer)
                    .await
                    .context("read backend response")?;
                if read == 0 {
                    anyhow::bail!("backend closed during response");
                }
                if backend_scan_buffer.len() + backend_buffer.len() > max_backend_buffer_bytes {
                    anyhow::bail!("backend response exceeded configured buffer limit");
                }
                client
                    .write_all(&backend_buffer)
                    .await
                    .context("write backend response")?;
                backend_scan_buffer.extend_from_slice(&backend_buffer);
                backend_buffer.clear();
                if ready_seen(
                    &mut backend_scan_buffer,
                    &mut response_drain,
                    max_backend_buffer_bytes,
                )? {
                    break;
                }
            }
        }
    }

    fn ready_seen(
        buffer: &mut BytesMut,
        drain: &mut crate::io_runtime::BackendResponseDrain,
        max_backend_buffer_bytes: usize,
    ) -> anyhow::Result<bool> {
        let mut forwarded = Vec::new();
        let event = drain.drain_with_limit(buffer, &mut forwarded, max_backend_buffer_bytes)?;
        if matches!(
            event,
            crate::io_runtime::ResponseDrainEvent::BufferLimitExceeded
        ) {
            anyhow::bail!("backend response exceeded configured buffer limit");
        }
        Ok(matches!(
            event,
            crate::io_runtime::ResponseDrainEvent::Frames { ready: Some(_), .. }
        ))
    }

    fn bind_reuseport_listener(addr: SocketAddr) -> anyhow::Result<TcpListener> {
        let mut opts = ListenerOpts::new();
        opts.reuse_addr = true;
        opts.reuse_port = true;
        opts.backlog = 1024;
        TcpListener::bind_with_config(addr, &opts).context("bind monoio listener")
    }

    fn wait_for_shutdown_blocking() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build io_uring shutdown signal runtime")?;
        runtime
            .block_on(wait_for_shutdown_signal())
            .context("wait for io_uring shutdown signal")?;
        Ok(())
    }

    fn default_shard_count() -> usize {
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    }
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
mod linux {
    use super::*;

    pub fn run(_config: Config) -> anyhow::Result<()> {
        anyhow::bail!(
            "experimental_io_uring requires Linux and the pg-kinetic io-uring cargo feature"
        )
    }
}

pub fn run(config: Config) -> anyhow::Result<()> {
    config.validate().map_err(anyhow::Error::msg)?;
    metrics::install(metrics::MetricsConfig {
        listen_addr: config.observability.metrics_addr,
    })?;
    linux::run(config)
}

pub fn validate_supported_config_for_test(config: &Config) -> anyhow::Result<()> {
    validate_supported_config(config)
}

pub fn direct_backend_addr_for_test(config: &Config) -> anyhow::Result<SocketAddr> {
    direct_backend_addr(config)
}

fn validate_supported_config(config: &Config) -> anyhow::Result<()> {
    use crate::config::{AuthMode, BackendTlsMode, ClientTlsMode};

    if config.tls.client_tls_mode != ClientTlsMode::Disable {
        anyhow::bail!("experimental_io_uring currently requires client_tls_mode=disable");
    }
    if config.tls.backend_tls_mode != BackendTlsMode::Disable {
        anyhow::bail!("experimental_io_uring currently requires backend_tls_mode=disable");
    }
    if config.auth.auth_mode != AuthMode::PassThrough {
        anyhow::bail!("experimental_io_uring currently requires auth_mode=pass_through");
    }
    direct_backend_addr(config)?;
    Ok(())
}

fn direct_backend_addr(config: &Config) -> anyhow::Result<SocketAddr> {
    use crate::config::{BackendTlsMode, FreshnessConfig, HaConfig, ReadRoutingConfig};

    if !config.pools.is_empty() {
        anyhow::bail!(
            "experimental_io_uring currently rejects pool configs until shared pool checkout exists"
        );
    }

    let routes = config.effective_routes();
    let [route] = routes.as_slice() else {
        anyhow::bail!("experimental_io_uring currently requires a single primary route");
    };
    if !route.replicas.is_empty() {
        anyhow::bail!(
            "experimental_io_uring currently rejects replicas until route selection exists"
        );
    }
    if route.read_routing != ReadRoutingConfig::default() {
        anyhow::bail!(
            "experimental_io_uring currently rejects read routing until route selection exists"
        );
    }
    if route.freshness != FreshnessConfig::default() {
        anyhow::bail!(
            "experimental_io_uring currently rejects freshness policy until route selection exists"
        );
    }
    if route.ha != HaConfig::default() {
        anyhow::bail!(
            "experimental_io_uring currently rejects route HA until route selection exists"
        );
    }
    if route.primary.tls_mode != BackendTlsMode::Disable {
        anyhow::bail!("experimental_io_uring currently requires route primary tls_mode=disable");
    }
    Ok(route.primary.address)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::{AuthMode, ClientTlsMode, PoolConfig, ReadRoutingConfig, RouteConfig};
    use pg_kinetic_core::routing::ReadRoutingMode;

    #[test]
    fn supported_config_accepts_plain_pass_through_defaults() {
        let config = Config::default();

        validate_supported_config(&config).expect("default config is supported");
    }

    #[test]
    fn supported_config_rejects_managed_auth() {
        let mut config = Config::default();
        config.auth.auth_mode = AuthMode::Trust;

        let error = validate_supported_config(&config).expect_err("managed auth is rejected");

        assert!(error.to_string().contains("auth_mode=pass_through"));
    }

    #[test]
    fn supported_config_rejects_tls() {
        let mut config = Config::default();
        config.tls.client_tls_mode = ClientTlsMode::VerifyClient;

        let error = validate_supported_config(&config).expect_err("client TLS is rejected");

        assert!(error.to_string().contains("client_tls_mode=disable"));
    }

    #[test]
    fn supported_config_accepts_single_primary_route_configuration() {
        let mut config = Config::default();
        config.routes = vec![crate::config::RouteConfig::from_backend_addr(
            "127.0.0.1:6544".parse().expect("route addr"),
        )];

        let addr = direct_backend_addr(&config).expect("single primary route is supported");

        assert_eq!(addr.to_string(), "127.0.0.1:6544");
    }

    #[test]
    fn supported_config_rejects_multiple_route_configurations() {
        let mut config = Config::default();
        config.routes = vec![
            RouteConfig::from_backend_addr("127.0.0.1:6544".parse().expect("route addr")),
            RouteConfig::from_backend_addr("127.0.0.1:6545".parse().expect("route addr")),
        ];

        let error = validate_supported_config(&config).expect_err("multiple routes are rejected");

        assert!(error.to_string().contains("single primary route"));
    }

    #[test]
    fn supported_config_rejects_pool_configuration() {
        let mut config = Config::default();
        config.pools = vec![PoolConfig {
            database: "app".to_string(),
            user: "app".to_string(),
            backend_addr: "127.0.0.1:6544".parse().expect("pool addr"),
            max_backends: None,
        }];

        let error = validate_supported_config(&config).expect_err("pools are rejected");

        assert!(error.to_string().contains("pool configs"));
    }

    #[test]
    fn supported_config_rejects_read_routing() {
        let mut config = Config::default();
        let mut route =
            RouteConfig::from_backend_addr("127.0.0.1:6544".parse().expect("route addr"));
        route.read_routing = ReadRoutingConfig {
            read_routing_mode: ReadRoutingMode::PreferReplica,
            ..ReadRoutingConfig::default()
        };
        config.routes = vec![route];

        let error = validate_supported_config(&config).expect_err("read routing is rejected");

        assert!(error.to_string().contains("read routing"));
    }
}
