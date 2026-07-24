use std::net::SocketAddr;

use crate::{config::Config, metrics};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod linux {
    use super::*;

    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
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

    use crate::{drain::DrainController, lifecycle::wait_for_shutdown_signal};

    pub fn run(config: Config) -> anyhow::Result<()> {
        let proxy = crate::proxy::Proxy::new(config);
        let runtime_state = Arc::new(proxy.initialize_runtime_state()?);
        let config = runtime_state.effective_config().clone();
        validate_supported_config(&config)?;
        let shard_count = config
            .runtime
            .engine
            .runtime_shards
            .unwrap_or_else(default_shard_count);
        let stop = Arc::new(AtomicBool::new(false));
        let start_accepting = Arc::new(AtomicBool::new(false));
        let buffer_pool = proxy.buffer_pool();
        let client_slots = proxy.client_slots();
        let backend_slots = proxy.backend_slots();
        let lifecycle = proxy.lifecycle_controller();
        let mut shard_threads = Vec::with_capacity(shard_count);
        let (startup_tx, startup_rx) = mpsc::channel();

        for shard_id in 0..shard_count {
            let stop = Arc::clone(&stop);
            let start_accepting = Arc::clone(&start_accepting);
            let buffer_pool = buffer_pool.clone();
            let client_slots = Arc::clone(&client_slots);
            let backend_slots = Arc::clone(&backend_slots);
            let runtime_state = Arc::clone(&runtime_state);
            let lifecycle = lifecycle.clone();
            let startup_tx = startup_tx.clone();
            let listen_addr = config.connection.listen_addr;
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
                        runtime_state,
                        stop,
                        start_accepting,
                        buffer_pool,
                        client_slots,
                        backend_slots,
                        lifecycle.drain_token(),
                        lifecycle.drain_controller(),
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
            backend_addr = %runtime_state.default_primary_backend_addr(),
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
        runtime_state: Arc<crate::proxy::ProxyRuntimeState>,
        stop: Arc<AtomicBool>,
        start_accepting: Arc<AtomicBool>,
        buffer_pool: crate::buffers::ProxyBufferPool,
        client_slots: Arc<tokio::sync::Semaphore>,
        backend_slots: Arc<tokio::sync::Semaphore>,
        drain: crate::lifecycle::DrainToken,
        drain_controller: Arc<DrainController>,
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
        let backend_pool = crate::io_uring_transport::MonoioBackendPool::new(
            runtime_state.default_primary_backend_addr(),
            crate::config::PoolLifecycleConfig::default(),
            128,
            128,
            128,
            Duration::from_millis(500),
            Some(backend_slots),
            None,
            None,
        );

        wait_for_start_gate(&start_accepting, &stop).await;
        while !stop.load(Ordering::Acquire) && drain.is_accepting() {
            let (client, client_addr) =
                listener.accept().await.context("accept io_uring client")?;
            if stop.load(Ordering::Acquire) {
                break;
            }
            let Some(session_guard) = drain.try_enter() else {
                continue;
            };
            let Ok(client_capacity_guard) = Arc::clone(&client_slots).try_acquire_owned() else {
                drop(session_guard);
                continue;
            };
            let buffer_pool = buffer_pool.clone();
            let backend_pool = Arc::clone(&backend_pool);
            let runtime_state = Arc::clone(&runtime_state);
            monoio::spawn(async move {
                if let Err(error) = proxy_connection(
                    client,
                    client_addr,
                    runtime_state,
                    buffer_pool,
                    backend_pool,
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
        client_addr: SocketAddr,
        runtime_state: Arc<crate::proxy::ProxyRuntimeState>,
        buffer_pool: crate::buffers::ProxyBufferPool,
        backend_pool: Arc<crate::io_uring_transport::MonoioBackendPool>,
        max_client_buffer_bytes: usize,
        max_backend_buffer_bytes: usize,
    ) -> anyhow::Result<()> {
        let mut client = crate::io_uring_transport::MonoioTransport::new(client);
        let mut client_buffer = BytesMut::with_capacity(16 * 1024);
        let startup_packet = match crate::proxy::handle_startup_or_cancel(
            &mut client,
            &mut client_buffer,
            max_client_buffer_bytes,
        )
        .await?
        {
            crate::proxy::StartupOrCancel::Startup(bytes) => bytes,
            crate::proxy::StartupOrCancel::Cancel { bytes, .. } => {
                let backend_addr = runtime_state.default_primary_backend_addr();
                let backend = TcpStream::connect_addr(backend_addr)
                    .await
                    .with_context(|| format!("connect io_uring backend {backend_addr}"))?;
                let mut backend = crate::io_uring_transport::MonoioTransport::new(backend);
                crate::io_runtime::write_all_to(&mut backend, &bytes)
                    .await
                    .context("forward cancel request")?;
                let _ = crate::io_runtime::shutdown(&mut backend).await;
                return Ok(());
            }
            crate::proxy::StartupOrCancel::Finished => return Ok(()),
        };
        let backend_credentials = runtime_state.backend_credentials();
        let startup_plan = runtime_state
            .startup_backend_plan(
                &startup_packet,
                client_addr,
                backend_credentials
                    .as_deref()
                    .map(crate::auth::BackendCredentials::username),
            )
            .context("resolve startup backend")?;
        let effective_config = runtime_state.effective_config();
        let context = crate::proxy::SharedClientSessionContext {
            pool: backend_pool,
            route: startup_plan.session_route,
            route_user: startup_plan.route_user,
            backend_startup_packet: startup_plan.backend_startup_packet,
            buffer_pool,
            max_client_buffer_bytes,
            max_backend_buffer_bytes,
            auth: effective_config.auth.clone(),
            auth_users: crate::reload::load_auth_users(effective_config)?,
            auth_query_service: runtime_state.auth_query_service(),
            backend_credentials,
            _backend: std::marker::PhantomData,
        };
        crate::proxy::handle_client_session::<
            _,
            crate::io_uring_transport::MonoioBackend,
            Arc<crate::io_uring_transport::MonoioBackendPool>,
        >(client, client_addr, context)
        .await
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoUringSessionLifecycleSummary {
    pub client_transport: &'static str,
    pub backend_checkout: &'static str,
    pub session_lifecycle: &'static str,
}

pub fn session_lifecycle_summary_for_test(
    config: Config,
) -> anyhow::Result<IoUringSessionLifecycleSummary> {
    validate_supported_config(&config)?;
    Ok(IoUringSessionLifecycleSummary {
        client_transport: "monoio",
        backend_checkout: "shared_pool",
        session_lifecycle: "shared_proxy",
    })
}

pub fn direct_backend_addr_for_test(config: &Config) -> anyhow::Result<SocketAddr> {
    direct_backend_addr(config)
}

pub fn startup_backend_addr_for_test(
    config: Config,
    startup_packet: &[u8],
    client_addr: SocketAddr,
) -> anyhow::Result<SocketAddr> {
    validate_supported_config(&config)?;
    let proxy = crate::proxy::Proxy::new(config);
    let runtime_state = proxy.initialize_runtime_state()?;
    runtime_state.startup_primary_backend_addr(startup_packet, client_addr)
}

#[derive(Debug)]
pub struct StartupBackendPlanForTest {
    pub backend_addr: SocketAddr,
    pub backend_startup_packet: bytes::BytesMut,
}

pub fn startup_backend_plan_for_test(
    config: Config,
    startup_packet: &[u8],
    client_addr: SocketAddr,
) -> anyhow::Result<StartupBackendPlanForTest> {
    validate_supported_config(&config)?;
    let proxy = crate::proxy::Proxy::new(config);
    let runtime_state = proxy.initialize_runtime_state()?;
    let plan = runtime_state.startup_backend_plan(startup_packet, client_addr, None)?;
    Ok(StartupBackendPlanForTest {
        backend_addr: plan.primary_backend_addr(),
        backend_startup_packet: plan.backend_startup_packet,
    })
}

pub fn shared_capacity_limits_for_test(config: Config) -> anyhow::Result<(usize, usize)> {
    validate_supported_config(&config)?;
    let proxy = crate::proxy::Proxy::new(config);
    let _runtime_state = proxy.initialize_runtime_state()?;
    Ok((
        proxy.available_client_slots(),
        proxy.available_backend_slots(),
    ))
}

fn validate_supported_config(config: &Config) -> anyhow::Result<()> {
    use crate::config::{BackendTlsMode, ClientTlsMode};

    if config.tls.client_tls_mode != ClientTlsMode::Disable {
        anyhow::bail!("experimental_io_uring currently requires client_tls_mode=disable");
    }
    if config.tls.backend_tls_mode != BackendTlsMode::Disable {
        anyhow::bail!("experimental_io_uring currently requires backend_tls_mode=disable");
    }
    direct_backend_addr(config)?;
    Ok(())
}

fn direct_backend_addr(config: &Config) -> anyhow::Result<SocketAddr> {
    use crate::config::{BackendTlsMode, FreshnessConfig, HaConfig, ReadRoutingConfig};

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
    fn supported_config_accepts_managed_auth() {
        let mut config = Config::default();
        config.auth.auth_mode = AuthMode::Trust;

        validate_supported_config(&config).expect("managed auth uses shared auth path");
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
    fn supported_config_accepts_pool_configuration() {
        let mut config = Config::default();
        config.pools = vec![PoolConfig {
            database: "app".to_string(),
            user: "app".to_string(),
            backend_addr: "127.0.0.1:6544".parse().expect("pool addr"),
            max_backends: None,
        }];

        validate_supported_config(&config).expect("pool configuration is supported");
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
