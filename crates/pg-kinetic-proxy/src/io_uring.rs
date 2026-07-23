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
    use monoio::{
        io::{AsyncWriteRent, Splitable},
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

        let shard_count = config
            .runtime
            .engine
            .runtime_shards
            .unwrap_or_else(default_shard_count);
        let stop = Arc::new(AtomicBool::new(false));
        let start_accepting = Arc::new(AtomicBool::new(false));
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
            let lifecycle = lifecycle.clone();
            let startup_tx = startup_tx.clone();
            let listen_addr = config.connection.listen_addr;
            let backend_addr = config.connection.backend_addr;
            let drain_timeout = config.drain.drain_timeout();
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
                        lifecycle.drain_token(),
                        lifecycle.drain_controller(),
                        drain_timeout,
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
            backend_addr = %config.connection.backend_addr,
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
        drain: crate::lifecycle::DrainToken,
        drain_controller: Arc<DrainController>,
        drain_timeout: Duration,
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
            monoio::spawn(async move {
                if let Err(error) = proxy_connection(client, backend_addr).await {
                    tracing::debug!(shard_id, error = %error, "io_uring connection ended");
                }
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

    async fn proxy_connection(client: TcpStream, backend_addr: SocketAddr) -> anyhow::Result<()> {
        let backend = TcpStream::connect_addr(backend_addr)
            .await
            .with_context(|| format!("connect io_uring backend {backend_addr}"))?;
        let (mut client_read, mut client_write) = client.into_split();
        let (mut backend_read, mut backend_write) = backend.into_split();

        monoio::select! {
            client_to_backend = monoio::io::copy(&mut client_read, &mut backend_write) => {
                let _ = backend_write.shutdown().await;
                client_to_backend.context("copy client to backend")?;
            }
            backend_to_client = monoio::io::copy(&mut backend_read, &mut client_write) => {
                let _ = client_write.shutdown().await;
                backend_to_client.context("copy backend to client")?;
            }
        }
        Ok(())
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
    if !config.routes.is_empty() {
        anyhow::bail!(
            "experimental_io_uring currently requires routes to be omitted; it uses connection.backend_addr directly"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::{AuthMode, ClientTlsMode};

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
    fn supported_config_rejects_route_configuration() {
        let mut config = Config::default();
        config.routes = vec![crate::config::RouteConfig::from_backend_addr(
            config.connection.backend_addr,
        )];

        let error = validate_supported_config(&config).expect_err("routes are rejected");

        assert!(error.to_string().contains("routes to be omitted"));
    }
}
