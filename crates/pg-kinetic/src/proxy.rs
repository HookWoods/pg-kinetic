use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use tokio::{
    io,
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

use crate::config::Config;

#[derive(Debug)]
pub struct Proxy {
    config: Config,
    client_slots: Arc<Semaphore>,
}

impl Proxy {
    #[must_use]
    pub fn new(config: Config) -> Self {
        let client_slots = Arc::new(Semaphore::new(config.max_clients));
        Self {
            config,
            client_slots,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.config.listen_addr)
            .await
            .with_context(|| format!("bind listener {}", self.config.listen_addr))?;

        tracing::info!(listen_addr = %self.config.listen_addr, "listening");

        loop {
            let (client, client_addr) = listener.accept().await.context("accept client")?;
            let backend_addr = self.config.backend_addr;
            let permit = self.client_slots.clone().acquire_owned().await?;

            tokio::spawn(async move {
                let result = handle_client(client, client_addr, backend_addr).await;
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
    backend_addr: SocketAddr,
) -> anyhow::Result<()> {
    client.set_nodelay(true).context("set client TCP_NODELAY")?;

    let mut backend = TcpStream::connect(backend_addr)
        .await
        .with_context(|| format!("connect backend {backend_addr}"))?;
    backend
        .set_nodelay(true)
        .context("set backend TCP_NODELAY")?;

    tracing::debug!(%client_addr, %backend_addr, "proxying client");
    let (from_client, from_backend) = io::copy_bidirectional(&mut client, &mut backend)
        .await
        .context("bidirectional proxy copy")?;

    tracing::debug!(
        %client_addr,
        client_to_backend_bytes = from_client,
        backend_to_client_bytes = from_backend,
        "client proxy finished"
    );

    Ok(())
}
