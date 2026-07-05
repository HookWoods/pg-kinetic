use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::Context;
use tokio::net::TcpStream;

static NEXT_BACKEND_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Backend {
    id: u64,
    stream: TcpStream,
    addr: SocketAddr,
}

impl Backend {
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect backend {addr}"))?;
        stream
            .set_nodelay(true)
            .context("set backend TCP_NODELAY")?;

        Ok(Self {
            id: NEXT_BACKEND_ID.fetch_add(1, Ordering::Relaxed),
            stream,
            addr,
        })
    }

    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }
}
