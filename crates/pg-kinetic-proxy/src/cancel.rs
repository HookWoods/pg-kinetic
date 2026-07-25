use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
};

use anyhow::Context;
use bytes::{BufMut, BytesMut};
use pg_kinetic_core::secrets;
use pg_kinetic_wire::protocol::CANCEL_REQUEST_CODE;
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelTarget {
    pub backend_addr: SocketAddr,
    pub process_id: i32,
    pub secret_key: i32,
}

#[derive(Debug, Default)]
pub struct CancelRegistry {
    entries: RwLock<HashMap<(i32, i32), Arc<CancelBinding>>>,
}

#[derive(Debug)]
struct CancelBinding {
    target: Mutex<Option<CancelTarget>>,
    forwarding: AtomicBool,
    forwarding_done: Notify,
}

pub(crate) struct CancelForwardingLease {
    binding: Arc<CancelBinding>,
    target: CancelTarget,
}

impl CancelForwardingLease {
    pub(crate) fn target(&self) -> CancelTarget {
        self.target
    }
}

impl Drop for CancelForwardingLease {
    fn drop(&mut self) {
        self.binding.forwarding.store(false, Ordering::Release);
        self.binding.forwarding_done.notify_waiters();
    }
}

impl CancelRegistry {
    pub fn issue_client_key(&self) -> anyhow::Result<(i32, i32)> {
        loop {
            let nonce = secrets::generate_nonce().context("generate cancel key nonce")?;
            let digest = Sha256::digest(nonce.as_bytes());
            let process_id = i32::from_be_bytes(digest[0..4].try_into().expect("process id bytes"))
                & 0x7fff_ffff;
            if process_id == 0 {
                continue;
            }
            let secret_key = i32::from_be_bytes(digest[4..8].try_into().expect("secret key bytes"));
            let key = (process_id, secret_key);

            let mut entries = self.entries.write().expect("cancel registry poisoned");
            if let std::collections::hash_map::Entry::Vacant(entry) = entries.entry(key) {
                entry.insert(Arc::new(CancelBinding {
                    target: Mutex::new(None),
                    forwarding: AtomicBool::new(false),
                    forwarding_done: Notify::new(),
                }));
                return Ok(key);
            }
        }
    }

    pub fn bind(&self, key: (i32, i32), target: CancelTarget) {
        if let Some(binding) = self
            .entries
            .read()
            .expect("cancel registry poisoned")
            .get(&key)
        {
            *binding.target.lock().expect("cancel binding poisoned") = Some(target);
        }
    }

    pub async fn unbind(&self, key: (i32, i32)) {
        let binding = self
            .entries
            .read()
            .expect("cancel registry poisoned")
            .get(&key)
            .cloned();
        let Some(binding) = binding else { return };
        *binding.target.lock().expect("cancel binding poisoned") = None;
        while binding.forwarding.load(Ordering::Acquire) {
            binding.forwarding_done.notified().await;
        }
    }

    #[must_use]
    pub fn lookup(&self, key: (i32, i32)) -> Option<CancelTarget> {
        self.entries
            .read()
            .expect("cancel registry poisoned")
            .get(&key)
            .and_then(|binding| {
                binding
                    .target
                    .lock()
                    .expect("cancel binding poisoned")
                    .as_ref()
                    .copied()
            })
    }

    pub(crate) fn acquire_forwarding(&self, key: (i32, i32)) -> Option<CancelForwardingLease> {
        let binding = self
            .entries
            .read()
            .expect("cancel registry poisoned")
            .get(&key)
            .cloned()?;
        if binding
            .forwarding
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        let target = binding
            .target
            .lock()
            .expect("cancel binding poisoned")
            .as_ref()
            .copied();
        match target {
            Some(target) => Some(CancelForwardingLease { binding, target }),
            None => {
                binding.forwarding.store(false, Ordering::Release);
                binding.forwarding_done.notify_waiters();
                None
            }
        }
    }

    pub async fn forward_cancel(&self, key: (i32, i32)) -> anyhow::Result<()> {
        let Some(lease) = self.acquire_forwarding(key) else {
            return Ok(());
        };
        forward_cancel(lease.target()).await
    }

    pub fn remove_session(&self, key: (i32, i32)) {
        self.entries
            .write()
            .expect("cancel registry poisoned")
            .remove(&key);
    }
}

pub async fn forward_cancel(target: CancelTarget) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(target.backend_addr)
        .await
        .with_context(|| format!("connect backend {} for cancel", target.backend_addr))?;
    let packet = encode_cancel_request(target.process_id, target.secret_key);
    stream
        .write_all(&packet)
        .await
        .context("write backend cancel request")?;
    Ok(())
}

pub(crate) fn encode_cancel_request(process_id: i32, secret_key: i32) -> BytesMut {
    let mut packet = BytesMut::with_capacity(16);
    packet.put_i32(16);
    packet.put_i32(CANCEL_REQUEST_CODE);
    packet.put_i32(process_id);
    packet.put_i32(secret_key);
    packet
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn bind_lookup_unbind_cycle() {
        let registry = CancelRegistry::default();
        let key = registry.issue_client_key().expect("client key");
        assert_eq!(registry.lookup(key), None);

        let target = CancelTarget {
            backend_addr: "127.0.0.1:5432".parse().expect("addr"),
            process_id: 7,
            secret_key: 9,
        };
        registry.bind(key, target);
        assert_eq!(registry.lookup(key), Some(target));
        registry.unbind(key).await;
        assert_eq!(registry.lookup(key), None);
        registry.remove_session(key);
        registry.bind(key, target);
        assert_eq!(registry.lookup(key), None);
    }

    #[tokio::test]
    async fn unbind_waits_for_forwarding_lease() {
        let registry = Arc::new(CancelRegistry::default());
        let key = registry.issue_client_key().expect("client key");
        registry.bind(
            key,
            CancelTarget {
                backend_addr: "127.0.0.1:5432".parse().expect("addr"),
                process_id: 7,
                secret_key: 9,
            },
        );
        let lease = registry.acquire_forwarding(key).expect("forwarding lease");

        let unbind_registry = Arc::clone(&registry);
        let mut unbind = tokio::spawn(async move {
            unbind_registry.unbind(key).await;
        });
        tokio::task::yield_now().await;
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut unbind)
            .await
            .is_err());

        drop(lease);
        tokio::time::timeout(Duration::from_secs(1), unbind)
            .await
            .expect("unbind completes after lease drop")
            .expect("unbind task completes");
        assert_eq!(registry.lookup(key), None);
    }

    #[test]
    fn cancel_packet_uses_target_credentials() {
        let packet = encode_cancel_request(7, 9);
        assert_eq!(
            &packet[..],
            &[0, 0, 0, 16, 4, 210, 22, 46, 0, 0, 0, 7, 0, 0, 0, 9,]
        );
    }
}
