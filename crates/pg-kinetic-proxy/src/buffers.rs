use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use bytes::{BufMut, Bytes, BytesMut};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferReusePolicy {
    pub initial_capacity: usize,
    pub max_cached_sessions: usize,
}

impl Default for BufferReusePolicy {
    fn default() -> Self {
        Self {
            initial_capacity: 16 * 1024,
            max_cached_sessions: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OversizedBufferPolicy {
    pub max_retained_capacity: usize,
}

impl Default for OversizedBufferPolicy {
    fn default() -> Self {
        Self {
            max_retained_capacity: 64 * 1024,
        }
    }
}

#[derive(Debug, Default)]
struct BufferCounters {
    sessions_created: AtomicU64,
    sessions_reused: AtomicU64,
    allocations: AtomicU64,
    allocation_bytes: AtomicU64,
    copies: AtomicU64,
    copied_bytes: AtomicU64,
    frontend_to_backend_copies: AtomicU64,
    frontend_to_backend_copied_bytes: AtomicU64,
    backend_to_client_copies: AtomicU64,
    backend_to_client_copied_bytes: AtomicU64,
    oversized_buffers_released: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProxyBufferStats {
    pub sessions_created: u64,
    pub sessions_reused: u64,
    pub allocations: u64,
    pub allocation_bytes: u64,
    pub copies: u64,
    pub copied_bytes: u64,
    pub frontend_to_backend_copies: u64,
    pub frontend_to_backend_copied_bytes: u64,
    pub backend_to_client_copies: u64,
    pub backend_to_client_copied_bytes: u64,
    pub oversized_buffers_released: u64,
}

#[derive(Clone, Debug)]
pub struct ProxyBufferPool {
    reuse_policy: BufferReusePolicy,
    oversized_policy: OversizedBufferPolicy,
    counters: Arc<BufferCounters>,
    available: Arc<Mutex<Vec<SessionBufferSet>>>,
}

impl Default for ProxyBufferPool {
    fn default() -> Self {
        Self::new(
            BufferReusePolicy::default(),
            OversizedBufferPolicy::default(),
        )
    }
}

impl ProxyBufferPool {
    #[must_use]
    pub fn new(reuse_policy: BufferReusePolicy, oversized_policy: OversizedBufferPolicy) -> Self {
        Self {
            reuse_policy: BufferReusePolicy {
                initial_capacity: reuse_policy.initial_capacity.max(1),
                max_cached_sessions: reuse_policy.max_cached_sessions,
            },
            oversized_policy: OversizedBufferPolicy {
                max_retained_capacity: oversized_policy
                    .max_retained_capacity
                    .max(reuse_policy.initial_capacity.max(1)),
            },
            counters: Arc::new(BufferCounters::default()),
            available: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    pub fn acquire(&self) -> SessionBufferLease {
        let buffers = self.available.lock().expect("buffer pool poisoned").pop();
        let buffers = match buffers {
            Some(buffers) => {
                self.counters
                    .sessions_reused
                    .fetch_add(1, Ordering::Relaxed);
                buffers
            }
            None => {
                self.counters
                    .sessions_created
                    .fetch_add(1, Ordering::Relaxed);
                SessionBufferSet::new(
                    self.reuse_policy,
                    self.oversized_policy,
                    Arc::clone(&self.counters),
                )
            }
        };

        SessionBufferLease {
            pool: self.clone(),
            buffers: Some(buffers),
        }
    }

    #[must_use]
    pub fn stats(&self) -> ProxyBufferStats {
        ProxyBufferStats {
            sessions_created: self.counters.sessions_created.load(Ordering::Relaxed),
            sessions_reused: self.counters.sessions_reused.load(Ordering::Relaxed),
            allocations: self.counters.allocations.load(Ordering::Relaxed),
            allocation_bytes: self.counters.allocation_bytes.load(Ordering::Relaxed),
            copies: self.counters.copies.load(Ordering::Relaxed),
            copied_bytes: self.counters.copied_bytes.load(Ordering::Relaxed),
            frontend_to_backend_copies: self
                .counters
                .frontend_to_backend_copies
                .load(Ordering::Relaxed),
            frontend_to_backend_copied_bytes: self
                .counters
                .frontend_to_backend_copied_bytes
                .load(Ordering::Relaxed),
            backend_to_client_copies: self
                .counters
                .backend_to_client_copies
                .load(Ordering::Relaxed),
            backend_to_client_copied_bytes: self
                .counters
                .backend_to_client_copied_bytes
                .load(Ordering::Relaxed),
            oversized_buffers_released: self
                .counters
                .oversized_buffers_released
                .load(Ordering::Relaxed),
        }
    }

    fn recycle(&self, mut buffers: SessionBufferSet) {
        buffers.prepare_for_reuse();
        let mut available = self.available.lock().expect("buffer pool poisoned");
        if available.len() < self.reuse_policy.max_cached_sessions {
            available.push(buffers);
        }
    }
}

#[derive(Debug)]
pub struct SessionBufferLease {
    pool: ProxyBufferPool,
    buffers: Option<SessionBufferSet>,
}

impl SessionBufferLease {
    #[must_use]
    pub fn buffers_mut(&mut self) -> &mut SessionBufferSet {
        self.buffers
            .as_mut()
            .expect("session buffer lease released")
    }
}

impl Drop for SessionBufferLease {
    fn drop(&mut self) {
        if let Some(buffers) = self.buffers.take() {
            self.pool.recycle(buffers);
        }
    }
}

#[derive(Debug)]
pub struct SessionBufferSet {
    client_read: BytesMut,
    backend_read: BytesMut,
    backend_write: BytesMut,
    client_write: BytesMut,
    backend_frames: Vec<([u8; 5], Bytes)>,
    client_read_capacity: usize,
    backend_read_capacity: usize,
    backend_write_capacity: usize,
    client_write_capacity: usize,
    initial_capacity: usize,
    oversized_policy: OversizedBufferPolicy,
    counters: Arc<BufferCounters>,
}

impl SessionBufferSet {
    fn new(
        policy: BufferReusePolicy,
        oversized_policy: OversizedBufferPolicy,
        counters: Arc<BufferCounters>,
    ) -> Self {
        let initial_capacity = policy.initial_capacity;

        Self {
            client_read: BytesMut::new(),
            backend_read: BytesMut::new(),
            backend_write: BytesMut::new(),
            client_write: BytesMut::new(),
            backend_frames: Vec::new(),
            client_read_capacity: 0,
            backend_read_capacity: 0,
            backend_write_capacity: 0,
            client_write_capacity: 0,
            initial_capacity,
            oversized_policy,
            counters,
        }
    }

    #[must_use]
    pub fn client_read_mut(&mut self) -> &mut BytesMut {
        &mut self.client_read
    }

    #[must_use]
    pub fn backend_read_mut(&mut self) -> &mut BytesMut {
        &mut self.backend_read
    }

    #[must_use]
    pub fn backend_write(&self) -> &[u8] {
        &self.backend_write
    }

    #[must_use]
    pub fn client_write(&self) -> &[u8] {
        &self.client_write
    }

    pub fn append_frontend_frame(&mut self, tag: u8, payload: &[u8]) {
        append_frame(&mut self.backend_write, tag, payload);
        self.record_frontend_copy(payload.len());
        observe_capacity(
            &self.backend_write,
            &mut self.backend_write_capacity,
            &self.counters,
        );
    }

    pub fn append_backend_frame(&mut self, tag: u8, payload: &[u8]) {
        append_frame(&mut self.client_write, tag, payload);
        self.record_backend_copy(payload.len());
        observe_capacity(
            &self.client_write,
            &mut self.client_write_capacity,
            &self.counters,
        );
    }

    pub fn clear_backend_write(&mut self) {
        self.backend_write.clear();
        self.trim_backend_write();
    }

    pub fn clear_client_write(&mut self) {
        self.client_write.clear();
        self.trim_client_write();
    }

    pub fn take_backend_frames(&mut self) -> Vec<([u8; 5], Bytes)> {
        let mut backend_frames = std::mem::take(&mut self.backend_frames);
        backend_frames.clear();
        if backend_frames.capacity() > 1024 {
            backend_frames.shrink_to(64);
        }
        backend_frames
    }

    pub fn restore_backend_frames(&mut self, backend_frames: Vec<([u8; 5], Bytes)>) {
        self.backend_frames = backend_frames;
    }

    pub fn clear_backend_frames(&mut self) {
        self.backend_frames.clear();
        if self.backend_frames.capacity() > 1024 {
            self.backend_frames.shrink_to(64);
        }
    }

    pub fn observe_client_read(&mut self) {
        observe_capacity(
            &self.client_read,
            &mut self.client_read_capacity,
            &self.counters,
        );
    }

    pub fn observe_backend_read(&mut self) {
        observe_capacity(
            &self.backend_read,
            &mut self.backend_read_capacity,
            &self.counters,
        );
    }

    pub fn trim_empty_buffers(&mut self) {
        trim_buffer(
            &mut self.client_read,
            &mut self.client_read_capacity,
            self.initial_capacity,
            self.oversized_policy,
            &self.counters,
        );
        trim_buffer(
            &mut self.backend_read,
            &mut self.backend_read_capacity,
            self.initial_capacity,
            self.oversized_policy,
            &self.counters,
        );
        trim_buffer(
            &mut self.backend_write,
            &mut self.backend_write_capacity,
            self.initial_capacity,
            self.oversized_policy,
            &self.counters,
        );
        trim_buffer(
            &mut self.client_write,
            &mut self.client_write_capacity,
            self.initial_capacity,
            self.oversized_policy,
            &self.counters,
        );
    }

    #[must_use]
    pub fn capacities(&self) -> [usize; 4] {
        [
            self.client_read.capacity(),
            self.backend_read.capacity(),
            self.backend_write.capacity(),
            self.client_write.capacity(),
        ]
    }

    fn prepare_for_reuse(&mut self) {
        self.client_read.clear();
        self.backend_read.clear();
        self.backend_write.clear();
        self.client_write.clear();
        self.backend_frames.clear();
        if self.backend_frames.capacity() > 1024 {
            self.backend_frames.shrink_to(64);
        }
        self.trim_empty_buffers();
    }

    fn record_copy(&self, payload_len: usize) {
        self.counters.copies.fetch_add(1, Ordering::Relaxed);
        self.counters
            .copied_bytes
            .fetch_add(payload_len as u64, Ordering::Relaxed);
    }

    fn record_frontend_copy(&self, payload_len: usize) {
        self.record_copy(payload_len);
        self.counters
            .frontend_to_backend_copies
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .frontend_to_backend_copied_bytes
            .fetch_add(payload_len as u64, Ordering::Relaxed);
    }

    fn record_backend_copy(&self, payload_len: usize) {
        self.record_copy(payload_len);
        self.counters
            .backend_to_client_copies
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .backend_to_client_copied_bytes
            .fetch_add(payload_len as u64, Ordering::Relaxed);
    }

    fn trim_backend_write(&mut self) {
        trim_buffer(
            &mut self.backend_write,
            &mut self.backend_write_capacity,
            self.initial_capacity,
            self.oversized_policy,
            &self.counters,
        );
    }

    fn trim_client_write(&mut self) {
        trim_buffer(
            &mut self.client_write,
            &mut self.client_write_capacity,
            self.initial_capacity,
            self.oversized_policy,
            &self.counters,
        );
    }
}

fn append_frame(buffer: &mut BytesMut, tag: u8, payload: &[u8]) {
    buffer.put_u8(tag);
    buffer.put_i32((payload.len() + 4) as i32);
    buffer.extend_from_slice(payload);
}

fn observe_capacity(buffer: &BytesMut, known_capacity: &mut usize, counters: &BufferCounters) {
    let capacity = buffer.capacity();
    if capacity > *known_capacity {
        counters.allocations.fetch_add(1, Ordering::Relaxed);
        counters
            .allocation_bytes
            .fetch_add((capacity - *known_capacity) as u64, Ordering::Relaxed);
        *known_capacity = capacity;
    }
}

fn trim_buffer(
    buffer: &mut BytesMut,
    known_capacity: &mut usize,
    initial_capacity: usize,
    policy: OversizedBufferPolicy,
    counters: &BufferCounters,
) {
    if buffer.is_empty() && buffer.capacity() > policy.max_retained_capacity {
        *buffer = BytesMut::with_capacity(initial_capacity);
        *known_capacity = initial_capacity;
        counters
            .oversized_buffers_released
            .fetch_add(1, Ordering::Relaxed);
        counters.allocations.fetch_add(1, Ordering::Relaxed);
        counters
            .allocation_bytes
            .fetch_add(initial_capacity as u64, Ordering::Relaxed);
    }
}
