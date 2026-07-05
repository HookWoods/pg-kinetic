#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PinnedBackend {
    backend_id: Option<u64>,
}

impl PinnedBackend {
    pub fn mark_pinned(&mut self, backend_id: u64) {
        self.backend_id = Some(backend_id);
    }

    pub fn clear(&mut self) {
        self.backend_id = None;
    }

    #[must_use]
    pub const fn is_pinned(&self) -> bool {
        self.backend_id.is_some()
    }

    #[must_use]
    pub const fn backend_id(&self) -> Option<u64> {
        self.backend_id
    }
}
