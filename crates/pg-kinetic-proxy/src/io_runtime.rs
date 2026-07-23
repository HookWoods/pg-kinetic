use bytes::BytesMut;

#[derive(Debug, Default)]
pub struct PlannedFrontendCycle {
    pub backend_bytes: BytesMut,
    pub injected_parse_completes: usize,
    pub needs_sync: bool,
}
