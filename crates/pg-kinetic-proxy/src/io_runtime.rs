use bytes::BytesMut;
use pg_kinetic_wire::backend::ReadyStatus;

#[derive(Debug, Default)]
pub struct PlannedFrontendCycle {
    pub backend_bytes: BytesMut,
    pub injected_parse_completes: usize,
    pub needs_sync: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseDrainEvent {
    Frames {
        ready: Option<ReadyStatus>,
        response_started: bool,
    },
    BufferLimitExceeded,
    NeedMoreBytes,
}
