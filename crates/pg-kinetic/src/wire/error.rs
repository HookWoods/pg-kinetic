#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("invalid startup packet length: {0}")]
    InvalidStartupLength(i32),

    #[error("startup packet is incomplete")]
    IncompleteStartupPacket,

    #[error("startup packet parameter list is not null terminated")]
    UnterminatedStartupParameters,

    #[error("startup packet parameter key has no value: {0}")]
    MissingStartupParameterValue(String),

    #[error("startup packet contains invalid utf-8")]
    InvalidUtf8,

    #[error("frontend frame length is invalid: {0}")]
    InvalidFrameLength(i32),

    #[error("frontend frame is incomplete")]
    IncompleteFrame,
}
