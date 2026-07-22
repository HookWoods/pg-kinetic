#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolVersion {
    V3,
}

impl ProtocolVersion {
    #[must_use]
    pub const fn to_i32(self) -> i32 {
        match self {
            Self::V3 => 196_608,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendTag {
    Query,
    Parse,
    Bind,
    Describe,
    Execute,
    Close,
    Sync,
    Terminate,
}

impl From<FrontendTag> for u8 {
    fn from(tag: FrontendTag) -> Self {
        match tag {
            FrontendTag::Query => b'Q',
            FrontendTag::Parse => b'P',
            FrontendTag::Bind => b'B',
            FrontendTag::Describe => b'D',
            FrontendTag::Execute => b'E',
            FrontendTag::Close => b'C',
            FrontendTag::Sync => b'S',
            FrontendTag::Terminate => b'X',
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendTag {
    Authentication,
    BackendKeyData,
    CommandComplete,
    DataRow,
    ErrorResponse,
    ParameterStatus,
    ReadyForQuery,
}

impl From<BackendTag> for u8 {
    fn from(tag: BackendTag) -> Self {
        match tag {
            BackendTag::Authentication => b'R',
            BackendTag::BackendKeyData => b'K',
            BackendTag::CommandComplete => b'C',
            BackendTag::DataRow => b'D',
            BackendTag::ErrorResponse => b'E',
            BackendTag::ParameterStatus => b'S',
            BackendTag::ReadyForQuery => b'Z',
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadyStatusByte {
    Idle,
    InTransaction,
    FailedTransaction,
}

impl From<ReadyStatusByte> for u8 {
    fn from(status: ReadyStatusByte) -> Self {
        match status {
            ReadyStatusByte::Idle => b'I',
            ReadyStatusByte::InTransaction => b'T',
            ReadyStatusByte::FailedTransaction => b'E',
        }
    }
}

pub const SSL_REQUEST_CODE: i32 = 80_877_103;
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;
