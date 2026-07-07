use core::fmt;
use core::str::FromStr;

use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PgLsn(u64);

impl PgLsn {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn from_parts(segment: u32, offset: u32) -> Self {
        Self(((segment as u64) << 32) | offset as u64)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn segment(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[must_use]
    pub const fn offset(self) -> u32 {
        self.0 as u32
    }
}

impl From<u64> for PgLsn {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<PgLsn> for u64 {
    fn from(value: PgLsn) -> Self {
        value.0
    }
}

impl fmt::Display for PgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.segment(), self.offset())
    }
}

impl FromStr for PgLsn {
    type Err = LsnParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (segment, offset) = input.split_once('/').ok_or(LsnParseError::MissingSeparator)?;
        if segment.is_empty() || offset.is_empty() {
            return Err(LsnParseError::EmptyPart);
        }

        let segment = parse_hex_u32(
            segment,
            LsnParseError::InvalidSegment,
            LsnParseError::SegmentOverflow,
        )?;
        let offset = parse_hex_u32(
            offset,
            LsnParseError::InvalidOffset,
            LsnParseError::OffsetOverflow,
        )?;

        Ok(Self::from_parts(segment, offset))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum LsnParseError {
    #[error("LSN must contain exactly one slash")]
    MissingSeparator,

    #[error("LSN segment or offset is empty")]
    EmptyPart,

    #[error("LSN segment contains a non-hex digit")]
    InvalidSegment,

    #[error("LSN offset contains a non-hex digit")]
    InvalidOffset,

    #[error("LSN segment overflows 32 bits")]
    SegmentOverflow,

    #[error("LSN offset overflows 32 bits")]
    OffsetOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FreshnessRequirement {
    session_write_lsn: Option<PgLsn>,
}

impl FreshnessRequirement {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            session_write_lsn: None,
        }
    }

    #[must_use]
    pub const fn session_write_lsn(session_write_lsn: PgLsn) -> Self {
        Self {
            session_write_lsn: Some(session_write_lsn),
        }
    }

    #[must_use]
    pub const fn required_session_write_lsn(self) -> Option<PgLsn> {
        self.session_write_lsn
    }

    #[must_use]
    pub fn is_satisfied_by(self, replica_replay_lsn: PgLsn) -> bool {
        match self.session_write_lsn {
            Some(required_session_write_lsn) => replica_replay_lsn >= required_session_write_lsn,
            None => true,
        }
    }

    #[must_use]
    pub fn status(self, replica_replay_state: ReplicaReplayState) -> FreshnessStatus {
        match replica_replay_state {
            ReplicaReplayState::ReplayLsn(replica_replay_lsn) => {
                if self.is_satisfied_by(replica_replay_lsn) {
                    FreshnessStatus::Satisfied
                } else {
                    FreshnessStatus::Waiting
                }
            }
            ReplicaReplayState::Stale => FreshnessStatus::Stale,
            ReplicaReplayState::Unknown => FreshnessStatus::Unknown,
            ReplicaReplayState::Unavailable => FreshnessStatus::Unavailable,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FreshnessStatus {
    Satisfied,
    Waiting,
    Stale,
    Unknown,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaReplayState {
    ReplayLsn(PgLsn),
    Stale,
    Unknown,
    Unavailable,
}

impl ReplicaReplayState {
    #[must_use]
    pub const fn replay_lsn(self) -> Option<PgLsn> {
        match self {
            Self::ReplayLsn(replay_lsn) => Some(replay_lsn),
            Self::Stale | Self::Unknown | Self::Unavailable => None,
        }
    }
}

fn parse_hex_u32(
    input: &str,
    invalid_error: LsnParseError,
    overflow_error: LsnParseError,
) -> Result<u32, LsnParseError> {
    let mut value = 0u32;
    let mut digits = 0usize;

    for byte in input.bytes() {
        if digits == 8 {
            return Err(overflow_error);
        }

        let digit = match byte {
            b'0'..=b'9' => (byte - b'0') as u32,
            b'a'..=b'f' => (byte - b'a' + 10) as u32,
            b'A'..=b'F' => (byte - b'A' + 10) as u32,
            _ => return Err(invalid_error),
        };

        value = value
            .checked_mul(16)
            .and_then(|current| current.checked_add(digit))
            .ok_or(overflow_error)?;
        digits += 1;
    }

    Ok(value)
}
