#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlState {
    TooManyConnections,
    QueryCanceled,
    OperatorIntervention,
    InvalidSqlStatementName,
    FeatureNotSupported,
    UndefinedTable,
    UndefinedColumn,
    UniqueViolation,
}

impl SqlState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TooManyConnections => "53300",
            Self::QueryCanceled => "57014",
            Self::OperatorIntervention => "57000",
            Self::InvalidSqlStatementName => "26000",
            Self::FeatureNotSupported => "0A000",
            Self::UndefinedTable => "42P01",
            Self::UndefinedColumn => "42703",
            Self::UniqueViolation => "23505",
        }
    }

    #[must_use]
    pub fn from_str(code: &str) -> Option<Self> {
        match code {
            "53300" => Some(Self::TooManyConnections),
            "57014" => Some(Self::QueryCanceled),
            "57000" => Some(Self::OperatorIntervention),
            "26000" => Some(Self::InvalidSqlStatementName),
            "0A000" => Some(Self::FeatureNotSupported),
            "42P01" => Some(Self::UndefinedTable),
            "42703" => Some(Self::UndefinedColumn),
            "23505" => Some(Self::UniqueViolation),
            _ => None,
        }
    }
}
