#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientTlsMode {
    Disable,
    Allow,
    Require,
    VerifyClient,
}

impl ClientTlsMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Allow => "allow",
            Self::Require => "require",
            Self::VerifyClient => "verify_client",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendTlsMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl BackendTlsMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify_ca",
            Self::VerifyFull => "verify_full",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthMode {
    PassThrough,
    Trust,
    ScramSha256,
}

impl AuthMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PassThrough => "pass_through",
            Self::Trust => "trust",
            Self::ScramSha256 => "scram_sha_256",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainState {
    Accepting,
    Draining,
    Drained,
}

impl DrainState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepting => "accepting",
            Self::Draining => "draining",
            Self::Drained => "drained",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthStatus {
    Ready,
    NotReady,
    Live,
    Degraded,
}

impl HealthStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NotReady => "not_ready",
            Self::Live => "live",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadField {
    Qos,
    Timeouts,
    Socket,
    TlsCertificates,
    AuthUsers,
}

impl ReloadField {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Qos => "qos",
            Self::Timeouts => "timeouts",
            Self::Socket => "socket",
            Self::TlsCertificates => "tls_certificates",
            Self::AuthUsers => "auth_users",
        }
    }
}
