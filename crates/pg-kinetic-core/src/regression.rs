use std::{fmt, path::Path, str::FromStr, sync::Arc, time::Duration};

use clap::ValueEnum;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
pub enum RegressionCategory {
    Smoke,
    Protocol,
    Docs,
    Benchmark,
    Compatibility,
}

impl RegressionCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Protocol => "protocol",
            Self::Docs => "docs",
            Self::Benchmark => "benchmark",
            Self::Compatibility => "compatibility",
        }
    }
}

impl fmt::Display for RegressionCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RegressionCategory {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "protocol" => Ok(Self::Protocol),
            "docs" => Ok(Self::Docs),
            "benchmark" => Ok(Self::Benchmark),
            "compatibility" => Ok(Self::Compatibility),
            _ => Err(format!("unknown regression category '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, ValueEnum)]
pub enum RegressionPlatform {
    Linux,
    Windows,
    All,
}

impl RegressionPlatform {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Windows => "windows",
            Self::All => "all",
        }
    }

    #[must_use]
    pub const fn supports_current_platform(self) -> bool {
        matches!(self, Self::All)
            || (cfg!(target_os = "linux") && matches!(self, Self::Linux))
            || (cfg!(target_os = "windows") && matches!(self, Self::Windows))
    }

    #[must_use]
    pub const fn matches_filter(self, filter: Self) -> bool {
        matches!(
            (self, filter),
            (_, Self::All)
                | (Self::All, _)
                | (Self::Linux, Self::Linux)
                | (Self::Windows, Self::Windows)
        )
    }
}

impl fmt::Display for RegressionPlatform {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RegressionPlatform {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "linux" => Ok(Self::Linux),
            "windows" => Ok(Self::Windows),
            "all" => Ok(Self::All),
            _ => Err(format!("unknown regression platform '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RegressionArtifactPolicy {
    None,
    Summary,
    Large,
}

impl RegressionArtifactPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Summary => "summary",
            Self::Large => "large",
        }
    }
}

impl fmt::Display for RegressionArtifactPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RegressionArtifactPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "summary" => Ok(Self::Summary),
            "large" => Ok(Self::Large),
            _ => Err(format!("unknown regression artifact policy '{value}'")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegressionCaseSpec {
    pub id: Arc<str>,
    pub category: RegressionCategory,
    pub platform: RegressionPlatform,
    pub timeout: Duration,
    pub services: Vec<Arc<str>>,
    pub command: Arc<str>,
    pub success_marker: Option<Arc<str>>,
    pub artifact_policy: RegressionArtifactPolicy,
    pub artifact_path: Option<Arc<str>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegressionCase {
    id: Arc<str>,
    category: RegressionCategory,
    platform: RegressionPlatform,
    timeout: Duration,
    services: Vec<Arc<str>>,
    command: Arc<str>,
    success_marker: Option<Arc<str>>,
    artifact_policy: RegressionArtifactPolicy,
    artifact_path: Option<Arc<str>>,
}

impl RegressionCase {
    pub fn new(spec: RegressionCaseSpec) -> Result<Self, String> {
        let RegressionCaseSpec {
            id,
            category,
            platform,
            timeout,
            services,
            command,
            success_marker,
            artifact_policy,
            artifact_path,
        } = spec;

        if id.trim().is_empty() {
            return Err(String::from("regression case id must not be empty"));
        }
        if command.trim().is_empty() {
            return Err(format!(
                "regression case '{}' command must not be empty",
                id
            ));
        }
        if timeout.is_zero() {
            return Err(format!("regression case '{}' timeout must be positive", id));
        }
        if services.iter().any(|service| service.trim().is_empty()) {
            return Err(format!(
                "regression case '{}' contains an empty service",
                id
            ));
        }
        if success_marker
            .as_deref()
            .is_some_and(|marker| marker.trim().is_empty())
        {
            return Err(format!(
                "regression case '{}' success marker must not be empty",
                id
            ));
        }
        if let Some(path) = artifact_path.as_deref() {
            validate_artifact_path(path)?;
        }

        Ok(Self {
            id,
            category,
            platform,
            timeout,
            services,
            command,
            success_marker,
            artifact_policy,
            artifact_path,
        })
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub const fn category(&self) -> RegressionCategory {
        self.category
    }

    #[must_use]
    pub const fn platform(&self) -> RegressionPlatform {
        self.platform
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    #[must_use]
    pub fn services(&self) -> &[Arc<str>] {
        &self.services
    }

    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }

    #[must_use]
    pub fn success_marker(&self) -> Option<&str> {
        self.success_marker.as_deref()
    }

    #[must_use]
    pub const fn artifact_policy(&self) -> RegressionArtifactPolicy {
        self.artifact_policy
    }

    #[must_use]
    pub fn artifact_path(&self) -> Option<&str> {
        self.artifact_path.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegressionManifest {
    cases: Vec<RegressionCase>,
}

impl RegressionManifest {
    pub fn new(cases: impl Into<Vec<RegressionCase>>) -> Result<Self, String> {
        let cases = cases.into();
        if cases.is_empty() {
            return Err(String::from(
                "regression manifest must contain at least one case",
            ));
        }
        for (index, case) in cases.iter().enumerate() {
            if cases[..index]
                .iter()
                .any(|previous| previous.id() == case.id())
            {
                return Err(format!("duplicate regression case id '{}'", case.id()));
            }
        }
        Ok(Self { cases })
    }

    #[must_use]
    pub fn cases(&self) -> &[RegressionCase] {
        &self.cases
    }
}

fn validate_artifact_path(path: &str) -> Result<(), String> {
    let path = Path::new(path);
    if path.is_absolute() {
        return Err(format!(
            "regression artifact path '{}' must be relative",
            path.display()
        ));
    }
    let components = path
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect::<Vec<_>>();
    if components.iter().any(|component| {
        matches!(
            component.to_ascii_lowercase().as_str(),
            "private" | "draft" | "drafts" | "planning" | "generated-output"
        )
    }) {
        return Err(format!(
            "regression artifact path '{}' uses a private artifact location",
            path.display()
        ));
    }
    if components.first() != Some(&"target") {
        return Err(format!(
            "regression artifact path '{}' must be under the ignored target directory",
            path.display()
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RegressionOutcome {
    Passed,
    Failed,
    Skipped,
    TimedOut,
    Blocked,
}

impl RegressionOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "pass",
            Self::Failed => "fail",
            Self::Skipped => "skip",
            Self::TimedOut => "timeout",
            Self::Blocked => "blocked",
        }
    }
}

impl fmt::Display for RegressionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
