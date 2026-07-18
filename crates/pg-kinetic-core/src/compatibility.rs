use std::{fmt, path::Path, str::FromStr, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CompatibilityLanguage {
    Rust,
    Go,
    Java,
    JavaScript,
    Python,
    DotNet,
    C,
    Cpp,
}

impl CompatibilityLanguage {
    pub const ALL: [Self; 8] = [
        Self::Rust,
        Self::Go,
        Self::Java,
        Self::JavaScript,
        Self::Python,
        Self::DotNet,
        Self::C,
        Self::Cpp,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Go => "go",
            Self::Java => "java",
            Self::JavaScript => "javascript",
            Self::Python => "python",
            Self::DotNet => "dotnet",
            Self::C => "c",
            Self::Cpp => "cpp",
        }
    }
}

impl fmt::Display for CompatibilityLanguage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CompatibilityLanguage {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "rust" => Ok(Self::Rust),
            "go" => Ok(Self::Go),
            "java" => Ok(Self::Java),
            "javascript" => Ok(Self::JavaScript),
            "python" => Ok(Self::Python),
            "dotnet" => Ok(Self::DotNet),
            "c" => Ok(Self::C),
            "cpp" => Ok(Self::Cpp),
            _ => Err(format!("unknown compatibility language '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CompatibilityTarget {
    DirectPostgres,
    PgKinetic,
}

impl CompatibilityTarget {
    pub const ALL: [Self; 2] = [Self::DirectPostgres, Self::PgKinetic];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectPostgres => "direct-postgres",
            Self::PgKinetic => "pg-kinetic",
        }
    }
}

impl fmt::Display for CompatibilityTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CompatibilityTarget {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "direct-postgres" => Ok(Self::DirectPostgres),
            "pg-kinetic" => Ok(Self::PgKinetic),
            _ => Err(format!("unknown compatibility target '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompatibilityArtifactPolicy {
    None,
    Summary,
    Large,
}

impl CompatibilityArtifactPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Summary => "summary",
            Self::Large => "large",
        }
    }
}

impl fmt::Display for CompatibilityArtifactPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CompatibilityArtifactPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "summary" => Ok(Self::Summary),
            "large" => Ok(Self::Large),
            _ => Err(format!("unknown compatibility artifact policy '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompatibilityOutcome {
    Passed,
    Failed,
    Skipped,
    Blocked,
}

impl CompatibilityOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "pass",
            Self::Failed => "fail",
            Self::Skipped => "skip",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompatibilitySkipReason {
    ToolchainUnavailable,
    LibraryUnavailable,
    FeatureUnsupported,
    LiveStackUnavailable,
    OptionalCase,
}

impl CompatibilitySkipReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ToolchainUnavailable => "toolchain-unavailable",
            Self::LibraryUnavailable => "library-unavailable",
            Self::FeatureUnsupported => "feature-unsupported",
            Self::LiveStackUnavailable => "live-stack-unavailable",
            Self::OptionalCase => "optional-case",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilityLibrary {
    name: Arc<str>,
    version: Option<Arc<str>>,
    required: bool,
    skip_reason: Option<Arc<str>>,
}

impl CompatibilityLibrary {
    pub fn new(
        name: impl Into<Arc<str>>,
        version: Option<impl Into<Arc<str>>>,
        required: bool,
        skip_reason: Option<impl Into<Arc<str>>>,
    ) -> Result<Self, String> {
        let name = name.into();
        let version = version.map(Into::into);
        let skip_reason = skip_reason.map(Into::into);
        if name.trim().is_empty() {
            return Err(String::from("compatibility library name cannot be empty"));
        }
        if version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(format!(
                "compatibility library '{}' version cannot be empty",
                name
            ));
        }
        if !required
            && skip_reason
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(format!(
                "compatibility library '{}' skip reason cannot be empty",
                name
            ));
        }
        Ok(Self {
            name,
            version,
            required,
            skip_reason,
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    #[must_use]
    pub const fn required(&self) -> bool {
        self.required
    }

    #[must_use]
    pub fn skip_reason(&self) -> Option<&str> {
        self.skip_reason.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilitySuiteSpec {
    pub id: Arc<str>,
    pub language: CompatibilityLanguage,
    pub library: CompatibilityLibrary,
    pub target: CompatibilityTarget,
    pub command: Arc<str>,
    pub timeout: Duration,
    pub required_services: Vec<Arc<str>>,
    pub artifact_policy: CompatibilityArtifactPolicy,
    pub artifact_path: Option<Arc<str>>,
    pub smoke: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilitySuite {
    id: Arc<str>,
    language: CompatibilityLanguage,
    library: CompatibilityLibrary,
    target: CompatibilityTarget,
    command: Arc<str>,
    timeout: Duration,
    required_services: Vec<Arc<str>>,
    artifact_policy: CompatibilityArtifactPolicy,
    artifact_path: Option<Arc<str>>,
    smoke: bool,
}

impl CompatibilitySuite {
    pub fn new(spec: CompatibilitySuiteSpec) -> Result<Self, String> {
        let CompatibilitySuiteSpec {
            id,
            language,
            library,
            target,
            command,
            timeout,
            required_services,
            artifact_policy,
            artifact_path,
            smoke,
        } = spec;

        validate_stable_id(&id)?;
        if command.trim().is_empty() {
            return Err(format!(
                "compatibility suite '{}' command cannot be empty",
                id
            ));
        }
        validate_public_path(&command)?;
        if timeout.is_zero() {
            return Err(format!(
                "compatibility suite '{}' timeout must be positive",
                id
            ));
        }
        if required_services
            .iter()
            .any(|service| service.trim().is_empty())
        {
            return Err(format!(
                "compatibility suite '{}' has an empty required service",
                id
            ));
        }
        if let Some(path) = artifact_path.as_deref() {
            validate_public_path(path)?;
        }

        Ok(Self {
            id,
            language,
            library,
            target,
            command,
            timeout,
            required_services,
            artifact_policy,
            artifact_path,
            smoke,
        })
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub const fn language(&self) -> CompatibilityLanguage {
        self.language
    }

    #[must_use]
    pub const fn target(&self) -> CompatibilityTarget {
        self.target
    }

    #[must_use]
    pub fn library(&self) -> &CompatibilityLibrary {
        &self.library
    }

    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    #[must_use]
    pub fn required_services(&self) -> &[Arc<str>] {
        &self.required_services
    }

    #[must_use]
    pub const fn artifact_policy(&self) -> CompatibilityArtifactPolicy {
        self.artifact_policy
    }

    #[must_use]
    pub fn artifact_path(&self) -> Option<&str> {
        self.artifact_path.as_deref()
    }

    #[must_use]
    pub const fn smoke(&self) -> bool {
        self.smoke
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilityCase {
    id: Arc<str>,
    category: Arc<str>,
    description: Arc<str>,
    sql_mode: Arc<str>,
    expected_outcome: Arc<str>,
    required_capability: Arc<str>,
}

impl CompatibilityCase {
    pub fn new(
        id: impl Into<Arc<str>>,
        category: impl Into<Arc<str>>,
        description: impl Into<Arc<str>>,
        sql_mode: impl Into<Arc<str>>,
        expected_outcome: impl Into<Arc<str>>,
        required_capability: impl Into<Arc<str>>,
    ) -> Result<Self, String> {
        let case = Self {
            id: id.into(),
            category: category.into(),
            description: description.into(),
            sql_mode: sql_mode.into(),
            expected_outcome: expected_outcome.into(),
            required_capability: required_capability.into(),
        };
        validate_stable_id(&case.id)?;
        for (name, value) in [
            ("category", &case.category),
            ("description", &case.description),
            ("sql mode", &case.sql_mode),
            ("expected outcome", &case.expected_outcome),
            ("required capability", &case.required_capability),
        ] {
            if value.trim().is_empty() {
                return Err(format!(
                    "compatibility case '{}' {name} cannot be empty",
                    case.id
                ));
            }
        }
        Ok(case)
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatibilityReport {
    pub language: CompatibilityLanguage,
    pub library: Arc<str>,
    pub version: Option<Arc<str>>,
    pub target: CompatibilityTarget,
    pub case_id: Arc<str>,
    pub outcome: CompatibilityOutcome,
    pub duration_ms: u128,
    pub skip_reason: Option<Arc<str>>,
    pub error_summary: Option<Arc<str>>,
}

fn validate_stable_id(value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(String::from("compatibility id cannot be empty"));
    }
    if value.len() > 96 {
        return Err(format!("compatibility id '{value}' is too long"));
    }
    if !value.chars().all(|character| {
        character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
    }) {
        return Err(format!(
            "compatibility id '{value}' must use lowercase letters, digits, and '-'"
        ));
    }
    validate_public_path(value)
}

pub fn validate_public_path(value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(String::from("compatibility path or label cannot be empty"));
    }
    if value.starts_with('~') {
        return Err(format!(
            "compatibility path or label '{value}' must be project-relative"
        ));
    }
    if value.contains('\0') || value.contains('\r') || value.contains('\n') {
        return Err(format!(
            "compatibility path or label '{value}' contains a control character"
        ));
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return Err(format!(
            "compatibility path or label '{value}' must be project-relative"
        ));
    }
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(format!(
                "compatibility path or label '{value}' must stay within the project"
            ));
        }
    }
    Ok(())
}
