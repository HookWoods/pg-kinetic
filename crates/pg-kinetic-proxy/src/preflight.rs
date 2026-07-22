use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::{
    auth,
    config::{AuthMode, Config, MirrorConfig, PolicyConfig, ShardingConfig, TlsConfig},
    runtime_engine::{RuntimeEngineExperiment, RuntimeEngineSelector},
    tls,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreflightRunner {
    config_path: PathBuf,
}

impl PreflightRunner {
    #[must_use]
    pub fn new(config_path: impl AsRef<Path>) -> Self {
        Self {
            config_path: config_path.as_ref().to_path_buf(),
        }
    }

    #[must_use]
    pub fn run(&self) -> PreflightReport {
        let mut report = PreflightReport::new(self.config_path.clone());
        let contents = match fs::read_to_string(&self.config_path) {
            Ok(contents) => contents,
            Err(error) => {
                report.add_error(
                    PreflightCheck::ConfigLoad,
                    format!("read {}: {error}", self.config_path.display()),
                );
                return report;
            }
        };

        let config = match toml::from_str::<Config>(&contents) {
            Ok(config) => config,
            Err(error) => {
                report.add_error(
                    PreflightCheck::ConfigLoad,
                    format!("parse {}: {error}", self.config_path.display()),
                );
                return report;
            }
        };

        self.validate_runtime_engine(&config, &mut report);
        self.validate_tls(&config.tls, &mut report);
        self.validate_auth(&config, &mut report);
        self.validate_route_maps(&contents, &mut report);
        self.validate_policies(&contents, &mut report);
        self.validate_mirror(&contents, config.connection.backend_addr, &mut report);
        self.validate_lifecycle(&config, &mut report);
        self.validate_adaptive(&config, &mut report);

        report
    }

    fn validate_runtime_engine(&self, config: &Config, report: &mut PreflightReport) {
        let selector = RuntimeEngineSelector::new(config.runtime.engine.runtime_engine)
            .with_experiment(RuntimeEngineExperiment::new(
                config.runtime.engine.experimental_runtime_enabled,
            ));

        if let Err(error) = selector.validate() {
            report.add_error(PreflightCheck::RuntimeEngine, error.to_string());
        }

        if config.runtime.engine.runtime_engine
            == pg_kinetic_core::runtime::RuntimeEngine::ExperimentalIoUring
            && !cfg!(feature = "io-uring")
        {
            report.add_error(
                PreflightCheck::RuntimeEngine,
                "runtime engine 'experimental_io_uring' requires the io-uring cargo feature",
            );
        }
    }

    fn validate_tls(&self, tls_config: &TlsConfig, report: &mut PreflightReport) {
        let mut client_tls_files_configured = false;

        if let Some(path) = tls_config.client_cert_path.as_deref() {
            client_tls_files_configured = true;
            if let Err(error) = read_configured_file(path) {
                report.add_error(
                    PreflightCheck::TlsFiles,
                    format!("read client TLS certificate {}: {error}", path.display()),
                );
            }
        }

        if let Some(path) = tls_config.client_key_path.as_deref() {
            client_tls_files_configured = true;
            if let Err(error) = read_configured_file(path) {
                report.add_error(
                    PreflightCheck::TlsFiles,
                    format!("read client TLS private key {}: {error}", path.display()),
                );
            }
        }

        if let Some(path) = tls_config.client_ca_path.as_deref() {
            client_tls_files_configured = true;
            if let Err(error) = read_configured_file(path) {
                report.add_error(
                    PreflightCheck::TlsFiles,
                    format!("read client TLS CA {}: {error}", path.display()),
                );
            }
        }

        if client_tls_files_configured
            || tls_config.client_tls_mode != crate::config::ClientTlsMode::Disable
        {
            if let Err(error) = tls::load_server_config(tls_config) {
                report.add_error(PreflightCheck::TlsFiles, error.to_string());
            }
        }

        if let Some(path) = tls_config.backend_ca_path.as_deref() {
            if let Err(error) = read_configured_file(path) {
                report.add_error(
                    PreflightCheck::TlsFiles,
                    format!("read backend TLS CA {}: {error}", path.display()),
                );
            }
        }

        if tls_config.backend_ca_path.is_some()
            && tls_config.backend_tls_mode == crate::config::BackendTlsMode::Disable
        {
            if let Err(error) = tls::load_backend_client_config(tls_config) {
                report.add_error(PreflightCheck::TlsFiles, error.to_string());
            }
        }

        if tls_config.backend_tls_mode != crate::config::BackendTlsMode::Disable {
            if let Err(error) = tls::backend_tls_settings(tls_config) {
                report.add_error(PreflightCheck::TlsFiles, error.to_string());
            }
        }

        if tls_config.client_tls_mode == crate::config::ClientTlsMode::Disable {
            report.add_warning(PreflightCheck::TlsFiles, "client TLS is disabled");
        }

        if tls_config.backend_tls_mode == crate::config::BackendTlsMode::Disable {
            report.add_warning(PreflightCheck::TlsFiles, "backend TLS is disabled");
        }
    }

    fn validate_auth(&self, config: &Config, report: &mut PreflightReport) {
        if let Some(path) = config.auth.auth_users_file.as_deref() {
            if let Err(error) = auth::load_user_store(Some(path)) {
                report.add_error(PreflightCheck::AuthUsers, error.to_string());
            }
        }

        if let Err(error) = auth::load_backend_credentials(&config.auth) {
            report.add_error(PreflightCheck::AuthUsers, error.to_string());
        }

        if config.auth.auth_mode == AuthMode::PassThrough {
            report.add_warning(
                PreflightCheck::AuthUsers,
                "authentication mode is pass_through",
            );
        }
    }

    fn validate_route_maps(&self, contents: &str, report: &mut PreflightReport) {
        match toml::from_str::<OptionalShardingDocument>(contents) {
            Ok(document) => {
                if let Some(sharding) = document.sharding {
                    if sharding.sharding_enabled && sharding.route_maps.is_empty() {
                        report.add_error(
                            PreflightCheck::RouteMaps,
                            "sharding is enabled but no route maps are configured",
                        );
                    }
                }
            }
            Err(error) => {
                report.add_error(PreflightCheck::RouteMaps, error.to_string());
            }
        }
    }

    fn validate_policies(&self, contents: &str, report: &mut PreflightReport) {
        match toml::from_str::<OptionalPolicyDocument>(contents) {
            Ok(document) => {
                if let Some(policy) = document.policy {
                    if let Err(error) = policy.validate() {
                        report.add_error(PreflightCheck::Policies, error);
                    }

                    for file in &policy.policy_files {
                        if let Err(error) = read_configured_file(file.path.as_path()) {
                            report.add_error(
                                PreflightCheck::Policies,
                                format!("read policy file {}: {error}", file.path.display()),
                            );
                        }
                    }
                }
            }
            Err(error) => {
                report.add_error(PreflightCheck::Policies, error.to_string());
            }
        }
    }

    fn validate_mirror(
        &self,
        contents: &str,
        production_target: std::net::SocketAddr,
        report: &mut PreflightReport,
    ) {
        match toml::from_str::<OptionalMirrorDocument>(contents) {
            Ok(document) => {
                if let Some(mirror) = document.mirror {
                    if let Err(error) = mirror.validate(production_target) {
                        report.add_error(PreflightCheck::MirrorIsolation, error);
                    }

                    if !mirror.is_enabled() {
                        report
                            .add_warning(PreflightCheck::MirrorIsolation, "mirroring is disabled");
                    }
                }
            }
            Err(error) => {
                report.add_error(PreflightCheck::MirrorIsolation, error.to_string());
            }
        }
    }

    fn validate_lifecycle(&self, config: &Config, report: &mut PreflightReport) {
        let lifecycle = &config.runtime.lifecycle;
        let startup_grace = lifecycle.startup_grace_ms;
        let shutdown_grace = lifecycle.shutdown_grace_ms;
        let termination_grace_ms = lifecycle
            .termination_grace_period_seconds
            .saturating_mul(1_000);

        if startup_grace == 0 {
            report.add_error(
                PreflightCheck::LifecycleGuardrails,
                "startup grace must be greater than zero",
            );
        }

        if shutdown_grace == 0 {
            report.add_error(
                PreflightCheck::LifecycleGuardrails,
                "shutdown grace must be greater than zero",
            );
        }

        if lifecycle.termination_grace_period_seconds == 0 {
            report.add_error(
                PreflightCheck::LifecycleGuardrails,
                "termination grace period must be greater than zero",
            );
        }

        if shutdown_grace > termination_grace_ms {
            report.add_error(
                PreflightCheck::LifecycleGuardrails,
                "shutdown grace must not exceed the termination grace period",
            );
        }

        if lifecycle.pre_stop_drain_enabled && lifecycle.pre_stop_drain_endpoint.trim().is_empty() {
            report.add_error(
                PreflightCheck::LifecycleGuardrails,
                "pre-stop drain endpoint must not be empty when pre-stop drain is enabled",
            );
        }
    }

    fn validate_adaptive(&self, config: &Config, report: &mut PreflightReport) {
        if let Err(error) = config.runtime.production.adaptive.validate() {
            report.add_error(PreflightCheck::AdaptiveGuardrails, error);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreflightCheck {
    ConfigLoad,
    RuntimeEngine,
    TlsFiles,
    AuthUsers,
    RouteMaps,
    Policies,
    MirrorIsolation,
    LifecycleGuardrails,
    AdaptiveGuardrails,
}

impl PreflightCheck {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfigLoad => "config_load",
            Self::RuntimeEngine => "runtime_engine",
            Self::TlsFiles => "tls_files",
            Self::AuthUsers => "auth_users",
            Self::RouteMaps => "route_maps",
            Self::Policies => "policies",
            Self::MirrorIsolation => "mirror_isolation",
            Self::LifecycleGuardrails => "lifecycle_guardrails",
            Self::AdaptiveGuardrails => "adaptive_guardrails",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreflightSeverity {
    Warning,
    Error,
}

impl PreflightSeverity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreflightFinding {
    pub check: PreflightCheck,
    pub severity: PreflightSeverity,
    pub message: String,
}

impl PreflightFinding {
    #[must_use]
    pub fn new(
        check: PreflightCheck,
        severity: PreflightSeverity,
        message: impl Into<String>,
    ) -> Self {
        Self {
            check,
            severity,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreflightReport {
    pub config_path: PathBuf,
    findings: Vec<PreflightFinding>,
}

impl PreflightReport {
    #[must_use]
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            findings: Vec::new(),
        }
    }

    pub fn add_warning(&mut self, check: PreflightCheck, message: impl Into<String>) {
        self.findings.push(PreflightFinding::new(
            check,
            PreflightSeverity::Warning,
            message,
        ));
    }

    pub fn add_error(&mut self, check: PreflightCheck, message: impl Into<String>) {
        self.findings.push(PreflightFinding::new(
            check,
            PreflightSeverity::Error,
            message,
        ));
    }

    #[must_use]
    pub fn warnings(&self) -> Vec<&PreflightFinding> {
        self.findings
            .iter()
            .filter(|finding| finding.severity == PreflightSeverity::Warning)
            .collect()
    }

    #[must_use]
    pub fn errors(&self) -> Vec<&PreflightFinding> {
        self.findings
            .iter()
            .filter(|finding| finding.severity == PreflightSeverity::Error)
            .collect()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.severity == PreflightSeverity::Error)
    }

    #[must_use]
    pub fn render_json(&self) -> String {
        let warnings = self
            .warnings()
            .into_iter()
            .map(render_finding)
            .collect::<Vec<_>>()
            .join(",");
        let errors = self
            .errors()
            .into_iter()
            .map(render_finding)
            .collect::<Vec<_>>()
            .join(",");

        format!(
            "{{\"ok\":{},\"config\":{},\"warning_count\":{},\"error_count\":{},\"warnings\":[{}],\"errors\":[{}]}}",
            !self.has_errors(),
            json_string(self.config_path.to_string_lossy().as_ref()),
            self.warnings().len(),
            self.errors().len(),
            warnings,
            errors
        )
    }
}

#[derive(Debug, Deserialize)]
struct OptionalMirrorDocument {
    #[serde(default)]
    mirror: Option<MirrorConfig>,
}

#[derive(Debug, Deserialize)]
struct OptionalPolicyDocument {
    #[serde(default)]
    policy: Option<PolicyConfig>,
}

#[derive(Debug, Deserialize)]
struct OptionalShardingDocument {
    #[serde(default)]
    sharding: Option<ShardingConfig>,
}

fn read_configured_file(path: &Path) -> std::io::Result<Vec<u8>> {
    fs::read(path)
}

fn render_finding(finding: &PreflightFinding) -> String {
    format!(
        "{{\"check\":{},\"severity\":{},\"message\":{}}}",
        json_string(finding.check.as_str()),
        json_string(finding.severity.as_str()),
        json_string(&finding.message)
    )
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0C}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}
