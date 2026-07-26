use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use pg_kinetic_core::{
    fingerprint::fingerprint_sql,
    protocol::sql_classify::{is_ddl, is_unqualified_dml, SqlAnalysis},
    traffic::routing::QueryClass,
};

const MAX_ALLOWLIST_ENTRIES: usize = 256;
const MAX_FINGERPRINT_BYTES: usize = 256;
const MAX_OBSERVATIONS: usize = 256;
const MAX_ALLOWLIST_BYTES: u64 = 128 * 1024;

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    serde::Deserialize,
    serde::Serialize,
    clap::ValueEnum,
)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum GuardrailMode {
    #[default]
    Off,
    Observe,
    Enforce,
}

impl GuardrailMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Enforce => "enforce",
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, clap::Args, serde::Serialize)]
#[serde(default)]
pub struct GuardrailsConfig {
    #[arg(long = "guardrails-mode", env = "PG_KINETIC_GUARDRAILS_MODE", value_enum, default_value_t = GuardrailMode::Off)]
    pub mode: GuardrailMode,
    #[arg(
        long = "guardrails-block-unqualified-dml",
        env = "PG_KINETIC_GUARDRAILS_BLOCK_UNQUALIFIED_DML",
        default_value_t = false
    )]
    pub block_unqualified_dml: bool,
    #[arg(
        long = "guardrails-block-ddl",
        env = "PG_KINETIC_GUARDRAILS_BLOCK_DDL",
        default_value_t = false
    )]
    pub block_ddl: bool,
    #[arg(
        long = "guardrails-allowlist-file",
        env = "PG_KINETIC_GUARDRAILS_ALLOWLIST_FILE"
    )]
    pub allowlist_file: Option<PathBuf>,
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            mode: GuardrailMode::Off,
            block_unqualified_dml: false,
            block_ddl: false,
            allowlist_file: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GuardrailRule {
    UnqualifiedDml,
    Ddl,
    UnknownFingerprint,
}

impl GuardrailRule {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnqualifiedDml => "unqualified_dml",
            Self::Ddl => "ddl",
            Self::UnknownFingerprint => "unknown_fingerprint",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardrailDecision {
    Allow,
    Deny(GuardrailRule),
}

#[derive(Clone, Debug)]
pub struct GuardrailRegistry {
    config: GuardrailsConfig,
    allowlist: Arc<RwLock<HashSet<String>>>,
    observations: Arc<RwLock<HashMap<String, u64>>>,
    allowlist_file: Option<PathBuf>,
    loaded_mtime: Arc<RwLock<Option<SystemTime>>>,
}

impl GuardrailRegistry {
    pub fn from_config(config: &GuardrailsConfig) -> anyhow::Result<Self> {
        let registry = Self {
            config: config.clone(),
            allowlist: Arc::new(RwLock::new(HashSet::new())),
            observations: Arc::new(RwLock::new(HashMap::new())),
            allowlist_file: config.allowlist_file.clone(),
            loaded_mtime: Arc::new(RwLock::new(None)),
        };
        registry.reload_allowlist()?;
        Ok(registry)
    }

    pub fn evaluate(&self, analysis: SqlAnalysis, sql: &str) -> GuardrailDecision {
        if matches!(self.config.mode, GuardrailMode::Off) {
            return GuardrailDecision::Allow;
        }
        self.refresh_allowlist_if_changed();
        let fingerprint = fingerprint_sql(sql);
        if !fingerprint.is_empty() {
            let mut observations = self
                .observations
                .write()
                .expect("guardrail observations poisoned");
            if observations.len() >= MAX_OBSERVATIONS && !observations.contains_key(&fingerprint) {
                if let Some(oldest) = observations.keys().next().cloned() {
                    observations.remove(&oldest);
                }
            }
            *observations.entry(fingerprint.clone()).or_default() += 1;
        }
        if self.config.block_unqualified_dml
            && analysis.query_class() == QueryClass::Write
            && is_unqualified_dml(sql)
            && matches!(self.config.mode, GuardrailMode::Enforce)
        {
            return GuardrailDecision::Deny(GuardrailRule::UnqualifiedDml);
        }
        if self.config.block_ddl
            && is_ddl(sql)
            && matches!(self.config.mode, GuardrailMode::Enforce)
        {
            return GuardrailDecision::Deny(GuardrailRule::Ddl);
        }
        if matches!(self.config.mode, GuardrailMode::Enforce)
            && (fingerprint.is_empty()
                || !self
                    .allowlist
                    .read()
                    .expect("guardrail allowlist poisoned")
                    .contains(&fingerprint))
        {
            return GuardrailDecision::Deny(GuardrailRule::UnknownFingerprint);
        }
        GuardrailDecision::Allow
    }

    pub fn reload_allowlist(&self) -> anyhow::Result<()> {
        let Some(path) = self.allowlist_file.as_deref() else {
            return Ok(());
        };
        let (entries, mtime) = load_allowlist(path)?;
        *self
            .allowlist
            .write()
            .expect("guardrail allowlist poisoned") = entries;
        *self.loaded_mtime.write().expect("guardrail mtime poisoned") = Some(mtime);
        Ok(())
    }

    pub fn allowlist_len(&self) -> usize {
        self.allowlist
            .read()
            .expect("guardrail allowlist poisoned")
            .len()
    }
    pub fn observation_len(&self) -> usize {
        self.observations
            .read()
            .expect("guardrail observations poisoned")
            .len()
    }

    fn refresh_allowlist_if_changed(&self) {
        let Some(path) = self.allowlist_file.as_deref() else {
            return;
        };
        let Ok(mtime) = fs::metadata(path).and_then(|metadata| metadata.modified()) else {
            return;
        };
        let loaded = *self.loaded_mtime.read().expect("guardrail mtime poisoned");
        if loaded != Some(mtime) {
            let _ = self.reload_allowlist();
        }
    }
}

fn load_allowlist(path: &Path) -> anyhow::Result<(HashSet<String>, SystemTime)> {
    if fs::metadata(path)?.len() > MAX_ALLOWLIST_BYTES {
        anyhow::bail!("guardrail allowlist exceeds the configured size limit");
    }
    let content = fs::read_to_string(path)?;
    let mtime = fs::metadata(path)?.modified()?;
    let mut entries = HashSet::new();
    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        if entries.len() >= MAX_ALLOWLIST_ENTRIES {
            anyhow::bail!(
                "guardrail allowlist exceeds {} entries",
                MAX_ALLOWLIST_ENTRIES
            );
        }
        if line.len() > MAX_FINGERPRINT_BYTES || line.chars().any(char::is_control) {
            anyhow::bail!("guardrail allowlist contains an invalid fingerprint");
        }
        entries.insert(line.to_owned());
    }
    Ok((entries, mtime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn config(mode: GuardrailMode) -> GuardrailsConfig {
        GuardrailsConfig {
            mode,
            block_unqualified_dml: true,
            block_ddl: true,
            allowlist_file: None,
        }
    }

    #[test]
    fn hard_rules_are_conservative() {
        let registry = GuardrailRegistry::from_config(&config(GuardrailMode::Enforce)).unwrap();
        assert_eq!(
            registry.evaluate(
                pg_kinetic_core::protocol::sql_classify::analyze_sql("DELETE FROM t"),
                "DELETE FROM t"
            ),
            GuardrailDecision::Deny(GuardrailRule::UnqualifiedDml)
        );
        assert_eq!(
            GuardrailRegistry::from_config(&config(GuardrailMode::Observe))
                .unwrap()
                .evaluate(
                    pg_kinetic_core::protocol::sql_classify::analyze_sql(
                        "DELETE FROM t WHERE id=1"
                    ),
                    "DELETE FROM t WHERE id=1"
                ),
            GuardrailDecision::Allow
        );
    }

    #[test]
    fn allowlist_reload_is_bounded_and_replaces_atomically() {
        let path = std::env::temp_dir().join(format!(
            "pg-kinetic-guardrails-{}.txt",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut file = fs::File::create(&path).unwrap();
        writeln!(file, "select ?").unwrap();
        let config = GuardrailsConfig {
            mode: GuardrailMode::Enforce,
            allowlist_file: Some(path.clone()),
            ..config(GuardrailMode::Enforce)
        };
        let registry = GuardrailRegistry::from_config(&config).unwrap();
        assert_eq!(registry.allowlist_len(), 1);
        assert_eq!(registry.observation_len(), 0);
        writeln!(file, "select 2").unwrap();
        registry.reload_allowlist().unwrap();
        assert_eq!(registry.allowlist_len(), 2);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn observe_allows_hard_rules_and_enforce_requires_known_fingerprints() {
        let observe = GuardrailRegistry::from_config(&config(GuardrailMode::Observe)).unwrap();
        assert_eq!(
            observe.evaluate(
                pg_kinetic_core::protocol::sql_classify::analyze_sql("DROP TABLE t"),
                "DROP TABLE t"
            ),
            GuardrailDecision::Allow
        );
        let enforce = GuardrailRegistry::from_config(&config(GuardrailMode::Enforce)).unwrap();
        assert_eq!(
            enforce.evaluate(
                pg_kinetic_core::protocol::sql_classify::analyze_sql("SELECT 1"),
                "SELECT 1"
            ),
            GuardrailDecision::Deny(GuardrailRule::UnknownFingerprint)
        );
    }
}
