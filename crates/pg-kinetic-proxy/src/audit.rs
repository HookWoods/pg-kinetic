use std::{
    collections::hash_map::DefaultHasher,
    fs::OpenOptions,
    hash::{Hash, Hasher},
    io::{self, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
        Arc,
    },
    thread,
    time::Duration,
};

use pg_kinetic_core::{protocol::sql_classify::SqlAnalysis, traffic::route::RouteKey};
use serde::Serialize;

use crate::config::AuditConfig;

const CHANNEL_CAPACITY: usize = 256;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct AuditRecord {
    pub route: String,
    pub identity: String,
    pub fingerprint: String,
    pub template: String,
    pub query_class: String,
    pub outcome: String,
    pub latency_ms: u64,
    pub rows: u64,
}

#[derive(Clone, Debug)]
pub struct AuditDispatcher {
    sender: SyncSender<AuditRecord>,
    dropped: Arc<AtomicU64>,
}

impl AuditDispatcher {
    pub fn from_config(config: &AuditConfig) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        let sink = config.sink.clone();
        Some(Self::new_with_writer(move || open_sink(sink.as_ref())))
    }

    fn new_with_writer<F>(open_writer: F) -> Self
    where
        F: FnOnce() -> io::Result<Box<dyn Write + Send>> + Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        thread::Builder::new()
            .name(String::from("pg-kinetic-audit"))
            .spawn(move || {
                let Ok(mut writer) = open_writer() else {
                    return;
                };
                while let Ok(record) = receiver.recv() {
                    let write_result = serde_json::to_writer(&mut writer, &record)
                        .map_err(io::Error::other)
                        .and_then(|()| writer.write_all(b"\n"))
                        .and_then(|()| writer.flush());
                    if write_result.is_err() {
                        break;
                    }
                }
            })
            .expect("audit worker thread must start");

        Self { sender, dropped }
    }

    pub fn record(&self, record: AuditRecord) {
        if let Err(TrySendError::Full(_)) = self.sender.try_send(record) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            crate::observe::metrics::record_audit_drop();
        }
    }

    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

pub fn record_query(
    dispatcher: &AuditDispatcher,
    config: &AuditConfig,
    route: &RouteKey,
    analysis: SqlAnalysis,
    fingerprint: &str,
    outcome: &str,
    latency: Duration,
    rows: u64,
) {
    let query_class = analysis.query_class();
    if !config.include_reads && query_class.routes_to_replica() {
        return;
    }
    if !sample(fingerprint, config.sample_rate()) {
        return;
    }
    let record = AuditRecord {
        route: opaque_id(&route.metric_label()),
        identity: opaque_id(&format!(
            "{}/{}/{}",
            route.database(),
            route.user(),
            route.application_name().unwrap_or("<none>")
        )),
        fingerprint: fingerprint.to_owned(),
        template: fingerprint.to_owned(),
        query_class: query_class.as_str().to_owned(),
        outcome: outcome.to_owned(),
        latency_ms: latency.as_millis().min(u64::MAX as u128) as u64,
        rows,
    };
    dispatcher.record(record);
    crate::observe::metrics::record_audit_record();
}

fn open_sink(path: Option<&PathBuf>) -> io::Result<Box<dyn Write + Send>> {
    match path {
        Some(path) => OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map(|file| Box::new(file) as Box<dyn Write + Send>),
        None => Ok(Box::new(io::stderr())),
    }
}

fn sample(value: &str, rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 || !rate.is_finite() {
        return false;
    }
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() as f64 / u64::MAX as f64) < rate
}

fn opaque_id(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("h{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn config() -> AuditConfig {
        AuditConfig {
            enabled: true,
            sink: None,
            sample_rate: 1.0,
            include_reads: true,
        }
    }

    #[test]
    fn record_contains_metadata_without_literals() {
        let (tx, rx) = mpsc::sync_channel(1);
        let dispatcher = AuditDispatcher {
            sender: tx,
            dropped: Arc::new(AtomicU64::new(0)),
        };
        let route = RouteKey::new(
            "db",
            "user",
            None,
            None,
            pg_kinetic_core::traffic::route::QueryClass::Write,
        );
        record_query(
            &dispatcher,
            &config(),
            &route,
            pg_kinetic_core::protocol::sql_classify::analyze_sql(
                "select * from users where id = 123",
            ),
            "select * from users where id = ?",
            "ok",
            Duration::from_millis(3),
            2,
        );
        let record = rx.recv().expect("record sent");
        assert_eq!(record.fingerprint, "select * from users where id = ?");
        assert!(!serde_json::to_string(&record).unwrap().contains("123"));
        assert!(!record.identity.contains("user"));
    }

    #[test]
    fn overflow_is_nonblocking_and_counted() {
        let (tx, _rx) = mpsc::sync_channel(1);
        let dispatcher = AuditDispatcher {
            sender: tx,
            dropped: Arc::new(AtomicU64::new(0)),
        };
        let record = AuditRecord {
            route: String::from("route"),
            identity: String::from("identity"),
            fingerprint: String::from("select ?"),
            template: String::from("select ?"),
            query_class: String::from("read"),
            outcome: String::from("ok"),
            latency_ms: 1,
            rows: 1,
        };
        dispatcher.record(record.clone());
        dispatcher.record(record);
        assert_eq!(dispatcher.dropped(), 1);
    }

    #[test]
    fn sink_failure_does_not_escape_worker() {
        let dispatcher = AuditDispatcher::new_with_writer(|| {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "test sink"))
        });
        dispatcher.record(AuditRecord {
            route: String::from("route"),
            identity: String::from("identity"),
            fingerprint: String::from("select ?"),
            template: String::from("select ?"),
            query_class: String::from("write"),
            outcome: String::from("ok"),
            latency_ms: 1,
            rows: 0,
        });
    }

    #[test]
    fn disabled_by_default() {
        assert!(!AuditConfig::default().enabled);
        assert!(AuditDispatcher::from_config(&AuditConfig::default()).is_none());
    }
}
