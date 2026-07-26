use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

const SHARD_COUNT: usize = 8;
const SHARD_CAPACITY: usize = 64;
pub const TOP_QUERY_LIMIT: usize = 32;
const SAMPLE_LIMIT: usize = 256;

#[derive(Clone, Debug)]
pub struct QueryStats {
    shards: Arc<[Mutex<HashMap<String, QueryStat>>; SHARD_COUNT]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryStatView {
    pub fingerprint: String,
    pub sample: String,
    pub count: u64,
    pub errors: u64,
    pub rows: u64,
    pub total_latency_ms: f64,
}

#[derive(Clone, Debug)]
struct QueryStat {
    sample: String,
    count: u64,
    errors: u64,
    rows: u64,
    total_latency_ms: f64,
}

impl Default for QueryStats {
    fn default() -> Self {
        Self {
            shards: Arc::new(std::array::from_fn(|_| Mutex::new(HashMap::new()))),
        }
    }
}

impl QueryStats {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(
        &self,
        fingerprint: &str,
        sample: &str,
        latency: Duration,
        rows: u64,
        error: bool,
    ) {
        let shard_index = shard_index(fingerprint);
        let mut shard = self.shards[shard_index]
            .lock()
            .expect("query stats shard poisoned");
        let entry = shard
            .entry(fingerprint.to_owned())
            .or_insert_with(|| QueryStat {
                sample: truncate(sample),
                count: 0,
                errors: 0,
                rows: 0,
                total_latency_ms: 0.0,
            });
        entry.count = entry.count.saturating_add(1);
        entry.errors = entry.errors.saturating_add(u64::from(error));
        entry.rows = entry.rows.saturating_add(rows);
        entry.total_latency_ms += latency.as_secs_f64() * 1_000.0;
        if shard.len() > SHARD_CAPACITY {
            if let Some(key) = shard
                .iter()
                .min_by_key(|(_, stat)| stat.count)
                .map(|(key, _)| key.clone())
            {
                shard.remove(&key);
            }
        }
    }

    #[must_use]
    pub fn top(&self) -> Vec<QueryStatView> {
        let mut all = self
            .shards
            .iter()
            .flat_map(|shard| {
                shard
                    .lock()
                    .expect("query stats shard poisoned")
                    .iter()
                    .map(|(fingerprint, stat)| QueryStatView {
                        fingerprint: fingerprint.clone(),
                        sample: stat.sample.clone(),
                        count: stat.count,
                        errors: stat.errors,
                        rows: stat.rows,
                        total_latency_ms: stat.total_latency_ms,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        all.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.fingerprint.cmp(&right.fingerprint))
        });
        all.truncate(TOP_QUERY_LIMIT);
        all
    }

    #[must_use]
    pub fn metric_bucket(&self, fingerprint: &str) -> String {
        if self
            .top()
            .iter()
            .any(|stat| stat.fingerprint == fingerprint)
        {
            format!("query_{:02}", shard_index(fingerprint) % TOP_QUERY_LIMIT)
        } else {
            String::from("other")
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().expect("query stats shard poisoned").len())
            .sum()
    }
}

fn shard_index(value: &str) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() as usize) % SHARD_COUNT
}

fn truncate(value: &str) -> String {
    value.chars().take(SAMPLE_LIMIT).collect()
}

#[cfg(test)]
mod tests {
    use super::QueryStats;
    use std::time::Duration;

    #[test]
    fn accounts_values_and_bounds_registry() {
        let stats = QueryStats::new();
        for index in 0..600 {
            stats.record(
                &format!("select {index}"),
                "select ?",
                Duration::from_millis(2),
                3,
                index % 2 == 0,
            );
        }
        assert!(stats.len() <= 8 * 64);
        let top = stats.top();
        assert!(top.len() <= 32);
        assert_eq!(top[0].sample, "select ?");
    }

    #[test]
    fn metric_buckets_have_an_other_fallback() {
        let stats = QueryStats::new();
        stats.record("select ?", "select ?", Duration::from_millis(1), 1, false);
        assert_ne!(stats.metric_bucket("select ?"), "other");
        assert_eq!(stats.metric_bucket("select other"), "other");
    }
}
