//! Process-level resource probes for the running proxy.
//!
//! Lives in core because both the proxy runtime (admin snapshots) and the
//! offline benchmark lab need it; it is a runtime probe, not a benchmark
//! concept, and previously sat in the lab module purely by accident.

use std::fs;

use crate::observability::performance::{
    ProcessMetricCollectionStatus, ProcessMetricKind, ProcessMetricSample, ProcessMetricValue,
};

#[derive(Clone, Debug, PartialEq)]
pub struct ProcessMetricCollection {
    status: ProcessMetricCollectionStatus,
    sample: ProcessMetricSample,
}

impl ProcessMetricCollection {
    #[must_use]
    pub const fn status(&self) -> ProcessMetricCollectionStatus {
        self.status
    }

    #[must_use]
    pub const fn sample(&self) -> &ProcessMetricSample {
        &self.sample
    }
}

#[must_use]
pub fn collect_process_metrics() -> ProcessMetricCollection {
    let metrics = [
        (ProcessMetricKind::CpuTime, collect_cpu_time()),
        (ProcessMetricKind::ResidentMemory, collect_resident_memory()),
        (
            ProcessMetricKind::OpenFileDescriptors,
            collect_open_file_descriptors(),
        ),
    ];
    let unknown = metrics
        .iter()
        .filter(|(_, value)| value.is_unknown())
        .count();
    let status = if unknown == 0 {
        ProcessMetricCollectionStatus::Complete
    } else if unknown == metrics.len() {
        ProcessMetricCollectionStatus::Unavailable
    } else {
        ProcessMetricCollectionStatus::Partial
    };
    ProcessMetricCollection {
        status,
        sample: ProcessMetricSample::now(metrics).redacted(),
    }
}

#[cfg(unix)]
fn collect_cpu_time() -> ProcessMetricValue {
    let clock_ticks_per_second = rustix::param::clock_ticks_per_second();
    fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|contents| proc_stat_cpu_ticks(&contents))
        .and_then(|clock_ticks| cpu_time_seconds(clock_ticks, clock_ticks_per_second))
        .map_or(ProcessMetricValue::Unknown, ProcessMetricValue::Float)
}

#[cfg(not(unix))]
fn collect_cpu_time() -> ProcessMetricValue {
    ProcessMetricValue::Unknown
}

#[cfg(unix)]
fn collect_resident_memory() -> ProcessMetricValue {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
            })
        })
        .and_then(resident_memory_bytes)
        .map_or(ProcessMetricValue::Unknown, ProcessMetricValue::Integer)
}

#[cfg(not(unix))]
fn collect_resident_memory() -> ProcessMetricValue {
    ProcessMetricValue::Unknown
}

#[cfg(unix)]
fn collect_open_file_descriptors() -> ProcessMetricValue {
    fs::read_dir("/proc/self/fd")
        .ok()
        .and_then(|entries| entries.count().try_into().ok())
        .map_or(ProcessMetricValue::Unknown, ProcessMetricValue::Integer)
}

#[cfg_attr(not(unix), allow(dead_code))]
fn cpu_time_seconds(clock_ticks: u64, clock_ticks_per_second: u64) -> Option<f64> {
    (clock_ticks_per_second > 0).then(|| clock_ticks as f64 / clock_ticks_per_second as f64)
}

#[cfg_attr(not(unix), allow(dead_code))]
fn resident_memory_bytes(kibibytes: u64) -> Option<u64> {
    kibibytes.checked_mul(1024)
}

#[cfg_attr(not(unix), allow(dead_code))]
fn proc_stat_cpu_ticks(contents: &str) -> Option<u64> {
    let (_, fields) = contents.rsplit_once(") ")?;
    let mut fields = fields.split_whitespace();
    let utime = fields.nth(11)?.parse::<u64>().ok()?;
    let stime = fields.next()?.parse::<u64>().ok()?;
    utime.checked_add(stime)
}

#[cfg(not(unix))]
fn collect_open_file_descriptors() -> ProcessMetricValue {
    ProcessMetricValue::Unknown
}

#[cfg(test)]
mod tests {
    use super::{cpu_time_seconds, proc_stat_cpu_ticks, resident_memory_bytes};

    #[test]
    fn proc_stat_cpu_ticks_include_user_and_system_time() {
        let stat = "123 (pg kinetic) S 1 2 3 4 5 6 7 8 9 10 200 50 0 0";

        assert_eq!(proc_stat_cpu_ticks(stat), Some(250));
    }

    #[test]
    fn cpu_clock_ticks_convert_to_seconds() {
        assert_eq!(cpu_time_seconds(250, 100), Some(2.5));
        assert_eq!(cpu_time_seconds(1, 0), None);
    }

    #[test]
    fn resident_memory_kibibytes_convert_to_bytes() {
        assert_eq!(resident_memory_bytes(4_096), Some(4_194_304));
        assert_eq!(resident_memory_bytes(u64::MAX), None);
    }
}
