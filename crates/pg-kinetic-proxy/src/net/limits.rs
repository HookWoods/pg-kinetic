use std::{fs, path::Path};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ResourceLimits {
    pub cpu_quota: Option<f64>,
    pub mem_limit_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pressure {
    pub cpu_some_avg10: f64,
    pub mem_some_avg10: f64,
}

#[must_use]
pub fn detect_cgroup_limits() -> ResourceLimits {
    detect_cgroup_limits_at(Path::new("/sys/fs/cgroup"))
}

#[must_use]
pub fn detect_cgroup_limits_at(root: &Path) -> ResourceLimits {
    ResourceLimits {
        cpu_quota: fs::read_to_string(root.join("cpu.max"))
            .ok()
            .and_then(|contents| parse_cpu_max(&contents)),
        mem_limit_bytes: fs::read_to_string(root.join("memory.max"))
            .ok()
            .and_then(|contents| parse_memory_max(&contents)),
    }
}

#[must_use]
pub fn parse_cpu_max(contents: &str) -> Option<f64> {
    let mut fields = contents.split_whitespace();
    let quota = fields.next()?;
    let period = fields.next()?;
    if quota == "max" {
        return None;
    }

    let quota = quota.parse::<u64>().ok()?;
    let period = period.parse::<u64>().ok()?;
    if quota == 0 || period == 0 {
        return None;
    }

    Some(quota as f64 / period as f64)
}

#[must_use]
pub fn parse_memory_max(contents: &str) -> Option<u64> {
    let value = contents.split_whitespace().next()?;
    if value == "max" {
        return None;
    }
    value.parse::<u64>().ok().filter(|bytes| *bytes > 0)
}

#[must_use]
pub fn read_pressure() -> Option<Pressure> {
    read_pressure_at(Path::new("/proc/pressure"))
}

#[must_use]
pub fn read_pressure_at(root: &Path) -> Option<Pressure> {
    let cpu_some_avg10 = fs::read_to_string(root.join("cpu"))
        .ok()
        .and_then(|contents| parse_pressure_some_avg10(&contents))?;
    let mem_some_avg10 = fs::read_to_string(root.join("memory"))
        .ok()
        .and_then(|contents| parse_pressure_some_avg10(&contents))?;
    Some(Pressure {
        cpu_some_avg10,
        mem_some_avg10,
    })
}

#[must_use]
pub fn parse_pressure_some_avg10(contents: &str) -> Option<f64> {
    let some_line = contents.lines().find(|line| line.starts_with("some "))?;
    let avg10 = some_line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("avg10="))?
        .parse::<f64>()
        .ok()?;
    avg10.is_finite().then_some(avg10)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn parses_cgroup_v2_cpu_max_quota() {
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2.0));
        assert_eq!(parse_cpu_max("150000 100000\n"), Some(1.5));
        assert_eq!(parse_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cpu_max("0 100000\n"), None);
        assert_eq!(parse_cpu_max("100000 0\n"), None);
        assert_eq!(parse_cpu_max("not-a-number 100000\n"), None);
    }

    #[test]
    fn parses_cgroup_v2_memory_max_limit() {
        assert_eq!(parse_memory_max("536870912\n"), Some(536_870_912));
        assert_eq!(parse_memory_max("max\n"), None);
        assert_eq!(parse_memory_max("0\n"), None);
        assert_eq!(parse_memory_max("bad\n"), None);
    }

    #[test]
    fn detects_limits_from_fixture_root() {
        let root = std::env::temp_dir().join(format!(
            "pg-kinetic-cgroup-fixture-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        fs::write(root.join("cpu.max"), "200000 100000\n").expect("write cpu.max");
        fs::write(root.join("memory.max"), "536870912\n").expect("write memory.max");

        let limits = detect_cgroup_limits_at(&root);

        assert_eq!(limits.cpu_quota, Some(2.0));
        assert_eq!(limits.mem_limit_bytes, Some(536_870_912));

        fs::remove_dir_all(root).expect("remove fixture root");
    }

    #[test]
    fn parses_pressure_avg10() {
        assert_eq!(
            parse_pressure_some_avg10("some avg10=12.34 avg60=1.00 avg300=0.50 total=42\n"),
            Some(12.34)
        );
        assert_eq!(
            parse_pressure_some_avg10(
                "full avg10=1.00 avg60=1.00 avg300=1.00 total=42\nsome avg10=0.25 avg60=0.00 avg300=0.00 total=7\n"
            ),
            Some(0.25)
        );
        assert_eq!(parse_pressure_some_avg10("full avg10=1.0\n"), None);
        assert_eq!(parse_pressure_some_avg10("some avg60=1.0\n"), None);
        assert_eq!(parse_pressure_some_avg10("some avg10=bad\n"), None);
    }

    #[test]
    fn reads_pressure_from_fixture_root() {
        let root = std::env::temp_dir().join(format!(
            "pg-kinetic-pressure-fixture-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        fs::write(
            root.join("cpu"),
            "some avg10=3.50 avg60=1.00 avg300=0.50 total=42\n",
        )
        .expect("write cpu pressure");
        fs::write(
            root.join("memory"),
            "some avg10=7.25 avg60=1.00 avg300=0.50 total=42\n",
        )
        .expect("write memory pressure");

        let pressure = read_pressure_at(&root).expect("pressure");

        assert_eq!(
            pressure,
            Pressure {
                cpu_some_avg10: 3.5,
                mem_some_avg10: 7.25
            }
        );

        fs::remove_dir_all(root).expect("remove fixture root");
    }
}
