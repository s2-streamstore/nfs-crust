use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::{Distribution, HostMetadata, ResourceDelta};

pub const P99_MINIMUM_SAMPLES: u64 = 1_000;
pub const P999_MINIMUM_SAMPLES: u64 = 10_000;

#[derive(Debug, Clone, Copy)]
pub struct ResourceSnapshot {
    user_cpu_micros: u64,
    system_cpu_micros: u64,
    max_rss_kib: u64,
    host_rx_bytes: Option<u64>,
    host_tx_bytes: Option<u64>,
}

impl ResourceSnapshot {
    pub fn capture() -> Self {
        let (user_cpu_micros, system_cpu_micros, max_rss_kib) = process_usage();
        let (host_rx_bytes, host_tx_bytes) = network_totals();
        Self {
            user_cpu_micros,
            system_cpu_micros,
            max_rss_kib,
            host_rx_bytes,
            host_tx_bytes,
        }
    }

    pub fn delta(self, end: Self) -> ResourceDelta {
        ResourceDelta {
            user_cpu_ms: end.user_cpu_micros.saturating_sub(self.user_cpu_micros) as f64 / 1_000.0,
            system_cpu_ms: end.system_cpu_micros.saturating_sub(self.system_cpu_micros) as f64
                / 1_000.0,
            process_max_rss_kib: end.max_rss_kib,
            host_rx_bytes: subtract_options(end.host_rx_bytes, self.host_rx_bytes),
            host_tx_bytes: subtract_options(end.host_tx_bytes, self.host_tx_bytes),
        }
    }
}

pub fn distribution(values_ns: impl IntoIterator<Item = u64>) -> Distribution {
    let mut values: Vec<u64> = values_ns.into_iter().collect();
    if values.is_empty() {
        return Distribution::default();
    }
    values.sort_unstable();
    let count = values.len() as u64;
    let sum = values.iter().map(|&value| value as f64).sum::<f64>();
    let mean_ns = sum / count as f64;
    let variance_ns = values
        .iter()
        .map(|&value| {
            let delta = value as f64 - mean_ns;
            delta * delta
        })
        .sum::<f64>()
        / count as f64;

    Distribution {
        count,
        min_us: ns_to_us(values[0]),
        mean_us: mean_ns / 1_000.0,
        p50_us: ns_to_us(nearest_rank(&values, 0.50)),
        p90_us: ns_to_us(nearest_rank(&values, 0.90)),
        p95_us: ns_to_us(nearest_rank(&values, 0.95)),
        p99_us: ns_to_us(nearest_rank(&values, 0.99)),
        p999_us: ns_to_us(nearest_rank(&values, 0.999)),
        max_us: ns_to_us(*values.last().expect("non-empty values")),
        stddev_us: variance_ns.sqrt() / 1_000.0,
    }
}

fn nearest_rank(values: &[u64], percentile: f64) -> u64 {
    let rank = (percentile * values.len() as f64).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn ns_to_us(value: u64) -> f64 {
    value as f64 / 1_000.0
}

pub fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub fn host_metadata() -> HostMetadata {
    HostMetadata {
        hostname: read_trimmed("/etc/hostname").unwrap_or_else(|| command_output("hostname", &[])),
        os_release: read_trimmed("/etc/os-release").unwrap_or_default(),
        kernel: command_output("uname", &["-srv"]),
        architecture: command_output("uname", &["-m"]),
        rustc: command_output("rustc", &["-Vv"]),
    }
}

fn process_usage() -> (u64, u64, u64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the provided rusage on success. The zeroed
    // fallback is valid for the integer/timeval fields we read on failure.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return (0, 0, 0);
    }
    // SAFETY: a successful getrusage initialized the value.
    let usage = unsafe { usage.assume_init() };
    (
        timeval_micros(usage.ru_utime),
        timeval_micros(usage.ru_stime),
        usage.ru_maxrss.max(0) as u64,
    )
}

fn timeval_micros(value: libc::timeval) -> u64 {
    let seconds = value.tv_sec.max(0) as u64;
    let micros = value.tv_usec.max(0) as u64;
    seconds.saturating_mul(1_000_000).saturating_add(micros)
}

fn network_totals() -> (Option<u64>, Option<u64>) {
    let Ok(entries) = fs::read_dir("/sys/class/net") else {
        return (None, None);
    };
    let mut rx_total = 0_u64;
    let mut tx_total = 0_u64;
    let mut found = false;
    for entry in entries.flatten() {
        if entry.file_name() == OsStr::new("lo") {
            continue;
        }
        let statistics = entry.path().join("statistics");
        let Some(rx) = read_u64(&statistics.join("rx_bytes")) else {
            continue;
        };
        let Some(tx) = read_u64(&statistics.join("tx_bytes")) else {
            continue;
        };
        rx_total = rx_total.saturating_add(rx);
        tx_total = tx_total.saturating_add(tx);
        found = true;
    }
    if found {
        (Some(rx_total), Some(tx_total))
    } else {
        (None, None)
    }
}

fn subtract_options(end: Option<u64>, start: Option<u64>) -> Option<u64> {
    end.zip(start).map(|(end, start)| end.saturating_sub(start))
}

fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_trimmed(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_stable() {
        let distribution = distribution([1_000, 2_000, 3_000, 4_000]);
        assert_eq!(distribution.count, 4);
        assert_eq!(distribution.p50_us, 2.0);
        assert_eq!(distribution.p99_us, 4.0);
        assert_eq!(distribution.max_us, 4.0);
    }

    #[test]
    fn empty_distribution_is_zeroed() {
        assert_eq!(distribution([]).count, 0);
    }
}
