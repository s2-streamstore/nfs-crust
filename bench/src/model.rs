use std::path::PathBuf;

use clap::Parser;
use serde::Serialize;

pub const DEFAULT_READ_FILES_PER_WORKER: usize = 2;

#[derive(Debug, Clone, Parser)]
#[command(about = "Benchmark nfs-crust and a Linux NFS mount against one export")]
pub struct Args {
    /// NFS server endpoint used by nfs-crust, normally the mount-target IP.
    #[arg(long)]
    pub endpoint: String,

    /// DNS name verified against the NFS server certificate.
    #[arg(long)]
    pub tls_server_name: String,

    /// NFS export path.
    #[arg(long, default_value = "/")]
    pub export: String,

    /// Existing Linux mount of the same export.
    #[arg(long)]
    pub mount_root: PathBuf,

    /// Stable identifier included in every path and artifact.
    #[arg(long)]
    pub run_id: String,

    /// Directory receiving summary.json and raw JSONL files.
    #[arg(long)]
    pub output_dir: PathBuf,

    /// Source revision represented by this run.
    #[arg(long, default_value = "unknown")]
    pub source_revision: String,

    /// Record that the benchmark source bundle contained uncommitted changes.
    #[arg(long, default_value_t = false)]
    pub source_dirty: bool,

    /// Deterministic scenario-order and payload seed.
    #[arg(long, default_value_t = 0x4e46_5343_5255_5354)]
    pub seed: u64,

    /// Exercise the core workload briefly; intended only for harness validation.
    #[arg(long, default_value_t = false)]
    pub quick: bool,

    /// Independent repetitions of every publishable scenario.
    #[arg(long, default_value_t = 2)]
    pub repetitions: u32,

    /// Include exploratory connection, directory, cache, and operation-detail workloads.
    #[arg(long, default_value_t = false)]
    pub diagnostics: bool,

    /// Diagnostic mode: run only the first N shuffled closed-loop scenarios.
    #[arg(long)]
    pub closed_loop_limit: Option<usize>,

    /// Diagnostic KiB payload sizes appended as sequential create-new scenarios.
    #[arg(long, value_delimiter = ',')]
    pub write_size_sweep_kib: Vec<u64>,

    /// Timeout for each nfs-crust RPC-backed operation.
    #[arg(long, default_value_t = 120)]
    pub operation_timeout_seconds: u64,

    /// Leave the benchmark directory on EFS after the run.
    #[arg(long, default_value_t = false)]
    pub keep_data: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Backend {
    NfsCrust,
    LinuxRemote,
    LinuxRemoteTrustedSize,
    LinuxCached,
}

impl Backend {
    pub fn label(self) -> &'static str {
        match self {
            Self::NfsCrust => "nfs-crust",
            Self::LinuxRemote => "linux-remote",
            Self::LinuxRemoteTrustedSize => "linux-remote-trusted-size",
            Self::LinuxCached => "linux-page-cache",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Operation {
    Get,
    GetKnownSize,
    GetRange4k,
    PutCreateNew,
    PutOverwrite,
    EntryInfo,
    Delete,
    List100,
}

impl Operation {
    pub fn label(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::GetKnownSize => "get-known-size",
            Self::GetRange4k => "get-range-4k",
            Self::PutCreateNew => "put-create-new",
            Self::PutOverwrite => "put-overwrite",
            Self::EntryInfo => "entry-info",
            Self::Delete => "delete",
            Self::List100 => "list-100",
        }
    }

    pub fn transfers_payload(self) -> bool {
        matches!(
            self,
            Self::Get
                | Self::GetKnownSize
                | Self::GetRange4k
                | Self::PutCreateNew
                | Self::PutOverwrite
        )
    }

    pub fn is_write(self) -> bool {
        matches!(self, Self::PutCreateNew | Self::PutOverwrite)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScenarioMode {
    ClosedLoop,
    OpenLoop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpenLoopLoadBasis {
    EqualDemand,
    EqualUtilization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioSpec {
    pub id: String,
    pub family: String,
    pub backend: Backend,
    pub operation: Operation,
    pub mode: ScenarioMode,
    pub object_size_bytes: u64,
    pub concurrency: usize,
    pub client_connections: usize,
    pub read_granularity_bytes: u32,
    pub directory_shards: usize,
    pub read_files_per_worker: usize,
    pub repetition: u32,
    pub warmup_ms: u64,
    pub duration_ms: u64,
    pub fixed_operations_per_worker: Option<u64>,
    pub offered_rate_ops_per_sec: Option<f64>,
    pub max_outstanding: Option<usize>,
    pub open_loop_load_basis: Option<OpenLoopLoadBasis>,
}

impl ScenarioSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn closed_loop(
        family: &str,
        backend: Backend,
        operation: Operation,
        object_size_bytes: u64,
        concurrency: usize,
        client_connections: usize,
        read_granularity_bytes: u32,
        directory_shards: usize,
        repetition: u32,
        warmup_ms: u64,
        duration_ms: u64,
    ) -> Self {
        let id = format!(
            "{family}--{}--{}--{}b--c{concurrency}--k{client_connections}--g{read_granularity_bytes}--s{directory_shards}--r{repetition}--f{DEFAULT_READ_FILES_PER_WORKER}",
            backend.label(),
            operation.label(),
            object_size_bytes,
        );
        Self {
            id,
            family: family.to_owned(),
            backend,
            operation,
            mode: ScenarioMode::ClosedLoop,
            object_size_bytes,
            concurrency,
            client_connections,
            read_granularity_bytes,
            directory_shards,
            read_files_per_worker: DEFAULT_READ_FILES_PER_WORKER,
            repetition,
            warmup_ms,
            duration_ms,
            fixed_operations_per_worker: None,
            offered_rate_ops_per_sec: None,
            max_outstanding: None,
            open_loop_load_basis: None,
        }
    }

    pub fn with_fixed_operations(mut self, operations_per_worker: u64) -> Self {
        self.fixed_operations_per_worker = Some(operations_per_worker);
        self.duration_ms = 0;
        self
    }

    pub fn with_read_files_per_worker(mut self, files: usize) -> Self {
        assert!(
            files > 0,
            "read scenarios require at least one file per worker"
        );
        let old_suffix = format!("--f{}", self.read_files_per_worker);
        let prefix = self
            .id
            .strip_suffix(&old_suffix)
            .expect("closed-loop scenario IDs end with the read-file count");
        self.id = format!("{prefix}--f{files}");
        self.read_files_per_worker = files;
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_loop(
        family: &str,
        backend: Backend,
        operation: Operation,
        object_size_bytes: u64,
        client_connections: usize,
        read_granularity_bytes: u32,
        directory_shards: usize,
        repetition: u32,
        warmup_ms: u64,
        duration_ms: u64,
        offered_rate_ops_per_sec: f64,
        max_outstanding: usize,
        load_basis: OpenLoopLoadBasis,
    ) -> Self {
        let id = format!(
            "{family}--{}--{}--{}b--rate{offered_rate_ops_per_sec:.3}--k{client_connections}--r{repetition}--f{DEFAULT_READ_FILES_PER_WORKER}",
            backend.label(),
            operation.label(),
            object_size_bytes,
        );
        Self {
            id,
            family: family.to_owned(),
            backend,
            operation,
            mode: ScenarioMode::OpenLoop,
            object_size_bytes,
            concurrency: max_outstanding,
            client_connections,
            read_granularity_bytes,
            directory_shards,
            read_files_per_worker: DEFAULT_READ_FILES_PER_WORKER,
            repetition,
            warmup_ms,
            duration_ms,
            fixed_operations_per_worker: None,
            offered_rate_ops_per_sec: Some(offered_rate_ops_per_sec),
            max_outstanding: Some(max_outstanding),
            open_loop_load_basis: Some(load_basis),
        }
    }

    pub fn transferred_bytes_per_success(&self) -> u64 {
        match self.operation {
            Operation::GetRange4k => 4096,
            operation if operation.transfers_payload() => self.object_size_bytes,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RawSample {
    pub scenario_id: String,
    pub worker: usize,
    pub sequence: u64,
    pub completion_offset_ns: u64,
    pub scheduled_offset_ns: Option<u64>,
    pub queue_ns: u64,
    pub service_ns: u64,
    pub total_response_ns: u64,
    pub status: &'static str,
    #[serde(skip_serializing_if = "is_false")]
    pub reconciled_outcome_unknown: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Distribution {
    pub count: u64,
    pub min_us: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
    pub stddev_us: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ResourceDelta {
    pub user_cpu_ms: f64,
    pub system_cpu_ms: f64,
    pub process_max_rss_kib: u64,
    pub host_rx_bytes: Option<u64>,
    pub host_tx_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    pub order: usize,
    pub spec: ScenarioSpec,
    pub started_at_unix_ms: u64,
    pub actual_elapsed_seconds: f64,
    pub successes: u64,
    pub measurement_errors: u64,
    pub errors: u64,
    pub overload_drops: u64,
    pub reconciled_outcomes: u64,
    pub achieved_ops_per_sec: f64,
    pub achieved_mib_per_sec: f64,
    pub service_latency: Distribution,
    pub queue_latency: Distribution,
    pub total_response_latency: Distribution,
    pub p99_sample_sufficient: bool,
    pub p999_sample_sufficient: bool,
    pub resource_delta: ResourceDelta,
    pub distinct_errors: Vec<String>,
    pub cross_backend_validation: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenLoopRateBasis {
    pub operation: Operation,
    pub object_size_bytes: u64,
    pub nfs_crust_peak_ops_per_sec: f64,
    pub linux_remote_peak_ops_per_sec: f64,
    pub load_fraction: f64,
    pub equal_demand_ops_per_sec: f64,
    pub nfs_crust_equal_utilization_ops_per_sec: f64,
    pub linux_remote_equal_utilization_ops_per_sec: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMetadata {
    pub run_id: String,
    pub started_at_unix_ms: u64,
    pub completed_at_unix_ms: u64,
    pub endpoint: String,
    pub tls_server_name: String,
    pub export: String,
    pub mount_root: String,
    pub benchmark_root: String,
    pub source_revision: String,
    pub source_dirty: bool,
    pub seed: u64,
    pub quick: bool,
    pub diagnostics: bool,
    pub repetitions: u32,
    pub mount_remote_read_mode: String,
    pub direct_io_probe: String,
    pub host: HostMetadata,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HostMetadata {
    pub hostname: String,
    pub os_release: String,
    pub kernel: String,
    pub architecture: String,
    pub rustc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessConfig {
    pub nfs_session_slots: u32,
    pub nfs_read_chunk_bytes: u32,
    pub nfs_write_chunk_bytes: u32,
    pub default_read_granularity_bytes: u32,
    pub alternate_read_granularity_bytes: u32,
    pub default_read_files_per_worker: usize,
    pub raw_latency_unit: &'static str,
    pub percentile_method: &'static str,
    pub p99_minimum_samples: u64,
    pub p999_minimum_samples: u64,
    pub timed_region: &'static str,
    pub rpc_transport: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub schema_version: u32,
    pub metadata: RunMetadata,
    pub harness: HarnessConfig,
    pub open_loop_rate_basis: Vec<OpenLoopRateBasis>,
    pub scenarios: Vec<ScenarioResult>,
}
