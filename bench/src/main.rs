mod model;
mod mount;
mod stats;
mod suite;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use model::{
    Args, Backend, DEFAULT_READ_FILES_PER_WORKER, HarnessConfig, OpenLoopLoadBasis,
    OpenLoopRateBasis, Operation, RawSample, RunMetadata, RunSummary, ScenarioMode, ScenarioResult,
    ScenarioSpec,
};
use mount::RemoteReadMode;
use nfs_crust::{NfsClient, PutMode, TlsConfig};
use serde_json::json;
use stats::{P99_MINIMUM_SAMPLES, P999_MINIMUM_SAMPLES, ResourceSnapshot};
use suite::{DEFAULT_READ_GRANULARITY, DEFAULT_SHARDS, LARGE_READ_GRANULARITY, MIB};
use tokio::task::JoinSet;
use tracing_subscriber::fmt::format::FmtSpan;

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type Result<T> = std::result::Result<T, AnyError>;

const SESSION_SLOTS: u32 = 64;
const READ_CHUNK_BYTES: u32 = 1024 * 1024;
const WRITE_CHUNK_BYTES: u32 = 504 * 1024;
const RANGE_BYTES: usize = 4096;
const LIST_ENTRIES: usize = 100;
const START_DELAY: Duration = Duration::from_millis(100);
const STANDARD_OPEN_LOOP_WARMUP_MS: u64 = 750;
const STANDARD_OPEN_LOOP_TARGET_OPERATIONS: f64 = 5_000.0;
const STANDARD_OPEN_LOOP_MIN_SECONDS: f64 = 5.0;
const STANDARD_OPEN_LOOP_MAX_SECONDS: f64 = 30.0;
const QUICK_OPEN_LOOP_MAX_OUTSTANDING: usize = 32;
const STANDARD_OPEN_LOOP_MAX_OUTSTANDING: usize = 1024;

#[derive(Clone)]
struct BenchmarkContext {
    endpoint: String,
    tls_server_name: String,
    export: String,
    mount_root: PathBuf,
    benchmark_relative_root: String,
    benchmark_mount_root: PathBuf,
    clients: Arc<Vec<NfsClient>>,
    payloads: Arc<BTreeMap<u64, Bytes>>,
    remote_read_mode: RemoteReadMode,
    operation_timeout: Duration,
    keep_data: bool,
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    Warmup,
    Measure,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Measure => "measure",
        }
    }
}

enum OperationValue {
    Body(Bytes),
    Size(u64),
    ReconciledSize(u64),
    Count(usize),
    Unit,
}

struct OperationExecution {
    result: std::result::Result<OperationValue, String>,
    dispatch_queue: Duration,
    service: Duration,
}

struct SampleBatch {
    samples: Vec<RawSample>,
    elapsed: Duration,
}

struct SampleTiming {
    completion_offset: Duration,
    scheduled_offset: Option<Duration>,
    queue: Duration,
    service: Duration,
    total: Duration,
}

struct EventLog {
    writer: BufWriter<File>,
}

impl EventLog {
    fn create(path: &Path) -> io::Result<Self> {
        Ok(Self {
            writer: BufWriter::new(File::create(path)?),
        })
    }

    fn emit(&mut self, event: &str, fields: serde_json::Value) -> io::Result<()> {
        serde_json::to_writer(
            &mut self.writer,
            &json!({
                "at_unix_ms": stats::unix_time_millis(),
                "event": event,
                "fields": fields,
            }),
        )?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()
    }
}

struct BenchmarkDataGuard {
    path: PathBuf,
    keep: bool,
}

impl Drop for BenchmarkDataGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.closed_loop_limit.is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_span_events(FmtSpan::CLOSE)
            .with_target(false)
            .init();
    }
    run(args).await
}

async fn run(args: Args) -> Result<()> {
    validate_args(&args)?;
    if let Some(parent) = args.output_dir.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(&args.output_dir)?;
    let summary_path = args.output_dir.join("summary.json");
    let summary_partial_path = args.output_dir.join("summary.json.partial");
    let raw_path = args.output_dir.join("raw-samples.jsonl");
    let warmup_path = args.output_dir.join("warmup-samples.jsonl");
    let events_path = args.output_dir.join("events.jsonl");
    let mut raw_writer = BufWriter::new(File::create(&raw_path)?);
    let mut warmup_writer = BufWriter::new(File::create(&warmup_path)?);
    let mut events = EventLog::create(&events_path)?;

    let started_at_unix_ms = stats::unix_time_millis();
    let safe_run_id = safe_component(&args.run_id)?;
    let benchmark_relative_root = format!("nfs-crust-bench-{safe_run_id}");
    let benchmark_mount_root = args.mount_root.join(&benchmark_relative_root);
    fs::create_dir(&benchmark_mount_root).map_err(|error| {
        format!(
            "refusing to replace benchmark root {}: {error}",
            benchmark_mount_root.display()
        )
    })?;
    let _guard = BenchmarkDataGuard {
        path: benchmark_mount_root.clone(),
        keep: args.keep_data,
    };

    let (remote_read_mode, direct_io_probe) = mount::probe_remote_read_mode(&benchmark_mount_root);
    if remote_read_mode != RemoteReadMode::Direct && !args.quick {
        return Err(format!(
            "publishable runs require O_DIRECT for remote Linux reads: {direct_io_probe}"
        )
        .into());
    }
    println!(
        "mount remote-read mode: {} ({direct_io_probe})",
        remote_read_mode.label()
    );
    events.emit(
        "run-started",
        json!({
            "run_id": args.run_id,
            "endpoint": args.endpoint,
            "tls_server_name": args.tls_server_name,
            "rpc_transport": "tls",
            "mount_remote_read_mode": remote_read_mode.label(),
            "direct_io_probe": direct_io_probe,
        }),
    )?;

    let diagnostic_write_sizes = args
        .write_size_sweep_kib
        .iter()
        .map(|size| size * 1024)
        .collect::<Vec<_>>();
    let payloads = Arc::new(build_payloads(args.seed, &diagnostic_write_sizes));
    let context = BenchmarkContext {
        endpoint: args.endpoint.clone(),
        tls_server_name: args.tls_server_name.clone(),
        export: args.export.clone(),
        mount_root: args.mount_root.clone(),
        benchmark_relative_root: benchmark_relative_root.clone(),
        benchmark_mount_root: benchmark_mount_root.clone(),
        clients: Arc::new(Vec::new()),
        payloads,
        remote_read_mode,
        operation_timeout: Duration::from_secs(args.operation_timeout_seconds),
        keep_data: args.keep_data,
    };

    let mut results = Vec::new();
    let mut specs =
        suite::closed_loop_suite(args.quick, args.seed, args.repetitions, args.diagnostics);
    if let Some(limit) = args.closed_loop_limit {
        specs.truncate(limit);
    }
    specs.extend(diagnostic_write_specs(&diagnostic_write_sizes));
    let planned_closed_loop = specs.len();
    for spec in specs.drain(..) {
        let order = results.len() + 1;
        println!(
            "[{order}/{planned_closed_loop}] {} {} {} c{} r{}",
            spec.family,
            spec.backend.label(),
            spec.operation.label(),
            spec.concurrency,
            spec.repetition
        );
        let result = run_scenario(
            &context,
            spec,
            order,
            &mut raw_writer,
            &mut warmup_writer,
            &mut events,
        )
        .await?;
        results.push(result);
    }

    let (rate_basis, open_loop_specs) = if args.closed_loop_limit.is_some() {
        (Vec::new(), Vec::new())
    } else {
        derive_open_loop_specs(&results, args.quick, args.repetitions)?
    };
    let open_loop_count = open_loop_specs.len();
    for (index, spec) in open_loop_specs.into_iter().enumerate() {
        let order = results.len() + 1;
        println!(
            "[tail {}/{}] {} {} at {:.1} ops/s",
            index + 1,
            open_loop_count,
            spec.backend.label(),
            spec.operation.label(),
            spec.offered_rate_ops_per_sec.unwrap_or_default()
        );
        let result = run_scenario(
            &context,
            spec,
            order,
            &mut raw_writer,
            &mut warmup_writer,
            &mut events,
        )
        .await?;
        results.push(result);
    }

    raw_writer.flush()?;
    warmup_writer.flush()?;
    let completed_at_unix_ms = stats::unix_time_millis();
    let metadata = RunMetadata {
        run_id: args.run_id.clone(),
        started_at_unix_ms,
        completed_at_unix_ms,
        endpoint: args.endpoint.clone(),
        tls_server_name: args.tls_server_name.clone(),
        export: args.export.clone(),
        mount_root: args.mount_root.display().to_string(),
        benchmark_root: benchmark_relative_root,
        source_revision: args.source_revision.clone(),
        source_dirty: args.source_dirty,
        seed: args.seed,
        quick: args.quick,
        diagnostics: args.diagnostics,
        repetitions: if args.quick { 1 } else { args.repetitions },
        mount_remote_read_mode: remote_read_mode.label().to_owned(),
        direct_io_probe,
        host: stats::host_metadata(),
    };
    let summary = RunSummary {
        schema_version: 4,
        metadata,
        harness: HarnessConfig {
            nfs_session_slots: SESSION_SLOTS,
            nfs_read_chunk_bytes: READ_CHUNK_BYTES,
            nfs_write_chunk_bytes: WRITE_CHUNK_BYTES,
            default_read_granularity_bytes: DEFAULT_READ_GRANULARITY,
            alternate_read_granularity_bytes: LARGE_READ_GRANULARITY,
            default_read_files_per_worker: DEFAULT_READ_FILES_PER_WORKER,
            raw_latency_unit: "nanoseconds",
            percentile_method: "nearest-rank-per-scenario-repetition",
            p99_minimum_samples: P99_MINIMUM_SAMPLES,
            p999_minimum_samples: P999_MINIMUM_SAMPLES,
            timed_region: "public API or matched syscall operation; payload checks excluded",
            rpc_transport: "tls",
        },
        open_loop_rate_basis: rate_basis,
        scenarios: results,
    };
    serde_json::to_writer_pretty(File::create(&summary_partial_path)?, &summary)?;
    fs::rename(&summary_partial_path, &summary_path)?;
    events.emit(
        "run-completed",
        json!({
            "summary": summary_path,
            "raw_samples": raw_path,
            "scenario_count": summary.scenarios.len(),
            "errors": summary.scenarios.iter().map(|result| result.errors).sum::<u64>(),
            "drops": summary.scenarios.iter().map(|result| result.overload_drops).sum::<u64>(),
        }),
    )?;

    let failures = summary
        .scenarios
        .iter()
        .filter(|result| {
            result.errors != 0
                || result.overload_drops != 0
                || result.cross_backend_validation.starts_with("FAILED")
        })
        .count();
    println!(
        "completed {} scenarios with {failures} invalid scenario(s); artifacts: {}",
        summary.scenarios.len(),
        args.output_dir.display()
    );
    if failures != 0 {
        return Err(format!("{failures} benchmark scenarios failed validation").into());
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if !(1..=20).contains(&args.repetitions) {
        return Err("repetitions must be between 1 and 20".into());
    }
    if args.closed_loop_limit == Some(0) {
        return Err("closed-loop-limit must be greater than zero".into());
    }
    if args.operation_timeout_seconds == 0 {
        return Err("operation-timeout-seconds must be greater than zero".into());
    }
    if !args.write_size_sweep_kib.is_empty() && args.closed_loop_limit.is_none() {
        return Err("write-size-sweep-kib requires closed-loop-limit diagnostic mode".into());
    }
    if args
        .write_size_sweep_kib
        .iter()
        .any(|size| !(1..=2048).contains(size))
    {
        return Err("write-size-sweep-kib values must be between 1 and 2048".into());
    }
    if !args.mount_root.is_dir() {
        return Err(format!(
            "mount root is not a directory: {}",
            args.mount_root.display()
        )
        .into());
    }
    if args.run_id.trim().is_empty() {
        return Err("run id must not be empty".into());
    }
    if args.tls_server_name.trim().is_empty() {
        return Err("TLS server name must not be empty".into());
    }
    if args.output_dir.exists() {
        return Err(format!(
            "output directory already exists; use a fresh path: {}",
            args.output_dir.display()
        )
        .into());
    }
    let mount_root = fs::canonicalize(&args.mount_root)?;
    let output_dir = canonicalize_new_path(&args.output_dir)?;
    if output_dir.starts_with(&mount_root) || mount_root.starts_with(&output_dir) {
        return Err("output directory and NFS mount must not overlap".into());
    }
    Ok(())
}

fn canonicalize_new_path(path: &Path) -> io::Result<PathBuf> {
    if path
        .components()
        .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "output directory must not contain parent-directory components",
        ));
    }
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    while !existing.exists() {
        let component = existing.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "path has no existing ancestor")
        })?;
        missing.push(component.to_owned());
        existing = existing.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "path has no existing ancestor")
        })?;
    }
    let mut resolved = fs::canonicalize(existing)?;
    for component in missing.iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

async fn connect_clients(
    endpoint: &str,
    tls_server_name: &str,
    export: &str,
    granularity: u32,
    count: usize,
    operation_timeout: Duration,
) -> Result<Vec<NfsClient>> {
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        clients.push(
            NfsClient::builder(endpoint, export)
                .tls(TlsConfig::new(tls_server_name))
                .session_slots(SESSION_SLOTS)
                .read_chunk_size(READ_CHUNK_BYTES)
                .write_chunk_size(WRITE_CHUNK_BYTES)
                .read_granularity(granularity)
                .operation_timeout(Some(operation_timeout))
                .connect()
                .await?,
        );
    }
    Ok(clients)
}

async fn run_scenario(
    context: &BenchmarkContext,
    spec: ScenarioSpec,
    order: usize,
    raw_writer: &mut BufWriter<File>,
    warmup_writer: &mut BufWriter<File>,
    events: &mut EventLog,
) -> Result<ScenarioResult> {
    prepare_scenario(context, &spec)?;
    let mut scenario_context = context.clone();
    if spec.backend == Backend::NfsCrust {
        scenario_context.clients = Arc::new(
            connect_clients(
                &context.endpoint,
                &context.tls_server_name,
                &context.export,
                spec.read_granularity_bytes,
                spec.client_connections,
                context.operation_timeout,
            )
            .await?,
        );
    }
    let context = &scenario_context;
    events.emit(
        "scenario-started",
        json!({"order": order, "scenario_id": spec.id, "spec": spec}),
    )?;

    let mut warmup_issue = None;
    if spec.warmup_ms != 0 {
        let warmup = match spec.mode {
            ScenarioMode::ClosedLoop => {
                run_closed_loop(context, &spec, Phase::Warmup, Some(spec.warmup_ms)).await?
            }
            ScenarioMode::OpenLoop => {
                run_open_loop(context, &spec, Phase::Warmup, spec.warmup_ms).await?
            }
        };
        warmup_issue = batch_issue(&warmup, "warmup");
        append_raw_samples(warmup_writer, &warmup.samples)?;
        warmup_writer.flush()?;
    }

    let resources_before = ResourceSnapshot::capture();
    let started_at_unix_ms = stats::unix_time_millis();
    let batch = match spec.mode {
        ScenarioMode::ClosedLoop => run_closed_loop(context, &spec, Phase::Measure, None).await?,
        ScenarioMode::OpenLoop => {
            run_open_loop(context, &spec, Phase::Measure, spec.duration_ms).await?
        }
    };
    let resources_after = ResourceSnapshot::capture();

    let cross_validation = cross_validate(context, &spec).await;
    let mut result = summarize_scenario(
        order,
        spec.clone(),
        started_at_unix_ms,
        &batch,
        resources_before.delta(resources_after),
        match &cross_validation {
            Ok(message) => message.clone(),
            Err(error) => format!("FAILED: {error}"),
        },
    );
    if let Err(error) = cross_validation {
        result.errors = result.errors.saturating_add(1);
        result
            .distinct_errors
            .push(format!("cross-backend validation: {error}"));
    }
    if let Some(issue) = warmup_issue {
        result.errors = result.errors.saturating_add(1);
        result.distinct_errors.push(issue);
    }
    if result.successes == 0 {
        result.errors = result.errors.saturating_add(1);
        result
            .distinct_errors
            .push("measurement completed without a successful operation".to_owned());
    }

    append_raw_samples(raw_writer, &batch.samples)?;
    raw_writer.flush()?;
    events.emit(
        "scenario-completed",
        json!({
            "order": order,
            "scenario_id": spec.id,
            "successes": result.successes,
            "measurement_errors": result.measurement_errors,
            "errors": result.errors,
            "drops": result.overload_drops,
            "reconciled_outcomes": result.reconciled_outcomes,
            "ops_per_sec": result.achieved_ops_per_sec,
            "p99_us": result.total_response_latency.p99_us,
            "p999_us": result.total_response_latency.p999_us,
            "validation": result.cross_backend_validation,
        }),
    )?;

    if !context.keep_data {
        let scenario_mount_root = scenario_mount_root(context, &spec);
        fs::remove_dir_all(&scenario_mount_root).map_err(|error| {
            format!(
                "failed to remove scenario data {}: {error}",
                scenario_mount_root.display()
            )
        })?;
    }
    Ok(result)
}

fn prepare_scenario(context: &BenchmarkContext, spec: &ScenarioSpec) -> Result<()> {
    let root = scenario_mount_root(context, spec);
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    fs::create_dir_all(&root)?;
    for phase in [Phase::Warmup, Phase::Measure] {
        for shard in 0..spec.directory_shards {
            fs::create_dir_all(root.join(phase.label()).join(format!("s{shard:02}")))?;
        }
    }
    for shard in 0..spec.directory_shards {
        fs::create_dir_all(root.join("data").join(format!("s{shard:02}")))?;
    }

    let payload = payload(context, spec.object_size_bytes)?;
    match spec.operation {
        Operation::Get | Operation::GetKnownSize | Operation::GetRange4k => {
            for worker in 0..spec.concurrency {
                for slot in 0..spec.read_files_per_worker {
                    let path =
                        operation_mount_path(context, spec, Phase::Measure, worker, slot as u64);
                    mount::write_seed(&path, &payload)?;
                    if spec.backend == Backend::LinuxCached {
                        mount::warm_page_cache(&path, payload.len())?;
                    }
                }
            }
        }
        Operation::PutOverwrite => {
            let sentinel = sentinel_payload(&payload);
            for phase in [Phase::Warmup, Phase::Measure] {
                for worker in 0..spec.concurrency {
                    for slot in 0..spec.read_files_per_worker {
                        let path = operation_mount_path(context, spec, phase, worker, slot as u64);
                        mount::write_seed(&path, &sentinel)?;
                    }
                }
            }
        }
        Operation::EntryInfo => {
            mount::write_seed(
                &operation_mount_path(context, spec, Phase::Measure, 0, 0),
                &payload,
            )?;
        }
        Operation::Delete => {
            let count = spec.fixed_operations_per_worker.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "delete scenario requires fixed count",
                )
            })?;
            for worker in 0..spec.concurrency {
                for sequence in 0..count {
                    mount::write_seed(
                        &operation_mount_path(context, spec, Phase::Measure, worker, sequence),
                        &payload,
                    )?;
                }
            }
        }
        Operation::List100 => {
            let list_root = operation_mount_path(context, spec, Phase::Measure, 0, 0);
            fs::create_dir_all(&list_root)?;
            for index in 0..LIST_ENTRIES {
                mount::write_seed(&list_root.join(format!("entry-{index:03}")), &payload)?;
            }
        }
        Operation::PutCreateNew => {}
    }
    Ok(())
}

async fn run_closed_loop(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    phase: Phase,
    duration_override_ms: Option<u64>,
) -> Result<SampleBatch> {
    let start = Instant::now() + START_DELAY;
    let fixed_operations = if duration_override_ms.is_some() {
        None
    } else {
        spec.fixed_operations_per_worker
    };
    let duration_ms = duration_override_ms.unwrap_or(spec.duration_ms);
    let deadline = start + Duration::from_millis(duration_ms);
    let mut workers: JoinSet<Result<Vec<RawSample>>> = JoinSet::new();

    for worker in 0..spec.concurrency {
        let context = context.clone();
        let spec = spec.clone();
        workers.spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(start)).await;
            let mut samples = Vec::new();
            let mut sequence = 0_u64;
            loop {
                if let Some(limit) = fixed_operations {
                    if sequence >= limit {
                        break;
                    }
                } else if Instant::now() >= deadline {
                    break;
                }
                let operation_started = Instant::now();
                let execution = execute_operation(&context, &spec, phase, worker, sequence).await;
                let operation_completed = Instant::now();
                let completion_offset = operation_completed.saturating_duration_since(start);
                let sample = sample_from_result(
                    &context,
                    &spec,
                    worker,
                    sequence,
                    SampleTiming {
                        completion_offset,
                        scheduled_offset: None,
                        queue: execution.dispatch_queue,
                        service: execution.service,
                        total: operation_completed.saturating_duration_since(operation_started),
                    },
                    execution.result,
                );
                samples.push(sample);
                sequence = sequence.saturating_add(1);
            }
            Ok(samples)
        });
    }

    let mut samples = Vec::new();
    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(Ok(worker_samples)) => samples.extend(worker_samples),
            Ok(Err(error)) => {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                return Err(error);
            }
            Err(error) => {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                return Err(format!("worker task failed: {error}").into());
            }
        }
    }
    let elapsed = elapsed_for_samples(&samples, Duration::from_millis(duration_ms));
    Ok(SampleBatch { samples, elapsed })
}

async fn run_open_loop(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    phase: Phase,
    duration_ms: u64,
) -> Result<SampleBatch> {
    let offered_rate = spec
        .offered_rate_ops_per_sec
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "open-loop rate missing"))?;
    let max_outstanding = spec
        .max_outstanding
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "outstanding cap missing"))?;
    let interval = Duration::from_secs_f64(1.0 / offered_rate);
    let launch_window = Duration::from_millis(duration_ms);
    let start = Instant::now() + START_DELAY;
    let deadline = start + launch_window;
    let mut tasks = JoinSet::new();
    let mut samples = Vec::new();
    let mut sequence = 0_u64;

    loop {
        let scheduled_offset = interval.mul_f64(sequence as f64);
        let scheduled = start + scheduled_offset;
        if scheduled >= deadline {
            break;
        }
        tokio::time::sleep_until(tokio::time::Instant::from_std(scheduled)).await;
        while let Some(joined) = tasks.try_join_next() {
            samples.push(joined.map_err(|error| format!("open-loop task failed: {error}"))?);
        }
        let worker = sequence as usize % max_outstanding;
        if tasks.len() >= max_outstanding {
            samples.push(RawSample {
                scenario_id: spec.id.clone(),
                worker,
                sequence,
                completion_offset_ns: nanos(scheduled_offset),
                scheduled_offset_ns: Some(nanos(scheduled_offset)),
                queue_ns: 0,
                service_ns: 0,
                total_response_ns: 0,
                status: "dropped",
                reconciled_outcome_unknown: false,
                error: Some("outstanding-cap-reached".to_owned()),
            });
        } else {
            let context = context.clone();
            let spec = spec.clone();
            tasks.spawn(async move {
                let operation_started = Instant::now();
                let queue = operation_started.saturating_duration_since(scheduled);
                let operation_sequence = sequence / max_outstanding as u64;
                let execution =
                    execute_operation(&context, &spec, phase, worker, operation_sequence).await;
                let operation_completed = Instant::now();
                let total = operation_completed.saturating_duration_since(scheduled);
                sample_from_result(
                    &context,
                    &spec,
                    worker,
                    sequence,
                    SampleTiming {
                        completion_offset: operation_completed.saturating_duration_since(start),
                        scheduled_offset: Some(scheduled_offset),
                        queue: queue.saturating_add(execution.dispatch_queue),
                        service: execution.service,
                        total,
                    },
                    execution.result,
                )
            });
        }
        sequence = sequence.saturating_add(1);
    }

    while let Some(joined) = tasks.join_next().await {
        samples.push(joined.map_err(|error| format!("open-loop task failed: {error}"))?);
    }
    let elapsed = elapsed_for_samples(&samples, launch_window);
    Ok(SampleBatch { samples, elapsed })
}

fn sample_from_result(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    worker: usize,
    sequence: u64,
    timing: SampleTiming,
    operation_result: std::result::Result<OperationValue, String>,
) -> RawSample {
    let reconciled_candidate = matches!(&operation_result, Ok(OperationValue::ReconciledSize(_)));
    let validated = operation_result.and_then(|value| validate_value(context, spec, &value));
    let (status, error) = match validated {
        Ok(()) => ("ok", None),
        Err(error) => ("error", Some(sanitize_error(&error))),
    };
    let reconciled_outcome_unknown = reconciled_candidate && status == "ok";
    RawSample {
        scenario_id: spec.id.clone(),
        worker,
        sequence,
        completion_offset_ns: nanos(timing.completion_offset),
        scheduled_offset_ns: timing.scheduled_offset.map(nanos),
        queue_ns: nanos(timing.queue),
        service_ns: nanos(timing.service),
        total_response_ns: nanos(timing.total),
        status,
        reconciled_outcome_unknown,
        error,
    }
}

async fn execute_operation(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    phase: Phase,
    worker: usize,
    sequence: u64,
) -> OperationExecution {
    match spec.backend {
        Backend::NfsCrust => {
            let started = Instant::now();
            let result = execute_nfs(context, spec, phase, worker, sequence).await;
            OperationExecution {
                result,
                dispatch_queue: Duration::ZERO,
                service: Instant::now().saturating_duration_since(started),
            }
        }
        Backend::LinuxRemote | Backend::LinuxRemoteTrustedSize | Backend::LinuxCached => {
            execute_mount(context, spec, phase, worker, sequence).await
        }
    }
}

async fn execute_nfs(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    phase: Phase,
    worker: usize,
    sequence: u64,
) -> std::result::Result<OperationValue, String> {
    let client = &context.clients[worker % context.clients.len()];
    let path = operation_relative_path(context, spec, phase, worker, sequence);
    let payload = payload(context, spec.object_size_bytes).map_err(|error| error.to_string())?;
    match spec.operation {
        Operation::Get => client
            .get(&path)
            .await
            .map(OperationValue::Body)
            .map_err(|error| error.to_string()),
        Operation::GetKnownSize => client
            .get_known_size(&path, spec.object_size_bytes)
            .await
            .map(OperationValue::Body)
            .map_err(|error| error.to_string()),
        Operation::GetRange4k => {
            let offset = range_offset(spec.object_size_bytes);
            client
                .get_range(&path, offset..offset + RANGE_BYTES as u64)
                .await
                .map(OperationValue::Body)
                .map_err(|error| error.to_string())
        }
        Operation::PutCreateNew => execute_nfs_create_new(client, &path, payload).await,
        Operation::PutOverwrite => client
            .put(&path, payload, PutMode::Overwrite)
            .await
            .map(|()| OperationValue::Size(spec.object_size_bytes))
            .map_err(|error| error.to_string()),
        Operation::EntryInfo => client
            .entry_info(&path)
            .await
            .map_err(|error| error.to_string())
            .map(|info| OperationValue::Size(info.size)),
        Operation::Delete => client
            .delete(&path)
            .await
            .map(|()| OperationValue::Unit)
            .map_err(|error| error.to_string()),
        Operation::List100 => client
            .list_page(&path, LIST_ENTRIES, None)
            .await
            .map(|result| OperationValue::Count(result.entries.len()))
            .map_err(|error| error.to_string()),
    }
}

async fn execute_nfs_create_new(
    client: &NfsClient,
    path: &str,
    payload: Bytes,
) -> std::result::Result<OperationValue, String> {
    let expected_size = payload.len() as u64;
    let original_error = match client
        .put(path, payload.clone(), PutMode::IfNotExists)
        .await
    {
        Ok(()) => return Ok(OperationValue::Size(expected_size)),
        Err(error) if error.is_outcome_unknown() => error,
        Err(error) => return Err(error.to_string()),
    };

    match client.get_known_size(path, expected_size).await {
        Ok(observed) if observed == payload => Ok(OperationValue::ReconciledSize(expected_size)),
        Ok(observed) => Err(format!(
            "create-new outcome was unknown and the destination payload differed: \
             observed {} bytes at {path:?}; original error: {original_error}",
            observed.len()
        )),
        Err(error) if error.is_not_found() => client
            .put(path, payload, PutMode::IfNotExists)
            .await
            .map(|()| OperationValue::ReconciledSize(expected_size))
            .map_err(|retry_error| {
                format!(
                    "create-new outcome was unknown and the destination was absent, but the \
                     single retry failed: {retry_error}; original error: {original_error}"
                )
            }),
        Err(error) => Err(format!(
            "create-new outcome was unknown and destination reconciliation failed: {error}; \
             original error: {original_error}"
        )),
    }
}

async fn execute_mount(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    phase: Phase,
    worker: usize,
    sequence: u64,
) -> OperationExecution {
    let dispatched = Instant::now();
    let path = operation_mount_path(context, spec, phase, worker, sequence);
    let payload = match payload(context, spec.object_size_bytes) {
        Ok(payload) => payload,
        Err(error) => {
            return OperationExecution {
                result: Err(error.to_string()),
                dispatch_queue: Duration::ZERO,
                service: Duration::ZERO,
            };
        }
    };
    let operation = spec.operation;
    let backend = spec.backend;
    let object_size = match usize::try_from(spec.object_size_bytes) {
        Ok(size) => size,
        Err(_) => {
            return OperationExecution {
                result: Err("object size exceeds usize".to_owned()),
                dispatch_queue: Duration::ZERO,
                service: Duration::ZERO,
            };
        }
    };
    let remote_read_mode = context.remote_read_mode;
    let temporary_suffix = format!("{}-{worker}-{sequence}", phase.label());
    let joined = tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let io_result = (|| -> io::Result<OperationValue> {
            match operation {
                Operation::Get => {
                    let body = if backend == Backend::LinuxCached {
                        mount::read_cached(&path, object_size)
                    } else {
                        mount::read_remote(&path, object_size, false, remote_read_mode)
                    }?;
                    Ok(OperationValue::Body(Bytes::from(body)))
                }
                Operation::GetKnownSize => {
                    let body = if backend == Backend::LinuxCached {
                        mount::read_cached(&path, object_size)
                    } else if backend == Backend::LinuxRemoteTrustedSize {
                        mount::read_remote_trusted_size(&path, object_size, remote_read_mode)
                    } else {
                        mount::read_remote(&path, object_size, true, remote_read_mode)
                    }?;
                    Ok(OperationValue::Body(Bytes::from(body)))
                }
                Operation::GetRange4k => {
                    let offset = range_offset(object_size as u64);
                    let body =
                        mount::read_remote_range(&path, offset, RANGE_BYTES, remote_read_mode)?;
                    Ok(OperationValue::Body(Bytes::from(body)))
                }
                Operation::PutCreateNew | Operation::PutOverwrite => {
                    mount::atomic_put(
                        &path,
                        &payload,
                        operation == Operation::PutCreateNew,
                        &temporary_suffix,
                    )?;
                    Ok(OperationValue::Size(payload.len() as u64))
                }
                Operation::EntryInfo => mount::metadata_size(&path).map(OperationValue::Size),
                Operation::Delete => mount::delete(&path).map(|()| OperationValue::Unit),
                Operation::List100 => mount::list_count(&path).map(OperationValue::Count),
            }
        })();
        let completed = Instant::now();
        (
            io_result.map_err(|error| error.to_string()),
            started.saturating_duration_since(dispatched),
            completed.saturating_duration_since(started),
        )
    })
    .await;
    match joined {
        Ok((result, dispatch_queue, service)) => OperationExecution {
            result,
            dispatch_queue,
            service,
        },
        Err(error) => OperationExecution {
            result: Err(format!("blocking task failed: {error}")),
            dispatch_queue: Duration::ZERO,
            service: Instant::now().saturating_duration_since(dispatched),
        },
    }
}

fn validate_value(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    value: &OperationValue,
) -> std::result::Result<(), String> {
    match (spec.operation, value) {
        (
            Operation::Get | Operation::GetKnownSize | Operation::GetRange4k,
            OperationValue::Body(body),
        ) => {
            let payload =
                payload(context, spec.object_size_bytes).map_err(|error| error.to_string())?;
            if spec.operation == Operation::GetRange4k {
                let offset = range_offset(spec.object_size_bytes) as usize;
                validate_sampled_body(body, &payload[offset..offset + RANGE_BYTES])
            } else {
                validate_sampled_body(body, &payload)
            }
        }
        (
            Operation::PutCreateNew | Operation::PutOverwrite | Operation::EntryInfo,
            OperationValue::Size(size),
        ) if *size == spec.object_size_bytes => Ok(()),
        (Operation::PutCreateNew, OperationValue::ReconciledSize(size))
            if *size == spec.object_size_bytes =>
        {
            Ok(())
        }
        (Operation::Delete, OperationValue::Unit) => Ok(()),
        (Operation::List100, OperationValue::Count(count)) if *count == LIST_ENTRIES => Ok(()),
        (_, OperationValue::Size(size)) => Err(format!(
            "observed size {size}, expected {}",
            spec.object_size_bytes
        )),
        (_, OperationValue::ReconciledSize(size)) => Err(format!(
            "observed reconciled size {size}, expected {}",
            spec.object_size_bytes
        )),
        (_, OperationValue::Count(count)) => {
            Err(format!("observed {count} entries, expected {LIST_ENTRIES}"))
        }
        _ => Err("operation returned an unexpected value shape".to_owned()),
    }
}

fn validate_sampled_body(actual: &[u8], expected: &[u8]) -> std::result::Result<(), String> {
    if actual.len() != expected.len() {
        return Err(format!(
            "body length {} did not match {}",
            actual.len(),
            expected.len()
        ));
    }
    if actual.is_empty() {
        return Ok(());
    }
    let last = actual.len() - 1;
    for index in [0, last / 7, last / 3, last / 2, last * 2 / 3, last] {
        if actual[index] != expected[index] {
            return Err(format!("body mismatch at byte {index}"));
        }
    }
    Ok(())
}

fn validate_full_body(actual: &[u8], expected: &[u8]) -> std::result::Result<(), String> {
    if actual.len() != expected.len() {
        return Err(format!(
            "body length {} did not match {}",
            actual.len(),
            expected.len()
        ));
    }
    if actual == expected {
        return Ok(());
    }
    let mismatch = actual
        .iter()
        .zip(expected)
        .position(|(actual, expected)| actual != expected)
        .unwrap_or_default();
    Err(format!("body mismatch at byte {mismatch}"))
}

async fn cross_validate(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
) -> std::result::Result<String, String> {
    if spec.operation == Operation::Delete {
        let count = spec
            .fixed_operations_per_worker
            .ok_or_else(|| "delete scenario did not declare its path count".to_owned())?;
        if spec.backend == Backend::NfsCrust {
            for worker in 0..spec.concurrency {
                for sequence in 0..count {
                    let path =
                        operation_mount_path(context, spec, Phase::Measure, worker, sequence);
                    match fs::metadata(&path) {
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                        Ok(_) => {
                            return Err(format!("deleted path still exists: {}", path.display()));
                        }
                        Err(error) => {
                            return Err(format!("could not verify {}: {error}", path.display()));
                        }
                    }
                }
            }
        } else {
            let clients = connect_clients(
                &context.endpoint,
                &context.tls_server_name,
                &context.export,
                DEFAULT_READ_GRANULARITY,
                1,
                context.operation_timeout,
            )
            .await
            .map_err(|error| error.to_string())?;
            for worker in 0..spec.concurrency {
                for sequence in 0..count {
                    let path =
                        operation_relative_path(context, spec, Phase::Measure, worker, sequence);
                    match clients[0].entry_info(&path).await {
                        Err(error) if error.is_not_found() => {}
                        Ok(_) => return Err(format!("deleted path still exists: {path}")),
                        Err(error) => return Err(format!("could not verify {path}: {error}")),
                    }
                }
            }
        }
        return Ok(format!(
            "passed: opposite backend confirmed {} deleted path(s) absent",
            count * spec.concurrency as u64
        ));
    }

    if !spec.operation.is_write() {
        return Ok(match spec.operation {
            Operation::Get | Operation::GetKnownSize | Operation::GetRange4k => {
                "payload length and sampled bytes validated after every read".to_owned()
            }
            Operation::EntryInfo => "metadata size validated after every lookup".to_owned(),
            Operation::List100 => "every listing returned exactly 100 entries".to_owned(),
            _ => "not applicable".to_owned(),
        });
    }

    let payload = payload(context, spec.object_size_bytes).map_err(|error| error.to_string())?;
    let paths: Vec<String> = if spec.operation == Operation::PutOverwrite {
        (0..spec.concurrency)
            .flat_map(|worker| {
                (0..spec.read_files_per_worker).map(move |slot| {
                    operation_relative_path(context, spec, Phase::Measure, worker, slot as u64)
                })
            })
            .collect()
    } else {
        vec![operation_relative_path(context, spec, Phase::Measure, 0, 0)]
    };

    if spec.backend == Backend::NfsCrust {
        for relative_path in &paths {
            let mount_path = context.mount_root.join(relative_path);
            let observed =
                mount::read_remote(&mount_path, payload.len(), true, context.remote_read_mode)
                    .map_err(|error| error.to_string())?;
            validate_full_body(&observed, &payload)?;
        }
    } else {
        let clients = connect_clients(
            &context.endpoint,
            &context.tls_server_name,
            &context.export,
            DEFAULT_READ_GRANULARITY,
            1,
            context.operation_timeout,
        )
        .await
        .map_err(|error| error.to_string())?;
        for relative_path in &paths {
            let observed = clients[0]
                .get_known_size(relative_path, spec.object_size_bytes)
                .await
                .map_err(|error| error.to_string())?;
            validate_full_body(&observed, &payload)?;
        }
    }
    Ok(if spec.backend == Backend::NfsCrust {
        format!(
            "passed: Linux remote reads fully verified {} nfs-crust publication(s)",
            paths.len()
        )
    } else {
        format!(
            "passed: nfs-crust fully verified {} Linux publication(s)",
            paths.len()
        )
    })
}

fn summarize_scenario(
    order: usize,
    spec: ScenarioSpec,
    started_at_unix_ms: u64,
    batch: &SampleBatch,
    resource_delta: model::ResourceDelta,
    cross_backend_validation: String,
) -> ScenarioResult {
    let successful: Vec<&RawSample> = batch
        .samples
        .iter()
        .filter(|sample| sample.status == "ok")
        .collect();
    let successes = successful.len() as u64;
    let errors = batch
        .samples
        .iter()
        .filter(|sample| sample.status == "error")
        .count() as u64;
    let overload_drops = batch
        .samples
        .iter()
        .filter(|sample| sample.status == "dropped")
        .count() as u64;
    let reconciled_outcomes = successful
        .iter()
        .filter(|sample| sample.reconciled_outcome_unknown)
        .count() as u64;
    let elapsed_seconds = batch.elapsed.as_secs_f64().max(f64::EPSILON);
    let achieved_ops_per_sec = successes as f64 / elapsed_seconds;
    let achieved_mib_per_sec =
        achieved_ops_per_sec * spec.transferred_bytes_per_success() as f64 / (1024.0 * 1024.0);
    let distinct_errors: BTreeSet<String> = batch
        .samples
        .iter()
        .filter_map(|sample| sample.error.clone())
        .collect();

    ScenarioResult {
        order,
        spec,
        started_at_unix_ms,
        actual_elapsed_seconds: elapsed_seconds,
        successes,
        measurement_errors: errors,
        errors,
        overload_drops,
        reconciled_outcomes,
        achieved_ops_per_sec,
        achieved_mib_per_sec,
        service_latency: stats::distribution(successful.iter().map(|sample| sample.service_ns)),
        queue_latency: stats::distribution(successful.iter().map(|sample| sample.queue_ns)),
        total_response_latency: stats::distribution(
            successful.iter().map(|sample| sample.total_response_ns),
        ),
        p99_sample_sufficient: successes >= P99_MINIMUM_SAMPLES,
        p999_sample_sufficient: successes >= P999_MINIMUM_SAMPLES,
        resource_delta,
        distinct_errors: distinct_errors.into_iter().collect(),
        cross_backend_validation,
    }
}

fn batch_issue(batch: &SampleBatch, phase: &str) -> Option<String> {
    let successes = batch
        .samples
        .iter()
        .filter(|sample| sample.status == "ok")
        .count();
    let errors = batch
        .samples
        .iter()
        .filter(|sample| sample.status == "error")
        .count();
    let drops = batch
        .samples
        .iter()
        .filter(|sample| sample.status == "dropped")
        .count();
    let distinct_errors = batch
        .samples
        .iter()
        .filter_map(|sample| sample.error.as_deref())
        .collect::<BTreeSet<_>>();
    (successes == 0 || errors != 0 || drops != 0).then(|| {
        let mut issue = format!(
            "{phase} was invalid: {successes} successes, {errors} errors, {drops} overload drops"
        );
        if !distinct_errors.is_empty() {
            issue.push_str("; distinct errors: ");
            issue.push_str(&distinct_errors.into_iter().collect::<Vec<_>>().join(" | "));
        }
        issue
    })
}

fn append_raw_samples(writer: &mut BufWriter<File>, samples: &[RawSample]) -> Result<()> {
    for sample in samples {
        serde_json::to_writer(&mut *writer, sample)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn derive_open_loop_specs(
    results: &[ScenarioResult],
    quick: bool,
    requested_repetitions: u32,
) -> Result<(Vec<OpenLoopRateBasis>, Vec<ScenarioSpec>)> {
    let mut bases = Vec::new();
    let mut specs = Vec::new();
    for operation in [Operation::GetKnownSize, Operation::PutCreateNew] {
        let nfs_peak = measured_peak(results, operation, Backend::NfsCrust)?;
        let linux_peak = measured_peak(results, operation, Backend::LinuxRemote)?;
        let common_peak = nfs_peak.min(linux_peak);
        for fraction in [0.60, 0.85] {
            let equal_demand_rate = (common_peak * fraction).max(1.0);
            let nfs_relative_rate = (nfs_peak * fraction).max(1.0);
            let linux_relative_rate = (linux_peak * fraction).max(1.0);
            bases.push(OpenLoopRateBasis {
                operation,
                object_size_bytes: 128 * 1024,
                nfs_crust_peak_ops_per_sec: nfs_peak,
                linux_remote_peak_ops_per_sec: linux_peak,
                load_fraction: fraction,
                equal_demand_ops_per_sec: equal_demand_rate,
                nfs_crust_equal_utilization_ops_per_sec: nfs_relative_rate,
                linux_remote_equal_utilization_ops_per_sec: linux_relative_rate,
            });
            let repetitions = if quick { 1 } else { requested_repetitions };
            for repetition in 1..=repetitions {
                let backends = if repetition.is_multiple_of(2) {
                    [Backend::LinuxRemote, Backend::NfsCrust]
                } else {
                    [Backend::NfsCrust, Backend::LinuxRemote]
                };
                for backend in backends {
                    specs.push(ScenarioSpec::open_loop(
                        &format!("open-loop-equal-demand-{}pct", (fraction * 100.0) as u32),
                        backend,
                        operation,
                        128 * 1024,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        DEFAULT_SHARDS,
                        repetition,
                        if quick {
                            200
                        } else {
                            STANDARD_OPEN_LOOP_WARMUP_MS
                        },
                        open_loop_duration_ms(equal_demand_rate, quick),
                        equal_demand_rate,
                        open_loop_max_outstanding(quick),
                        OpenLoopLoadBasis::EqualDemand,
                    ));

                    let relative_rate = match backend {
                        Backend::NfsCrust => nfs_relative_rate,
                        Backend::LinuxRemote => linux_relative_rate,
                        _ => unreachable!("open-loop comparison only uses remote backends"),
                    };
                    specs.push(ScenarioSpec::open_loop(
                        &format!(
                            "open-loop-equal-utilization-{}pct",
                            (fraction * 100.0) as u32
                        ),
                        backend,
                        operation,
                        128 * 1024,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        DEFAULT_SHARDS,
                        repetition,
                        if quick {
                            200
                        } else {
                            STANDARD_OPEN_LOOP_WARMUP_MS
                        },
                        open_loop_duration_ms(relative_rate, quick),
                        relative_rate,
                        open_loop_max_outstanding(quick),
                        OpenLoopLoadBasis::EqualUtilization,
                    ));
                }
            }
        }
    }
    Ok((bases, specs))
}

fn open_loop_duration_ms(rate: f64, quick: bool) -> u64 {
    let duration_seconds = if quick {
        1.0
    } else {
        (STANDARD_OPEN_LOOP_TARGET_OPERATIONS / rate).clamp(
            STANDARD_OPEN_LOOP_MIN_SECONDS,
            STANDARD_OPEN_LOOP_MAX_SECONDS,
        )
    };
    (duration_seconds * 1_000.0).ceil() as u64
}

fn open_loop_max_outstanding(quick: bool) -> usize {
    if quick {
        QUICK_OPEN_LOOP_MAX_OUTSTANDING
    } else {
        STANDARD_OPEN_LOOP_MAX_OUTSTANDING
    }
}

fn measured_peak(
    results: &[ScenarioResult],
    operation: Operation,
    backend: Backend,
) -> Result<f64> {
    let mut by_concurrency: BTreeMap<usize, Vec<f64>> = BTreeMap::new();
    for result in results.iter().filter(|result| {
        result.spec.family == "concurrency-scaling"
            && result.spec.object_size_bytes == 128 * 1024
            && result.spec.operation == operation
            && result.spec.backend == backend
            && result.errors == 0
    }) {
        by_concurrency
            .entry(result.spec.concurrency)
            .or_default()
            .push(result.achieved_ops_per_sec);
    }
    let peak = by_concurrency
        .values()
        .filter_map(|values| repeated_rate_floor(values))
        .fold(0.0_f64, f64::max);
    if peak <= 0.0 {
        Err(format!(
            "could not derive open-loop rate for {} {}",
            backend.label(),
            operation.label()
        )
        .into())
    } else {
        Ok(peak)
    }
}

fn repeated_rate_floor(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::min)
}

fn elapsed_for_samples(samples: &[RawSample], minimum: Duration) -> Duration {
    let latest = samples
        .iter()
        .map(|sample| Duration::from_nanos(sample.completion_offset_ns))
        .max()
        .unwrap_or_default();
    latest.max(minimum)
}

fn scenario_mount_root(context: &BenchmarkContext, spec: &ScenarioSpec) -> PathBuf {
    context
        .benchmark_mount_root
        .join("scenarios")
        .join(&spec.id)
}

fn scenario_relative_root(context: &BenchmarkContext, spec: &ScenarioSpec) -> String {
    format!("{}/scenarios/{}", context.benchmark_relative_root, spec.id)
}

fn operation_relative_path(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    phase: Phase,
    worker: usize,
    sequence: u64,
) -> String {
    let root = scenario_relative_root(context, spec);
    match spec.operation {
        Operation::Get | Operation::GetKnownSize | Operation::GetRange4k => {
            let slot = sequence as usize % spec.read_files_per_worker;
            let shard = shard_for(spec, worker, slot as u64);
            format!("{root}/data/s{shard:02}/w{worker}-slot{slot}.bin")
        }
        Operation::PutCreateNew | Operation::Delete => {
            let shard = shard_for(spec, worker, sequence);
            format!(
                "{root}/{}/s{shard:02}/w{worker}-n{sequence}.bin",
                phase.label()
            )
        }
        Operation::PutOverwrite => {
            let slot = sequence as usize % spec.read_files_per_worker;
            let shard = shard_for(spec, worker, slot as u64);
            format!(
                "{root}/{}/s{shard:02}/w{worker}-slot{slot}.bin",
                phase.label()
            )
        }
        Operation::EntryInfo => format!("{root}/data/s00/metadata.bin"),
        Operation::List100 => format!("{root}/list"),
    }
}

fn operation_mount_path(
    context: &BenchmarkContext,
    spec: &ScenarioSpec,
    phase: Phase,
    worker: usize,
    sequence: u64,
) -> PathBuf {
    context.mount_root.join(operation_relative_path(
        context, spec, phase, worker, sequence,
    ))
}

fn shard_for(spec: &ScenarioSpec, worker: usize, sequence: u64) -> usize {
    (worker.wrapping_mul(17).wrapping_add(sequence as usize)) % spec.directory_shards.max(1)
}

fn payload(context: &BenchmarkContext, size: u64) -> Result<Bytes> {
    context
        .payloads
        .get(&size)
        .cloned()
        .ok_or_else(|| format!("no payload configured for {size} bytes").into())
}

fn diagnostic_write_specs(sizes: &[u64]) -> Vec<ScenarioSpec> {
    let mut repetitions = BTreeMap::<u64, u32>::new();
    sizes
        .iter()
        .flat_map(|size| {
            let repetition = repetitions.entry(*size).or_default();
            *repetition += 1;
            [Operation::PutCreateNew, Operation::PutOverwrite].map(|operation| {
                ScenarioSpec::closed_loop(
                    "diagnostic-write-size",
                    Backend::NfsCrust,
                    operation,
                    *size,
                    1,
                    1,
                    DEFAULT_READ_GRANULARITY,
                    DEFAULT_SHARDS,
                    *repetition,
                    0,
                    3_000,
                )
            })
        })
        .collect()
}

fn build_payloads(seed: u64, additional_sizes: &[u64]) -> BTreeMap<u64, Bytes> {
    let mut sizes = BTreeSet::from([
        4 * 1024_u64,
        32 * 1024,
        128 * 1024,
        512 * 1024,
        MIB,
        2 * MIB,
    ]);
    sizes.extend(additional_sizes.iter().copied());
    sizes
        .into_iter()
        .map(|size| {
            let body = (0..size)
                .map(|index| {
                    let mixed = index
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .rotate_left((index % 63) as u32)
                        ^ seed;
                    (mixed ^ (mixed >> 17) ^ (mixed >> 41)) as u8
                })
                .collect::<Vec<_>>();
            (size, Bytes::from(body))
        })
        .collect()
}

fn sentinel_payload(payload: &[u8]) -> Vec<u8> {
    payload.iter().map(|byte| byte ^ 0xff).collect()
}

fn range_offset(object_size: u64) -> u64 {
    let midpoint = object_size / 2;
    let aligned = midpoint / RANGE_BYTES as u64 * RANGE_BYTES as u64;
    aligned.min(object_size.saturating_sub(RANGE_BYTES as u64))
}

fn safe_component(value: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 80
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(
            "run id must contain 1-80 ASCII letters, digits, hyphens, or underscores".into(),
        );
    }
    Ok(value.to_owned())
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn sanitize_error(error: &str) -> String {
    error
        .chars()
        .map(|character| {
            if character == '\n' || character == '\r' {
                ' '
            } else {
                character
            }
        })
        .take(512)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_create_new_test_context() -> BenchmarkContext {
        BenchmarkContext {
            endpoint: String::new(),
            tls_server_name: "localhost".to_owned(),
            export: "/".to_owned(),
            mount_root: PathBuf::new(),
            benchmark_relative_root: "bench".to_owned(),
            benchmark_mount_root: PathBuf::new(),
            clients: Arc::new(Vec::new()),
            payloads: Arc::new(BTreeMap::from([(
                4 * 1024,
                Bytes::from(vec![0x5a; 4 * 1024]),
            )])),
            remote_read_mode: RemoteReadMode::FadviseDontNeed,
            operation_timeout: Duration::from_secs(1),
            keep_data: false,
        }
    }

    fn put_create_new_test_spec() -> ScenarioSpec {
        ScenarioSpec::closed_loop(
            "test",
            Backend::NfsCrust,
            Operation::PutCreateNew,
            4 * 1024,
            1,
            1,
            DEFAULT_READ_GRANULARITY,
            1,
            1,
            0,
            1,
        )
    }

    #[test]
    fn payload_is_deterministic_and_nontrivial() {
        let first = build_payloads(42, &[]);
        let second = build_payloads(42, &[]);
        assert_eq!(first, second);
        assert_ne!(&first[&(4 * 1024)][..16], &[0_u8; 16]);
    }

    #[test]
    fn diagnostic_write_specs_preserve_requested_sizes() {
        let specs = diagnostic_write_specs(&[448 * 1024, 512 * 1024, 512 * 1024]);
        assert_eq!(specs.len(), 6);
        assert_eq!(specs[0].object_size_bytes, 448 * 1024);
        assert_eq!(specs[1].object_size_bytes, 448 * 1024);
        assert_eq!(specs[2].object_size_bytes, 512 * 1024);
        assert_eq!(specs[3].object_size_bytes, 512 * 1024);
        assert_eq!(specs[4].object_size_bytes, 512 * 1024);
        assert_eq!(specs[5].object_size_bytes, 512 * 1024);
        assert_eq!(specs[0].operation, Operation::PutCreateNew);
        assert_eq!(specs[1].operation, Operation::PutOverwrite);
        assert_eq!(specs[0].repetition, 1);
        assert_eq!(specs[2].repetition, 1);
        assert_eq!(specs[4].repetition, 2);
        assert!(specs.iter().all(|spec| {
            spec.family == "diagnostic-write-size"
                && spec.backend == Backend::NfsCrust
                && spec.concurrency == 1
                && spec.warmup_ms == 0
                && spec.duration_ms == 3_000
        }));
    }

    #[test]
    fn full_validation_catches_bytes_outside_the_timed_sample() {
        let expected = vec![0_u8; 128];
        let mut actual = expected.clone();
        actual[17] = 1;

        assert!(validate_sampled_body(&actual, &expected).is_ok());
        assert!(validate_full_body(&actual, &expected).is_err());
    }

    #[test]
    fn safe_components_reject_empty_values() {
        assert!(safe_component("***").is_err());
        assert!(safe_component("run:one").is_err());
        assert_eq!(safe_component("run-one").unwrap(), "run-one");
    }

    #[test]
    fn range_is_aligned_and_in_bounds() {
        let offset = range_offset(MIB);
        assert_eq!(offset % RANGE_BYTES as u64, 0);
        assert!(offset + RANGE_BYTES as u64 <= MIB);
    }

    #[test]
    fn standard_open_loop_duration_is_bounded() {
        assert_eq!(open_loop_duration_ms(10_000.0, false), 5_000);
        assert_eq!(open_loop_duration_ms(500.0, false), 10_000);
        assert_eq!(open_loop_duration_ms(1.0, false), 30_000);
        assert_eq!(open_loop_duration_ms(1.0, true), 1_000);
    }

    #[test]
    fn standard_open_loop_capacity_has_tail_headroom() {
        assert_eq!(open_loop_max_outstanding(false), 1024);
        assert_eq!(open_loop_max_outstanding(true), 32);
    }

    #[test]
    fn open_loop_calibration_uses_repeated_rate_floor() {
        assert_eq!(repeated_rate_floor(&[751.0, 830.0]), Some(751.0));
        assert_eq!(repeated_rate_floor(&[]), None);
    }

    #[test]
    fn warmup_issue_preserves_the_exact_error() {
        let issue = batch_issue(
            &SampleBatch {
                samples: vec![RawSample {
                    scenario_id: "warmup-test".to_owned(),
                    worker: 0,
                    sequence: 0,
                    completion_offset_ns: 0,
                    scheduled_offset_ns: None,
                    queue_ns: 0,
                    service_ns: 0,
                    total_response_ns: 0,
                    status: "error",
                    reconciled_outcome_unknown: false,
                    error: Some("server returned NFS4ERR_DELAY".to_owned()),
                }],
                elapsed: Duration::ZERO,
            },
            "warmup",
        )
        .unwrap();

        assert!(issue.contains("server returned NFS4ERR_DELAY"));
    }

    #[test]
    fn reconciled_create_new_is_a_success_and_is_counted() {
        let context = put_create_new_test_context();
        let spec = put_create_new_test_spec();
        let timing = SampleTiming {
            completion_offset: Duration::from_millis(3),
            scheduled_offset: None,
            queue: Duration::ZERO,
            service: Duration::from_millis(3),
            total: Duration::from_millis(3),
        };
        let reconciled = sample_from_result(
            &context,
            &spec,
            0,
            0,
            timing,
            Ok(OperationValue::ReconciledSize(4 * 1024)),
        );

        assert_eq!(reconciled.status, "ok");
        assert!(reconciled.reconciled_outcome_unknown);
        let result = summarize_scenario(
            1,
            spec,
            0,
            &SampleBatch {
                samples: vec![reconciled],
                elapsed: Duration::from_millis(3),
            },
            model::ResourceDelta::default(),
            "passed".to_owned(),
        );
        assert_eq!(result.successes, 1);
        assert_eq!(result.measurement_errors, 0);
        assert_eq!(result.errors, 0);
        assert_eq!(result.reconciled_outcomes, 1);
    }

    #[test]
    fn raw_sample_serialization_omits_false_reconciliation_marker() {
        let context = put_create_new_test_context();
        let spec = put_create_new_test_spec();
        let normal = sample_from_result(
            &context,
            &spec,
            0,
            0,
            SampleTiming {
                completion_offset: Duration::ZERO,
                scheduled_offset: None,
                queue: Duration::ZERO,
                service: Duration::ZERO,
                total: Duration::ZERO,
            },
            Ok(OperationValue::Size(4 * 1024)),
        );
        let serialized = serde_json::to_value(normal).unwrap();
        assert!(serialized.get("reconciled_outcome_unknown").is_none());
    }

    #[test]
    fn failed_reconciliation_is_not_marked_as_successfully_reconciled() {
        let context = put_create_new_test_context();
        let spec = put_create_new_test_spec();
        let sample = sample_from_result(
            &context,
            &spec,
            0,
            0,
            SampleTiming {
                completion_offset: Duration::ZERO,
                scheduled_offset: None,
                queue: Duration::ZERO,
                service: Duration::ZERO,
                total: Duration::ZERO,
            },
            Ok(OperationValue::ReconciledSize(1)),
        );

        assert_eq!(sample.status, "error");
        assert!(!sample.reconciled_outcome_unknown);
    }
}
