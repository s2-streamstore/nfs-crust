# AWS EFS benchmark

The benchmark measures `nfs-crust` and the Linux NFSv4.1 client from the same
EC2 host against one same-AZ Regional EFS mount target. It records raw
per-operation latency and renders a self-contained HTML report.

## Workloads

- sequential read latency for 32 KiB, 128 KiB, 512 KiB, and 2 MiB objects
- sequential write latency for 32 KiB, 128 KiB, 512 KiB, and 2 MiB objects
- `get`, `get_known_size`, create-new publication, and overwrite
- concurrency scaling at 1, 8, and 32 workers
- fixed-rate reads and writes ramping through 60% to 85% of measured peak
- equal-demand and equal-utilization open-loop comparisons
- two repetitions in standard runs; five in explicit publication runs

Pass `--diagnostics` to add exploratory connection-count, directory-sharding,
read-granularity, working-set, Linux cache, trusted-size, range-read, metadata,
delete, and listing workloads. AWS runs expose the same switch as
`BENCH_DIAGNOSTICS=1`.

The Linux publication path matches library semantics: write and `fsync` a
unique temporary file, verify its size, then publish by hard link or rename.
Remote Linux reads require `O_DIRECT` for full runs. Open-loop response time
starts at scheduled arrival and therefore includes queueing.

Equal-demand runs use the slower backend's peak to represent one application
load offered to both implementations. Equal-utilization runs use each
backend's own peak. These answer different questions and are reported
separately.

Create-new uses a unique destination per operation. An
`Error::OutcomeUnknown` sample counts as successful only when that destination
matches the expected payload, or after confirmed absence and one successful
retry. The report exposes reconciled outcomes separately.

## Local run

The direct endpoint and existing mount must both use TLS and refer to the same
export:

```sh
cargo run --locked --release --manifest-path bench/Cargo.toml -- \
  --endpoint 10.0.0.10:2049 \
  --tls-server-name nfs.example.com \
  --mount-root /mnt/efs \
  --run-id local-check \
  --output-dir bench/work/local-check \
  --quick
```

Quick mode always uses one repetition. Standard runs default to two; pass
`--repetitions` locally or `BENCH_REPETITIONS` to the AWS runner to override it.
Standard closed-loop scenarios use 750 ms warmups and 3-4 second measurement
windows. Open-loop scenarios ramp through 60% before the reported 85% load and
target 5,000 arrivals with a 5-30 second adaptive window. Their rate uses the
lowest repeated throughput at each concurrency, then selects the best such
concurrency, so a single fast repetition cannot overstate the target.

For a targeted reproduction on the real benchmark host, set
`BENCH_CLOSED_LOOP_LIMIT=N` to run only the first `N` shuffled closed-loop
scenarios and skip the open-loop phase. Diagnostic runs are intentionally not
valid report inputs; use the same seed as the source run to replay its prefix.
Add `BENCH_WRITE_SIZE_SWEEP_KIB=448,480,...` to append direct nfs-crust
create-new scenarios at selected payload sizes, and shorten diagnostic failures
with `BENCH_OPERATION_TIMEOUT_SECONDS`. Standard runs leave both settings at
their defaults.

## Throwaway AWS run

Use a dedicated sandbox VPC and public subnet:

```sh
AWS_PROFILE=sandbox-profile \
AWS_REGION=us-east-1 \
BENCH_VPC_ID=vpc-0123456789abcdef0 \
BENCH_SUBNET_ID=subnet-0123456789abcdef0 \
bench/aws/run.sh
```

For a publication run with stronger within-run statistics, add
`BENCH_REPETITIONS=5`. The runner allows three hours for one or two
repetitions and eight hours for larger runs; credential and presigned-URL
requirements scale with that choice.

The runner requires a clean revision and a single-use `RUN_ID`. Both the
`nfs-crust` connection and Linux reference mount use TLS. It records resource
ownership under `.bench/state/<run-id>/resources.json`, downloads the durable,
check-in-ready run into `bench/results/<run-id>/`, and tears down the throwaway
resources.
Recover an interrupted teardown with:

```sh
AWS_PROFILE=sandbox-profile bench/aws/cleanup.sh \
  .bench/state/<run-id>/resources.json
```

Normal and recovery teardown use the same cleanup implementation and verify the
creator account, run token, and final absence of owned resources.

## Artifacts

The benchmark emits:

- `summary.json`: scenario definitions and aggregate distributions
- `raw-samples.jsonl`: service, queue, and response latency per operation
- `warmup-samples.jsonl`: untimed warmup operations, including exact errors
- `events.jsonl`: progress and correctness events

AWS runs retain raw samples, event logs, CloudWatch data, infrastructure
provenance, host diagnostics, checksums, and cleanup evidence under
`.bench/state/<run-id>/artifacts`. Only a sanitized `summary.json` and rendered
`report.fragment.html` are committed under `bench/results/<run-id>`.
Publication omits infrastructure fields and any values containing AWS resource
IDs, ARNs, account IDs, or IP addresses. The report never renders an exact
endpoint, and the publisher rejects a result if a covered identifier remains.
`summary.json` remains the authoritative structured result.

Render the HTML report with:

```sh
python3 bench/report/generate.py \
  --run-root .bench/state/<run-id>/artifacts \
  --fragment bench/results/<run-id>/report.fragment.html
```

The report uses repetition medians without pooling raw samples and shows
deterministic 95% bootstrap intervals plus observed ranges. p99 and p99.9 are
marked when a repetition has fewer than 1,000 or 10,000 successful samples,
respectively. Process CPU and host network traffic are normalized per successful
operation.

The Linux known-size reference performs a timed metadata check so it matches
the library's stale-size detection semantics.
