#!/usr/bin/env bash
set -Eeuo pipefail

: "${RUN_ID:?RUN_ID is required}"
: "${MOUNT_TARGET_IP:?MOUNT_TARGET_IP is required}"
: "${FILE_SYSTEM_ID:?FILE_SYSTEM_ID is required}"
: "${AWS_REGION:?AWS_REGION is required}"
: "${SOURCE_REVISION:?SOURCE_REVISION is required}"
: "${SOURCE_SHA256:?SOURCE_SHA256 is required}"
: "${SOURCE_DOWNLOAD_URL:?SOURCE_DOWNLOAD_URL is required}"
: "${RESULT_UPLOAD_URL:?RESULT_UPLOAD_URL is required}"
BENCH_QUICK="${BENCH_QUICK:-0}"
BENCH_REPETITIONS="${BENCH_REPETITIONS:-2}"
BENCH_DIAGNOSTICS="${BENCH_DIAGNOSTICS:-0}"
BENCH_CLOSED_LOOP_LIMIT="${BENCH_CLOSED_LOOP_LIMIT:-}"
BENCH_WRITE_SIZE_SWEEP_KIB="${BENCH_WRITE_SIZE_SWEEP_KIB:-}"
BENCH_OPERATION_TIMEOUT_SECONDS="${BENCH_OPERATION_TIMEOUT_SECONDS:-120}"
RUSTUP_VERSION="1.29.0"
RUSTUP_TARGET="aarch64-unknown-linux-gnu"
RUSTUP_INIT_SHA256="9732d6c5e2a098d3521fca8145d826ae0aaa067ef2385ead08e6feac88fa5792"
if [[ ! "${RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$ ]]; then
  echo "invalid RUN_ID" >&2
  exit 1
fi
if [[ "${BENCH_QUICK}" != 0 && "${BENCH_QUICK}" != 1 ]]; then
  echo "BENCH_QUICK must be 0 or 1" >&2
  exit 1
fi
if [[ "${BENCH_DIAGNOSTICS}" != 0 && "${BENCH_DIAGNOSTICS}" != 1 ]]; then
  echo "BENCH_DIAGNOSTICS must be 0 or 1" >&2
  exit 1
fi
if [[ ! "${BENCH_REPETITIONS}" =~ ^[1-9][0-9]*$ ]] || (( BENCH_REPETITIONS > 20 )); then
  echo "BENCH_REPETITIONS must be between 1 and 20" >&2
  exit 1
fi
if [[ -n "${BENCH_CLOSED_LOOP_LIMIT}" ]] && \
    { [[ ! "${BENCH_CLOSED_LOOP_LIMIT}" =~ ^[1-9][0-9]*$ ]] || (( BENCH_CLOSED_LOOP_LIMIT > 180 )); }; then
  echo "BENCH_CLOSED_LOOP_LIMIT must be empty or between 1 and 180" >&2
  exit 1
fi
if [[ -n "${BENCH_WRITE_SIZE_SWEEP_KIB}" ]] && \
    [[ ! "${BENCH_WRITE_SIZE_SWEEP_KIB}" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]]; then
  echo "BENCH_WRITE_SIZE_SWEEP_KIB must be empty or a comma-separated integer list" >&2
  exit 1
fi
if [[ ! "${BENCH_OPERATION_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]] || \
    (( BENCH_OPERATION_TIMEOUT_SECONDS > 600 )); then
  echo "BENCH_OPERATION_TIMEOUT_SECONDS must be between 1 and 600" >&2
  exit 1
fi

WORK_ROOT="/opt/nfs-crust-benchmark"
SOURCE_ROOT="${WORK_ROOT}/source"
RESULT_ROOT="/var/tmp/nfs-crust-benchmark-results"
MOUNT_ROOT="/mnt/efs"

cleanup() {
  mountpoint -q "${MOUNT_ROOT}" && umount "${MOUNT_ROOT}" || true
}
trap cleanup EXIT

dnf install -y \
  gcc \
  gcc-c++ \
  amazon-efs-utils \
  gzip \
  jq \
  make \
  nfs-utils \
  openssl-devel \
  pkgconf-pkg-config \
  tar

rm -rf "${WORK_ROOT}" "${RESULT_ROOT}"
mkdir -p "${SOURCE_ROOT}" "${RESULT_ROOT}/host" "${MOUNT_ROOT}"
curl --fail --silent --show-error --location \
  "${SOURCE_DOWNLOAD_URL}" --output "${WORK_ROOT}/source.tar.gz"
printf '%s  %s\n' "${SOURCE_SHA256}" "${WORK_ROOT}/source.tar.gz" | sha256sum -c -
tar -xzf "${WORK_ROOT}/source.tar.gz" -C "${SOURCE_ROOT}"
chown -R ec2-user:ec2-user "${SOURCE_ROOT}" "${RESULT_ROOT}"

TLS_SERVER_NAME="${FILE_SYSTEM_ID}.efs.${AWS_REGION}.amazonaws.com"
mount -t efs \
  -o tls,mounttargetip="${MOUNT_TARGET_IP}",noresvport,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,actimeo=0,lookupcache=none \
  "${FILE_SYSTEM_ID}:/" "${MOUNT_ROOT}"
read -r mounted_type mounted_source < <(findmnt -n -o FSTYPE,SOURCE --target "${MOUNT_ROOT}")
if [[ "${mounted_type}" != nfs4 || "${mounted_source}" == "${MOUNT_TARGET_IP}:/" ]]; then
  echo "unexpected mount: type=${mounted_type}, source=${mounted_source}" >&2
  exit 1
fi
if ! pgrep -af 'efs-proxy|stunnel' > "${RESULT_ROOT}/host/efs-tls-process.txt"; then
  echo "EFS mount helper did not leave a TLS proxy process" >&2
  exit 1
fi
chmod 0777 "${MOUNT_ROOT}"

uname -a > "${RESULT_ROOT}/host/uname.txt"
cp /etc/os-release "${RESULT_ROOT}/host/os-release.txt"
lscpu --json > "${RESULT_ROOT}/host/lscpu.json"
free -b > "${RESULT_ROOT}/host/free.txt"
nfsstat -m > "${RESULT_ROOT}/host/nfsstat-m.txt"
grep " ${MOUNT_ROOT} " /proc/mounts > "${RESULT_ROOT}/host/proc-mounts.txt"
sysctl -a 2>/dev/null | grep '^sunrpc\|^fs.nfs' > "${RESULT_ROOT}/host/nfs-sysctls.txt" || true

curl --fail --silent --show-error --location \
  "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${RUSTUP_TARGET}/rustup-init" \
  --output /tmp/rustup-init
printf '%s  %s\n' "${RUSTUP_INIT_SHA256}" /tmp/rustup-init | sha256sum --check
chmod 0755 /tmp/rustup-init
runuser -u ec2-user -- env HOME=/home/ec2-user \
  /tmp/rustup-init -y --profile minimal --default-toolchain 1.97.0 --no-modify-path
rm -f /tmp/rustup-init
runuser -u ec2-user -- bash -lc \
  "cd '${SOURCE_ROOT}' && ~/.cargo/bin/cargo build --locked --release --manifest-path bench/Cargo.toml" \
  2>&1 | tee "${RESULT_ROOT}/build.log"
runuser -u ec2-user -- bash -lc "~/.cargo/bin/rustc -Vv" > "${RESULT_ROOT}/host/rustc.txt"
runuser -u ec2-user -- bash -lc "~/.cargo/bin/cargo -V" > "${RESULT_ROOT}/host/cargo.txt"

ss -s > "${RESULT_ROOT}/host/ss-before.txt" || true
cp /proc/net/snmp "${RESULT_ROOT}/host/proc-net-snmp-before.txt"
if command -v nstat >/dev/null; then
  nstat -az > "${RESULT_ROOT}/host/nstat-before.txt"
fi

benchmark_status=0
quick_arg=""
[[ "${BENCH_QUICK}" == 1 ]] && quick_arg="--quick"
diagnostics_arg=""
[[ "${BENCH_DIAGNOSTICS}" == 1 ]] && diagnostics_arg="--diagnostics"
closed_loop_limit_arg=""
[[ -n "${BENCH_CLOSED_LOOP_LIMIT}" ]] && closed_loop_limit_arg="--closed-loop-limit '${BENCH_CLOSED_LOOP_LIMIT}'"
write_size_sweep_arg=""
[[ -n "${BENCH_WRITE_SIZE_SWEEP_KIB}" ]] && write_size_sweep_arg="--write-size-sweep-kib '${BENCH_WRITE_SIZE_SWEEP_KIB}'"
set +e
runuser -u ec2-user -- bash -lc \
  "cd '${SOURCE_ROOT}' && ./target/release/nfs-crust-bench \
    --endpoint '${MOUNT_TARGET_IP}:2049' \
    --tls-server-name '${TLS_SERVER_NAME}' \
    --export / \
    --mount-root '${MOUNT_ROOT}' \
    --run-id '${RUN_ID}' \
    --output-dir '${RESULT_ROOT}/run' \
    --source-revision '${SOURCE_REVISION}' \
    --repetitions '${BENCH_REPETITIONS}' \
    --operation-timeout-seconds '${BENCH_OPERATION_TIMEOUT_SECONDS}' \
    ${quick_arg} ${diagnostics_arg} ${closed_loop_limit_arg} ${write_size_sweep_arg}" \
  2>&1 | tee "${RESULT_ROOT}/benchmark.log"
pipeline_status=("${PIPESTATUS[@]}")
set -e
benchmark_status="${pipeline_status[0]}"
if [[ "${pipeline_status[1]}" != 0 ]]; then
  echo "failed to persist benchmark output" >&2
  exit "${pipeline_status[1]}"
fi

ss -s > "${RESULT_ROOT}/host/ss-after.txt" || true
cp /proc/net/snmp "${RESULT_ROOT}/host/proc-net-snmp-after.txt"
if command -v nstat >/dev/null; then
  nstat -az > "${RESULT_ROOT}/host/nstat-after.txt"
fi

for samples in raw-samples warmup-samples; do
  if [[ -f "${RESULT_ROOT}/run/${samples}.jsonl" ]]; then
    gzip -9 "${RESULT_ROOT}/run/${samples}.jsonl"
  fi
done

jq -n \
  --arg run_id "${RUN_ID}" \
  --arg source_revision "${SOURCE_REVISION}" \
  --arg mount_target_ip "${MOUNT_TARGET_IP}" \
  --arg tls_server_name "${TLS_SERVER_NAME}" \
  --argjson benchmark_exit_code "${benchmark_status}" \
  --arg diagnostic_closed_loop_limit "${BENCH_CLOSED_LOOP_LIMIT}" \
  --argjson diagnostics "${BENCH_DIAGNOSTICS}" \
  --arg diagnostic_write_size_sweep_kib "${BENCH_WRITE_SIZE_SWEEP_KIB}" \
  --arg operation_timeout_seconds "${BENCH_OPERATION_TIMEOUT_SECONDS}" \
  '{
    run_id: $run_id,
    source_revision: $source_revision,
    benchmark_exit_code: $benchmark_exit_code,
    endpoint: ($mount_target_ip + ":2049"),
    tls_server_name: $tls_server_name,
    rpc_transport: "tls",
    linux_mount_transport: "tls",
    diagnostics: ($diagnostics == 1),
    diagnostic_closed_loop_limit: (if $diagnostic_closed_loop_limit == "" then null else ($diagnostic_closed_loop_limit | tonumber) end),
    diagnostic_write_size_sweep_kib: (if $diagnostic_write_size_sweep_kib == "" then [] else ($diagnostic_write_size_sweep_kib | split(",") | map(tonumber)) end),
    operation_timeout_seconds: ($operation_timeout_seconds | tonumber)
  }' > "${RESULT_ROOT}/remote-run.json"

(cd "${RESULT_ROOT}" && \
  find . -type f ! -name SHA256SUMS -print0 | sort -z | \
  xargs -0 sha256sum > SHA256SUMS)
tar -czf "${WORK_ROOT}/results.tar.gz" -C "${RESULT_ROOT}" .
curl --fail --silent --show-error --request PUT \
  --upload-file "${WORK_ROOT}/results.tar.gz" "${RESULT_UPLOAD_URL}"

exit "${benchmark_status}"
