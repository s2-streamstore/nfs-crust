#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE to an explicitly approved sandbox profile}"
AWS_REGION="${AWS_REGION:-$(aws configure get region --profile "${AWS_PROFILE}")}"
BENCH_VPC_ID="${BENCH_VPC_ID:?Set BENCH_VPC_ID to the approved benchmark VPC}"
BENCH_SUBNET_ID="${BENCH_SUBNET_ID:?Set BENCH_SUBNET_ID to an approved public subnet}"
RUN_ID="${RUN_ID:-efs-$(date -u +%Y%m%dT%H%M%SZ)-${RANDOM}}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c7g.xlarge}"
BENCH_QUICK="${BENCH_QUICK:-0}"
BENCH_REPETITIONS="${BENCH_REPETITIONS:-2}"
BENCH_DIAGNOSTICS="${BENCH_DIAGNOSTICS:-0}"
BENCH_CLOSED_LOOP_LIMIT="${BENCH_CLOSED_LOOP_LIMIT:-}"
BENCH_WRITE_SIZE_SWEEP_KIB="${BENCH_WRITE_SIZE_SWEEP_KIB:-}"
BENCH_OPERATION_TIMEOUT_SECONDS="${BENCH_OPERATION_TIMEOUT_SECONDS:-120}"
SOURCE_REVISION="$(git -C "${ROOT}" rev-parse HEAD)"
if [[ ! "${RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$ ]]; then
  echo "RUN_ID must contain 1-64 ASCII letters, digits, underscores, or hyphens" >&2
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
if (( BENCH_REPETITIONS <= 2 )); then
  REMOTE_EXECUTION_TIMEOUT_SECONDS=10800
else
  REMOTE_EXECUTION_TIMEOUT_SECONDS=28800
fi
MIN_TRANSFER_CREDENTIAL_VALIDITY_SECONDS="$((REMOTE_EXECUTION_TIMEOUT_SECONDS + 900))"
RESULT_ROOT="${ROOT}/bench/results/${RUN_ID}"
STATE_ROOT="${ROOT}/.bench/state/${RUN_ID}"
ARTIFACT_ROOT="${STATE_ROOT}/artifacts"
STATE_FILE="${STATE_ROOT}/resources.json"
if [[ -e "${RESULT_ROOT}" || -e "${STATE_ROOT}" ]]; then
  echo "refusing to reuse RUN_ID ${RUN_ID}; choose a new ID and retain prior evidence/recovery state" >&2
  echo "existing path: ${RESULT_ROOT} or ${STATE_ROOT}" >&2
  exit 1
fi

export AWS_PROFILE AWS_REGION

FS_ID=""
MOUNT_TARGET_ID=""
MOUNT_TARGET_IP=""
INSTANCE_ID=""
SECURITY_GROUP_ID=""
ROLE_NAME=""
INSTANCE_PROFILE_NAME=""
TRANSFER_BUCKET=""
ACCOUNT_ID=""
RUN_TOKEN="$(openssl rand -hex 16)"
FS_OWNED=0
FS_CREATE_ATTEMPTED=0
MOUNT_TARGET_OWNED=0
MOUNT_TARGET_CREATE_ATTEMPTED=0
INSTANCE_OWNED=0
INSTANCE_CREATE_ATTEMPTED=0
SECURITY_GROUP_OWNED=0
SECURITY_GROUP_CREATE_ATTEMPTED=0
ROLE_OWNED=0
ROLE_CREATE_ATTEMPTED=0
INSTANCE_PROFILE_OWNED=0
INSTANCE_PROFILE_CREATE_ATTEMPTED=0
TRANSFER_BUCKET_OWNED=0
TRANSFER_BUCKET_CREATE_ATTEMPTED=0
CLEANED=0

persist_state() {
  jq -n \
    --arg run_id "${RUN_ID}" \
    --arg run_token "${RUN_TOKEN}" \
    --arg account_id "${ACCOUNT_ID}" \
    --arg region "${AWS_REGION}" \
    --arg vpc_id "${BENCH_VPC_ID}" \
    --arg fs_id "${FS_ID}" \
    --arg mount_target_id "${MOUNT_TARGET_ID}" \
    --arg instance_id "${INSTANCE_ID}" \
    --arg security_group_id "${SECURITY_GROUP_ID}" \
    --arg role_name "${ROLE_NAME}" \
    --arg instance_profile_name "${INSTANCE_PROFILE_NAME}" \
    --arg transfer_bucket "${TRANSFER_BUCKET}" \
    --argjson file_system_owned "${FS_OWNED}" \
    --argjson file_system_create_attempted "${FS_CREATE_ATTEMPTED}" \
    --argjson mount_target_create_attempted "${MOUNT_TARGET_CREATE_ATTEMPTED}" \
    --argjson instance_create_attempted "${INSTANCE_CREATE_ATTEMPTED}" \
    --argjson security_group_create_attempted "${SECURITY_GROUP_CREATE_ATTEMPTED}" \
    --argjson role_create_attempted "${ROLE_CREATE_ATTEMPTED}" \
    --argjson instance_profile_create_attempted "${INSTANCE_PROFILE_CREATE_ATTEMPTED}" \
    --argjson transfer_bucket_create_attempted "${TRANSFER_BUCKET_CREATE_ATTEMPTED}" \
    --argjson mount_target_owned "${MOUNT_TARGET_OWNED}" \
    --argjson instance_owned "${INSTANCE_OWNED}" \
    --argjson security_group_owned "${SECURITY_GROUP_OWNED}" \
    --argjson role_owned "${ROLE_OWNED}" \
    --argjson instance_profile_owned "${INSTANCE_PROFILE_OWNED}" \
    --argjson transfer_bucket_owned "${TRANSFER_BUCKET_OWNED}" \
    '{run_id:$run_id,run_token:$run_token,account_id:$account_id,region:$region,vpc_id:$vpc_id,file_system_id:$fs_id,mount_target_id:$mount_target_id,instance_id:$instance_id,security_group_id:$security_group_id,role_name:$role_name,instance_profile_name:$instance_profile_name,transfer_bucket:$transfer_bucket,attempted:{file_system:($file_system_create_attempted == 1),mount_target:($mount_target_create_attempted == 1),instance:($instance_create_attempted == 1),security_group:($security_group_create_attempted == 1),role:($role_create_attempted == 1),instance_profile:($instance_profile_create_attempted == 1),transfer_bucket:($transfer_bucket_create_attempted == 1)},owned:{file_system:($file_system_owned == 1),mount_target:($mount_target_owned == 1),instance:($instance_owned == 1),security_group:($security_group_owned == 1),role:($role_owned == 1),instance_profile:($instance_profile_owned == 1),transfer_bucket:($transfer_bucket_owned == 1)}}' \
    > "${STATE_FILE}.partial"
  mv "${STATE_FILE}.partial" "${STATE_FILE}"
  jq -e '
    (.run_token | type == "string") and
    (.account_id | type == "string") and
    ([.attempted[] | type == "boolean"] | all) and
    ([.owned[] | type == "boolean"] | all)
  ' "${STATE_FILE}" >/dev/null
}

wait_for_file_system_available() {
  local state=""
  for _ in $(seq 1 120); do
    state="$(aws efs describe-file-systems --file-system-id "$1" \
      --query 'FileSystems[0].LifeCycleState' --output text)"
    [[ "${state}" == available ]] && return 0
    [[ "${state}" == creating ]] || break
    sleep 2
  done
  echo "EFS file system $1 did not become available; final state: ${state}" >&2
  return 1
}

wait_for_mount_target_available() {
  local state=""
  for _ in $(seq 1 120); do
    state="$(aws efs describe-mount-targets --mount-target-id "$1" \
      --query 'MountTargets[0].LifeCycleState' --output text)"
    [[ "${state}" == available ]] && return 0
    [[ "${state}" == creating ]] || break
    sleep 2
  done
  echo "EFS mount target $1 did not become available; final state: ${state}" >&2
  return 1
}

require_transfer_credential_lifetime() {
  local minimum_seconds=$1 remaining_seconds
  remaining_seconds="$(
    aws configure export-credentials --profile "${AWS_PROFILE}" --format process | \
      python3 -c '
import datetime, json, sys
credentials = json.load(sys.stdin)
expiration = credentials.get("Expiration")
if expiration is None:
    print(2**31 - 1)
else:
    deadline = datetime.datetime.fromisoformat(expiration.replace("Z", "+00:00"))
    now = datetime.datetime.now(datetime.timezone.utc)
    print(max(0, int((deadline - now).total_seconds())))
'
  )"
  if (( remaining_seconds < minimum_seconds )); then
    echo "AWS credentials expire in ${remaining_seconds}s; benchmark transfer requires at least ${minimum_seconds}s" >&2
    return 1
  fi
}

cleanup() {
  local original_status=$?
  trap - EXIT
  if [[ "${CLEANED}" == 1 ]]; then
    exit "${original_status}"
  fi
  CLEANED=1
  persist_state
  if ! python3 "${ROOT}/bench/aws/cleanup.py" "${STATE_FILE}" \
      --result "${ARTIFACT_ROOT}/cleanup.json"; then
    echo "resource cleanup verification failed; inspect ${STATE_FILE}" >&2
    exit 1
  fi
  if ! find "${ARTIFACT_ROOT}" -type f ! -name SHA256SUMS -print0 | sort -z | \
      xargs -0 shasum -a 256 > "${STATE_ROOT}/SHA256SUMS.partial" || \
      ! mv "${STATE_ROOT}/SHA256SUMS.partial" "${ARTIFACT_ROOT}/SHA256SUMS"; then
    rm -f "${STATE_ROOT}/SHA256SUMS.partial"
    echo "failed to write run checksums" >&2
    exit 1
  fi
  if [[ -s "${ARTIFACT_ROOT}/run/run/summary.json" ]] && \
      ! python3 "${ROOT}/bench/aws/publish_results.py" \
        --artifact-root "${ARTIFACT_ROOT}" --result-root "${RESULT_ROOT}"; then
    echo "failed to publish review-facing results from ${ARTIFACT_ROOT}" >&2
    exit 1
  fi
  exit "${original_status}"
}

if [[ -n "$(git -C "${ROOT}" status --porcelain)" ]]; then
  echo "refusing to benchmark a dirty tree; commit the exact source first" >&2
  exit 1
fi
require_transfer_credential_lifetime "${MIN_TRANSFER_CREDENTIAL_VALIDITY_SECONDS}"

IDENTITY_JSON="$(aws sts get-caller-identity)"
ACCOUNT_ID="$(jq -r .Account <<<"${IDENTITY_JSON}")"
VPC_ACTUAL="$(aws ec2 describe-subnets --subnet-ids "${BENCH_SUBNET_ID}" --query 'Subnets[0].VpcId' --output text)"
PUBLIC_IP_ON_LAUNCH="$(aws ec2 describe-subnets --subnet-ids "${BENCH_SUBNET_ID}" --query 'Subnets[0].MapPublicIpOnLaunch' --output text)"
if [[ "${VPC_ACTUAL}" != "${BENCH_VPC_ID}" || "${PUBLIC_IP_ON_LAUNCH}" != "True" ]]; then
  echo "subnet is not the approved public subnet in the requested VPC" >&2
  exit 1
fi

IAM_NAME_HASH="$(printf '%s' "${RUN_ID}:${RUN_TOKEN}" | shasum -a 256 | awk '{print substr($1, 1, 16)}')"
ROLE_NAME="nfs-crust-bench-${RUN_ID:0:30}-${IAM_NAME_HASH}"
INSTANCE_PROFILE_NAME="${ROLE_NAME}"
TRANSFER_BUCKET="nfs-crust-bench-${ACCOUNT_ID}-${RUN_TOKEN}"
SECURITY_GROUP_NAME="nfs-crust-bench-${RUN_ID}"

existing_fs_count="$(aws efs describe-file-systems --creation-token "${RUN_ID}" \
  --query 'length(FileSystems)' --output text)"
existing_instance_count="$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=nfs-crust" "Name=tag:Purpose,Values=benchmark" \
    "Name=tag:RunId,Values=${RUN_ID}" \
    "Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped" \
  --query 'length(Reservations[].Instances[])' --output text)"
existing_volume_count="$(aws ec2 describe-volumes \
  --filters "Name=tag:Project,Values=nfs-crust" "Name=tag:Purpose,Values=benchmark" \
    "Name=tag:RunId,Values=${RUN_ID}" \
  --query 'length(Volumes)' --output text)"
existing_sg_count="$(aws ec2 describe-security-groups \
  --filters "Name=vpc-id,Values=${BENCH_VPC_ID}" "Name=group-name,Values=${SECURITY_GROUP_NAME}" \
  --query 'length(SecurityGroups)' --output text)"
existing_role_count="$(aws iam list-roles \
  --query "length(Roles[?RoleName=='${ROLE_NAME}'])" --output text)"
existing_profile_count="$(aws iam list-instance-profiles \
  --query "length(InstanceProfiles[?InstanceProfileName=='${INSTANCE_PROFILE_NAME}'])" \
  --output text)"
existing_bucket_count="$(aws s3api list-buckets \
  --query "length(Buckets[?Name=='${TRANSFER_BUCKET}'])" --output text)"
if [[ "${existing_fs_count}" != 0 || "${existing_instance_count}" != 0 || \
      "${existing_volume_count}" != 0 || "${existing_sg_count}" != 0 || \
      "${existing_role_count}" != 0 || "${existing_profile_count}" != 0 || \
      "${existing_bucket_count}" != 0 ]]; then
  echo "refusing to adopt pre-existing AWS resources for RUN_ID ${RUN_ID}" >&2
  exit 1
fi

AMI_ID="$(aws ssm get-parameter \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
  --query Parameter.Value --output text)"
ROOT_DEVICE_NAME="$(aws ec2 describe-images --image-ids "${AMI_ID}" --query 'Images[0].RootDeviceName' --output text)"
BLOCK_DEVICE_MAPPINGS="$(jq -cn --arg device "${ROOT_DEVICE_NAME}" '[{DeviceName:$device,Ebs:{VolumeSize:30,VolumeType:"gp3",Encrypted:true,DeleteOnTermination:true}}]')"
mkdir -p "${ROOT}/bench/results" "${ROOT}/.bench/state"
if ! mkdir "${RESULT_ROOT}"; then
  echo "RUN_ID ${RUN_ID} was claimed by another process during preflight" >&2
  exit 1
fi
if ! mkdir "${STATE_ROOT}"; then
  rmdir "${RESULT_ROOT}"
  echo "RUN_ID ${RUN_ID} recovery state was claimed by another process during preflight" >&2
  exit 1
fi
mkdir -p "${ARTIFACT_ROOT}/provenance" "${ARTIFACT_ROOT}/cloudwatch"
persist_state
trap cleanup EXIT

echo "benchmark run ${RUN_ID} in account ${ACCOUNT_ID}, region ${AWS_REGION}"
git -C "${ROOT}" archive --format=tar.gz -o "${STATE_ROOT}/source.tar.gz" HEAD
SOURCE_SHA256="$(shasum -a 256 "${STATE_ROOT}/source.tar.gz" | awk '{print $1}')"

TRANSFER_BUCKET_CREATE_ATTEMPTED=1
persist_state
if [[ "${AWS_REGION}" == us-east-1 ]]; then
  aws s3api create-bucket --bucket "${TRANSFER_BUCKET}" >/dev/null
else
  aws s3api create-bucket --bucket "${TRANSFER_BUCKET}" \
    --create-bucket-configuration "LocationConstraint=${AWS_REGION}" >/dev/null
fi
TRANSFER_BUCKET_OWNED=1
persist_state
aws s3api put-public-access-block --bucket "${TRANSFER_BUCKET}" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-tagging --bucket "${TRANSFER_BUCKET}" \
  --tagging "TagSet=[{Key=Project,Value=nfs-crust},{Key=Purpose,Value=benchmark},{Key=RunId,Value=${RUN_ID}},{Key=RunToken,Value=${RUN_TOKEN}}]"
aws s3api put-bucket-encryption --bucket "${TRANSFER_BUCKET}" \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":false}]}'
aws s3 cp "${STATE_ROOT}/source.tar.gz" "s3://${TRANSFER_BUCKET}/source.tar.gz" --only-show-errors
aws s3 cp "${ROOT}/bench/aws/remote-run.sh" "s3://${TRANSFER_BUCKET}/remote-run.sh" --only-show-errors

ROLE_CREATE_ATTEMPTED=1
persist_state
set +e
aws iam create-role --role-name "${ROLE_NAME}" \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  --tags Key=Project,Value=nfs-crust Key=Purpose,Value=benchmark Key=RunId,Value="${RUN_ID}" Key=RunToken,Value="${RUN_TOKEN}" >/dev/null
create_role_status=$?
set -e
if [[ "${create_role_status}" != 0 && \
      "$(aws iam list-role-tags --role-name "${ROLE_NAME}" \
        --query "length(Tags[?Key=='RunToken' && Value=='${RUN_TOKEN}'])" --output text 2>/dev/null || true)" != 1 ]]; then
  exit "${create_role_status}"
fi
ROLE_OWNED=1
persist_state
aws iam attach-role-policy --role-name "${ROLE_NAME}" \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
INSTANCE_PROFILE_CREATE_ATTEMPTED=1
persist_state
set +e
aws iam create-instance-profile --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
  --tags Key=Project,Value=nfs-crust Key=Purpose,Value=benchmark Key=RunId,Value="${RUN_ID}" Key=RunToken,Value="${RUN_TOKEN}" >/dev/null
create_profile_status=$?
set -e
if [[ "${create_profile_status}" != 0 && \
      "$(aws iam list-instance-profile-tags --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
        --query "length(Tags[?Key=='RunToken' && Value=='${RUN_TOKEN}'])" --output text 2>/dev/null || true)" != 1 ]]; then
  exit "${create_profile_status}"
fi
INSTANCE_PROFILE_OWNED=1
persist_state
aws iam add-role-to-instance-profile \
  --instance-profile-name "${INSTANCE_PROFILE_NAME}" --role-name "${ROLE_NAME}"
sleep 12

SECURITY_GROUP_CREATE_ATTEMPTED=1
persist_state
set +e
SECURITY_GROUP_ID="$(aws ec2 create-security-group \
  --group-name "${SECURITY_GROUP_NAME}" \
  --description "Throwaway same-AZ EFS benchmark" \
  --vpc-id "${BENCH_VPC_ID}" \
  --tag-specifications \
    "ResourceType=security-group,Tags=[{Key=Name,Value=${SECURITY_GROUP_NAME}},{Key=Project,Value=nfs-crust},{Key=Purpose,Value=benchmark},{Key=RunId,Value=${RUN_ID}},{Key=RunToken,Value=${RUN_TOKEN}}]" \
  --query GroupId --output text)"
create_sg_status=$?
set -e
if [[ "${create_sg_status}" != 0 ]]; then
  SECURITY_GROUP_ID="$(aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=${BENCH_VPC_ID}" "Name=group-name,Values=${SECURITY_GROUP_NAME}" \
      "Name=tag:RunToken,Values=${RUN_TOKEN}" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
  if [[ -z "${SECURITY_GROUP_ID}" || "${SECURITY_GROUP_ID}" == None ]]; then
    exit "${create_sg_status}"
  fi
fi
SECURITY_GROUP_OWNED=1
persist_state
aws ec2 authorize-security-group-ingress --group-id "${SECURITY_GROUP_ID}" \
  --ip-permissions "IpProtocol=tcp,FromPort=2049,ToPort=2049,UserIdGroupPairs=[{GroupId=${SECURITY_GROUP_ID}}]" >/dev/null

FS_CREATE_ATTEMPTED=1
persist_state
set +e
FS_ID="$(aws efs create-file-system \
  --creation-token "${RUN_ID}" \
  --performance-mode generalPurpose \
  --throughput-mode elastic \
  --encrypted \
  --no-backup \
  --tags Key=Name,Value="nfs-crust-bench-${RUN_ID}" Key=Project,Value=nfs-crust Key=Purpose,Value=benchmark Key=RunId,Value="${RUN_ID}" Key=RunToken,Value="${RUN_TOKEN}" \
  --query FileSystemId --output text)"
create_fs_status=$?
set -e
if [[ "${create_fs_status}" != 0 ]]; then
  FS_ID=""
  for _ in $(seq 1 15); do
    FS_ID="$(aws efs describe-file-systems --creation-token "${RUN_ID}" \
      --query 'FileSystems[0].FileSystemId' --output text 2>/dev/null || true)"
    [[ -n "${FS_ID}" && "${FS_ID}" != None ]] && break
    FS_ID=""
    sleep 2
  done
  if [[ -z "${FS_ID}" || "${FS_ID}" == None ]]; then
    exit "${create_fs_status}"
  fi
fi
if [[ "$(aws efs describe-tags --file-system-id "${FS_ID}" \
    --query "length(Tags[?Key=='RunToken' && Value=='${RUN_TOKEN}'])" --output text)" != 1 ]]; then
  echo "EFS creation token collision: returned file system is not owned by this attempt" >&2
  exit 1
fi
FS_OWNED=1
persist_state
wait_for_file_system_available "${FS_ID}"

INSTANCE_CREATE_ATTEMPTED=1
persist_state
for _ in $(seq 1 12); do
  set +e
  launch_output="$(aws ec2 run-instances \
    --image-id "${AMI_ID}" \
    --instance-type "${INSTANCE_TYPE}" \
    --client-token "${RUN_TOKEN}" \
    --iam-instance-profile "Name=${INSTANCE_PROFILE_NAME}" \
    --network-interfaces "DeviceIndex=0,SubnetId=${BENCH_SUBNET_ID},Groups=${SECURITY_GROUP_ID},AssociatePublicIpAddress=true,DeleteOnTermination=true" \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --monitoring Enabled=true \
    --block-device-mappings "${BLOCK_DEVICE_MAPPINGS}" \
    --tag-specifications \
      "ResourceType=instance,Tags=[{Key=Name,Value=nfs-crust-bench-${RUN_ID}},{Key=Project,Value=nfs-crust},{Key=Purpose,Value=benchmark},{Key=RunId,Value=${RUN_ID}},{Key=RunToken,Value=${RUN_TOKEN}}]" \
      "ResourceType=volume,Tags=[{Key=Name,Value=nfs-crust-bench-${RUN_ID}},{Key=Project,Value=nfs-crust},{Key=Purpose,Value=benchmark},{Key=RunId,Value=${RUN_ID}},{Key=RunToken,Value=${RUN_TOKEN}}]" \
    2>"${STATE_ROOT}/run-instances.err")"
  launch_status=$?
  set -e
  if [[ "${launch_status}" == 0 ]]; then
    INSTANCE_ID="$(jq -r '.Instances[0].InstanceId' <<<"${launch_output}")"
    INSTANCE_OWNED=1
    break
  fi
  INSTANCE_ID="$(aws ec2 describe-instances \
    --filters "Name=client-token,Values=${RUN_TOKEN}" "Name=tag:Project,Values=nfs-crust" \
      "Name=tag:Purpose,Values=benchmark" "Name=tag:RunId,Values=${RUN_ID}" \
      "Name=tag:RunToken,Values=${RUN_TOKEN}" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null || true)"
  if [[ -n "${INSTANCE_ID}" && "${INSTANCE_ID}" != None ]]; then
    INSTANCE_OWNED=1
    break
  fi
  INSTANCE_ID=""
  if grep -q 'Invalid IAM Instance Profile\|InvalidParameterValue.*instance profile' "${STATE_ROOT}/run-instances.err"; then
    sleep 10
    continue
  fi
  cat "${STATE_ROOT}/run-instances.err" >&2
  exit "${launch_status}"
done
if [[ -z "${INSTANCE_ID}" ]]; then
  echo "instance profile did not propagate before the launch retry budget expired" >&2
  exit 1
fi
persist_state
aws ec2 wait instance-running --instance-ids "${INSTANCE_ID}"
INSTANCE_SUBNET="$(aws ec2 describe-instances --instance-ids "${INSTANCE_ID}" --query 'Reservations[0].Instances[0].SubnetId' --output text)"
INSTANCE_AZ="$(aws ec2 describe-instances --instance-ids "${INSTANCE_ID}" --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' --output text)"

MOUNT_TARGET_CREATE_ATTEMPTED=1
persist_state
MOUNT_TARGET_ID="$(aws efs create-mount-target \
  --file-system-id "${FS_ID}" \
  --subnet-id "${INSTANCE_SUBNET}" \
  --security-groups "${SECURITY_GROUP_ID}" \
  --query MountTargetId --output text)"
MOUNT_TARGET_OWNED=1
persist_state
wait_for_mount_target_available "${MOUNT_TARGET_ID}"
MOUNT_TARGET_IP="$(aws efs describe-mount-targets --mount-target-id "${MOUNT_TARGET_ID}" --query 'MountTargets[0].IpAddress' --output text)"
MOUNT_TARGET_SUBNET="$(aws efs describe-mount-targets --mount-target-id "${MOUNT_TARGET_ID}" --query 'MountTargets[0].SubnetId' --output text)"
MOUNT_TARGET_AZ="$(aws efs describe-mount-targets --mount-target-id "${MOUNT_TARGET_ID}" --query 'MountTargets[0].AvailabilityZoneName' --output text)"
EFS_THROUGHPUT="$(aws efs describe-file-systems --file-system-id "${FS_ID}" --query 'FileSystems[0].ThroughputMode' --output text)"
EFS_AZ="$(aws efs describe-file-systems --file-system-id "${FS_ID}" --query 'FileSystems[0].AvailabilityZoneName' --output text)"
EFS_ENCRYPTED="$(aws efs describe-file-systems --file-system-id "${FS_ID}" --query 'FileSystems[0].Encrypted' --output text)"
EFS_PERFORMANCE="$(aws efs describe-file-systems --file-system-id "${FS_ID}" --query 'FileSystems[0].PerformanceMode' --output text)"
if [[ "${INSTANCE_SUBNET}" != "${MOUNT_TARGET_SUBNET}" || "${INSTANCE_AZ}" != "${MOUNT_TARGET_AZ}" ]]; then
  echo "EC2 and EFS mount target are not in the same subnet/AZ" >&2
  exit 1
fi
if [[ "${EFS_THROUGHPUT}" != elastic || "${EFS_AZ}" != None || "${EFS_ENCRYPTED}" != True || "${EFS_PERFORMANCE}" != generalPurpose ]]; then
  echo "EFS is not Regional, encrypted, General Purpose, and Elastic" >&2
  exit 1
fi
echo "verified Regional EFS with Elastic throughput; EC2 and mount target are both in ${INSTANCE_AZ}"

aws efs describe-file-systems --file-system-id "${FS_ID}" > "${ARTIFACT_ROOT}/provenance/efs.json"
aws efs describe-mount-targets --mount-target-id "${MOUNT_TARGET_ID}" > "${ARTIFACT_ROOT}/provenance/mount-target.json"
aws ec2 describe-instances --instance-ids "${INSTANCE_ID}" > "${ARTIFACT_ROOT}/provenance/ec2.json"
aws ec2 describe-subnets --subnet-ids "${INSTANCE_SUBNET}" > "${ARTIFACT_ROOT}/provenance/subnet.json"
aws ec2 describe-images --image-ids "${AMI_ID}" > "${ARTIFACT_ROOT}/provenance/ami.json"

for _ in $(seq 1 60); do
  PING_STATUS="$(aws ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=${INSTANCE_ID}" \
    --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)"
  [[ "${PING_STATUS}" == Online ]] && break
  sleep 5
done
if [[ "${PING_STATUS:-}" != Online ]]; then
  echo "instance never became available through SSM" >&2
  exit 1
fi

require_transfer_credential_lifetime "$((REMOTE_EXECUTION_TIMEOUT_SECONDS + 300))"
PRESIGN_EXPIRY_SECONDS="$((REMOTE_EXECUTION_TIMEOUT_SECONDS + 3600))"
SOURCE_DOWNLOAD_URL="$(aws s3 presign \
  "s3://${TRANSFER_BUCKET}/source.tar.gz" --expires-in "${PRESIGN_EXPIRY_SECONDS}")"
REMOTE_RUN_URL="$(aws s3 presign \
  "s3://${TRANSFER_BUCKET}/remote-run.sh" --expires-in "${PRESIGN_EXPIRY_SECONDS}")"
RESULT_UPLOAD_URL="$(
  aws configure export-credentials --profile "${AWS_PROFILE}" --format process | \
    python3 "${ROOT}/bench/aws/presign_put.py" \
      --bucket "${TRANSFER_BUCKET}" \
      --key results.tar.gz \
      --region "${AWS_REGION}" \
      --expires "${PRESIGN_EXPIRY_SECONDS}"
)"

REMOTE_COMMAND="$(jq -nr \
  --arg remote_run_url "${REMOTE_RUN_URL}" \
  --arg source_download_url "${SOURCE_DOWNLOAD_URL}" \
  --arg result_upload_url "${RESULT_UPLOAD_URL}" \
  --arg run_id "${RUN_ID}" \
  --arg mount_target_ip "${MOUNT_TARGET_IP}" \
  --arg file_system_id "${FS_ID}" \
  --arg aws_region "${AWS_REGION}" \
  --arg source_revision "${SOURCE_REVISION}" \
  --arg source_sha256 "${SOURCE_SHA256}" \
  --arg bench_quick "${BENCH_QUICK}" \
  --arg bench_repetitions "${BENCH_REPETITIONS}" \
  --arg bench_diagnostics "${BENCH_DIAGNOSTICS}" \
  --arg bench_closed_loop_limit "${BENCH_CLOSED_LOOP_LIMIT}" \
  --arg bench_write_size_sweep_kib "${BENCH_WRITE_SIZE_SWEEP_KIB}" \
  --arg bench_operation_timeout_seconds "${BENCH_OPERATION_TIMEOUT_SECONDS}" \
  '[
    "set -Eeuo pipefail",
    "command -v curl >/dev/null",
    ("curl --fail --silent --show-error --location " + ($remote_run_url|@sh) + " --output /tmp/nfs-crust-remote-run.sh"),
    "chmod 0700 /tmp/nfs-crust-remote-run.sh",
    ("RUN_ID=" + ($run_id|@sh) +
      " MOUNT_TARGET_IP=" + ($mount_target_ip|@sh) +
      " FILE_SYSTEM_ID=" + ($file_system_id|@sh) +
      " AWS_REGION=" + ($aws_region|@sh) +
      " SOURCE_REVISION=" + ($source_revision|@sh) +
      " SOURCE_SHA256=" + ($source_sha256|@sh) +
      " SOURCE_DOWNLOAD_URL=" + ($source_download_url|@sh) +
      " RESULT_UPLOAD_URL=" + ($result_upload_url|@sh) +
      " BENCH_QUICK=" + ($bench_quick|@sh) +
      " BENCH_REPETITIONS=" + ($bench_repetitions|@sh) +
      " BENCH_DIAGNOSTICS=" + ($bench_diagnostics|@sh) +
      " BENCH_CLOSED_LOOP_LIMIT=" + ($bench_closed_loop_limit|@sh) +
      " BENCH_WRITE_SIZE_SWEEP_KIB=" + ($bench_write_size_sweep_kib|@sh) +
      " BENCH_OPERATION_TIMEOUT_SECONDS=" + ($bench_operation_timeout_seconds|@sh) +
      " /tmp/nfs-crust-remote-run.sh")
  ] | join("; ")')"
COMMANDS="$(jq -cn --arg command "${REMOTE_COMMAND}" \
  --arg execution_timeout "${REMOTE_EXECUTION_TIMEOUT_SECONDS}" \
  '{executionTimeout:[$execution_timeout],commands:[$command]}')"
COMMAND_ID="$(aws ssm send-command \
  --instance-ids "${INSTANCE_ID}" \
  --document-name AWS-RunShellScript \
  --timeout-seconds "${REMOTE_EXECUTION_TIMEOUT_SECONDS}" \
  --parameters "${COMMANDS}" \
  --comment "nfs-crust benchmark ${RUN_ID}" \
  --query Command.CommandId --output text)"

COMMAND_STATUS=Pending
for _ in $(seq 1 720); do
  COMMAND_STATUS="$(aws ssm get-command-invocation \
    --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}" \
    --query Status --output text 2>/dev/null || true)"
  case "${COMMAND_STATUS}" in
    Success|Failed|Cancelled|TimedOut) break ;;
  esac
  sleep 15
done
aws ssm get-command-invocation --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}" \
  > "${ARTIFACT_ROOT}/ssm-command.json"

for _ in $(seq 1 20); do
  aws s3 cp "s3://${TRANSFER_BUCKET}/results.tar.gz" "${STATE_ROOT}/results.tar.gz" --only-show-errors && break
  sleep 5
done
if [[ ! -s "${STATE_ROOT}/results.tar.gz" ]]; then
  echo "benchmark results were not uploaded; SSM status ${COMMAND_STATUS}" >&2
  exit 1
fi
mkdir -p "${ARTIFACT_ROOT}/run"
tar -xzf "${STATE_ROOT}/results.tar.gz" -C "${ARTIFACT_ROOT}/run"
(cd "${ARTIFACT_ROOT}/run" && shasum -a 256 -c SHA256SUMS)

SUMMARY="${ARTIFACT_ROOT}/run/run/summary.json"
if [[ ! -s "${SUMMARY}" ]]; then
  echo "benchmark summary is missing; SSM status ${COMMAND_STATUS}" >&2
  exit 1
fi
START_TIME="$(python3 -c 'import datetime,json,sys; d=json.load(open(sys.argv[1])); second=(d["metadata"]["started_at_unix_ms"]//60000)*60-60; print(datetime.datetime.fromtimestamp(second,datetime.timezone.utc).isoformat())' "${SUMMARY}")"
END_TIME="$(python3 -c 'import datetime,json,sys; d=json.load(open(sys.argv[1])); second=(d["metadata"]["completed_at_unix_ms"]//60000)*60+120; print(datetime.datetime.fromtimestamp(second,datetime.timezone.utc).isoformat())' "${SUMMARY}")"
EXPECTED_FINAL_EPOCH="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print((d["metadata"]["completed_at_unix_ms"]//60000)*60)' "${SUMMARY}")"

collect_metric() {
  local namespace=$1 metric=$2 statistic=$3 dimension_name=$4 dimension_value=$5 output=$6
  aws cloudwatch get-metric-statistics \
    --namespace "${namespace}" \
    --metric-name "${metric}" \
    --dimensions "Name=${dimension_name},Value=${dimension_value}" \
    --start-time "${START_TIME}" \
    --end-time "${END_TIME}" \
    --period 60 \
    --statistics "${statistic}" \
    --output json > "${ARTIFACT_ROOT}/cloudwatch/${output}.json"
}

cloudwatch_ready=false
for _ in $(seq 1 12); do
  collect_metric AWS/EFS PercentIOLimit Maximum FileSystemId "${FS_ID}" "efs-PercentIOLimit-maximum"
  if python3 -c 'import datetime,json,sys; d=json.load(open(sys.argv[1])); expected=float(sys.argv[2]); raise SystemExit(0 if any(datetime.datetime.fromisoformat(p["Timestamp"].replace("Z","+00:00")).timestamp() >= expected for p in d.get("Datapoints",[])) else 1)' \
      "${ARTIFACT_ROOT}/cloudwatch/efs-PercentIOLimit-maximum.json" "${EXPECTED_FINAL_EPOCH}"; then
    cloudwatch_ready=true
    break
  fi
  sleep 30
done
if [[ "${cloudwatch_ready}" != true ]]; then
  echo "CloudWatch did not publish the final benchmark minute before the polling deadline" >&2
fi

for metric in PercentIOLimit PermittedThroughput ClientConnections; do
  collect_metric AWS/EFS "${metric}" Average FileSystemId "${FS_ID}" "efs-${metric}-average"
  collect_metric AWS/EFS "${metric}" Maximum FileSystemId "${FS_ID}" "efs-${metric}-maximum"
done
for metric in MeteredIOBytes DataReadIOBytes DataWriteIOBytes MetadataIOBytes; do
  collect_metric AWS/EFS "${metric}" Sum FileSystemId "${FS_ID}" "efs-${metric}-sum"
done
for metric in CPUUtilization; do
  collect_metric AWS/EC2 "${metric}" Average InstanceId "${INSTANCE_ID}" "ec2-${metric}-average"
  collect_metric AWS/EC2 "${metric}" Maximum InstanceId "${INSTANCE_ID}" "ec2-${metric}-maximum"
done
for metric in NetworkIn NetworkOut NetworkPacketsIn NetworkPacketsOut; do
  collect_metric AWS/EC2 "${metric}" Sum InstanceId "${INSTANCE_ID}" "ec2-${metric}-sum"
done

jq -n \
  --arg run_id "${RUN_ID}" \
  --arg account_id "${ACCOUNT_ID}" \
  --arg region "${AWS_REGION}" \
  --arg availability_zone "${INSTANCE_AZ}" \
  --arg vpc_id "${BENCH_VPC_ID}" \
  --arg subnet_id "${INSTANCE_SUBNET}" \
  --arg instance_id "${INSTANCE_ID}" \
  --arg instance_type "${INSTANCE_TYPE}" \
  --arg ami_id "${AMI_ID}" \
  --arg file_system_id "${FS_ID}" \
  --arg mount_target_id "${MOUNT_TARGET_ID}" \
  --arg source_revision "${SOURCE_REVISION}" \
  --arg source_sha256 "${SOURCE_SHA256}" \
  --arg efs_encrypted "${EFS_ENCRYPTED}" \
  --arg efs_performance_mode "${EFS_PERFORMANCE}" \
  --arg efs_throughput_mode "${EFS_THROUGHPUT}" \
  '{
    run_id:$run_id,aws_account_id:$account_id,region:$region,availability_zone:$availability_zone,
    vpc_id:$vpc_id,subnet_id:$subnet_id,instance_id:$instance_id,instance_type:$instance_type,
    ami_id:$ami_id,file_system_id:$file_system_id,mount_target_id:$mount_target_id,
    efs:{regional:true,encrypted:($efs_encrypted=="True"),performance_mode:$efs_performance_mode,throughput_mode:$efs_throughput_mode,same_az_mount_target:true},
    transport:{nfs_crust:"tls",linux_reference:"tls",tls_server_name:($file_system_id + ".efs." + $region + ".amazonaws.com")},
    source:{revision:$source_revision,dirty:false,archive_sha256:$source_sha256}
  }' > "${ARTIFACT_ROOT}/provenance.json"

if [[ "${COMMAND_STATUS}" != Success ]]; then
  echo "benchmark command ended with status ${COMMAND_STATUS}; artifacts were preserved" >&2
  exit 1
fi

cleanup
