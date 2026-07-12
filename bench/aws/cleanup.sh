#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STATE_FILE="${1:?usage: cleanup.sh .bench/state/<run-id>/resources.json}"
ARTIFACT_ROOT="$(dirname "${STATE_FILE}")/artifacts"
mkdir -p "${ARTIFACT_ROOT}"

exec python3 "${ROOT}/bench/aws/cleanup.py" "${STATE_FILE}" \
  --result "${ARTIFACT_ROOT}/cleanup.json"
