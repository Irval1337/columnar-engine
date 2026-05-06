#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: run_query.sh <query_num> <columnar> <output.csv> <log.file>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

QUERY_NUM="$1"
COLUMNAR="$2"
OUTPUT_CSV="$3"
LOG_FILE="$4"
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/build-release}"
RUNNER="${BUILD_DIR}/apps/clickbench/clickbench_runner"

if [[ ! "${QUERY_NUM}" =~ ^[0-9]+$ ]]; then
  echo "query_num must be a non-negative integer: ${QUERY_NUM}" >&2
  exit 1
fi

if [[ ! -f "${COLUMNAR}" ]]; then
  echo "columnar file not found: ${COLUMNAR}" >&2
  exit 1
fi

if [[ ! -x "${RUNNER}" ]]; then
  bash "${SCRIPT_DIR}/build.sh"
fi

mkdir -p "$(dirname "${OUTPUT_CSV}")"
mkdir -p "$(dirname "${LOG_FILE}")"

{
  echo "[run_query] query=${QUERY_NUM}"
  echo "[run_query] input=${COLUMNAR}"
  echo "[run_query] output=${OUTPUT_CSV}"
} >"${LOG_FILE}"

# Unimplemented queries return success with an empty CSV from the runner,
# so we don't gate by query number here anymore.
time_file="$(mktemp)"
cleanup() {
  rm -f "${time_file}"
}
trap cleanup EXIT

status=0
if [[ -x /usr/bin/time ]]; then
  /usr/bin/time -f "[run_query] elapsed_seconds=%e" -o "${time_file}" \
    "${RUNNER}" --input="${COLUMNAR}" --query="${QUERY_NUM}" --output="${OUTPUT_CSV}" \
    >>"${LOG_FILE}" 2>&1 || status="$?"
  cat "${time_file}" >>"${LOG_FILE}"
else
  "${RUNNER}" --input="${COLUMNAR}" --query="${QUERY_NUM}" --output="${OUTPUT_CSV}" \
    >>"${LOG_FILE}" 2>&1 || status="$?"
fi

if [[ "${status}" == "0" ]]; then
  exit 0
fi

echo "[run_query] failed with status ${status}" >>"${LOG_FILE}"
exit "${status}"
