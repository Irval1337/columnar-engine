#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: convert.sh <input.csv> <input.schema.csv> <output.columnar>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_CSV="$1"
INPUT_SCHEMA="$2"
OUTPUT_COLUMNAR="$3"
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/build-release}"
CONVERTER="${BUILD_DIR}/apps/converter/converter"

if [[ ! -f "${INPUT_CSV}" ]]; then
  echo "input csv not found: ${INPUT_CSV}" >&2
  exit 1
fi

if [[ ! -f "${INPUT_SCHEMA}" ]]; then
  echo "input schema not found: ${INPUT_SCHEMA}" >&2
  exit 1
fi

if [[ ! -x "${CONVERTER}" ]]; then
  bash "${SCRIPT_DIR}/build.sh"
fi

mkdir -p "$(dirname "${OUTPUT_COLUMNAR}")"

echo "[convert] csv2bruh"
"${CONVERTER}" \
  --mode=csv2bruh \
  --schema="${INPUT_SCHEMA}" \
  --input="${INPUT_CSV}" \
  --output="${OUTPUT_COLUMNAR}"

echo "[convert] done: ${OUTPUT_COLUMNAR}"
