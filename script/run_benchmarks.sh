#!/usr/bin/env bash
set -euo pipefail

unset CMAKE_C_COMPILER_LAUNCHER
unset CMAKE_CXX_COMPILER_LAUNCHER

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/build-release}"
DATASET_DIR="${DATASET_DIR:-${REPO_ROOT}/clickbench/reduced-clickbench-v1}"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/build/benchmarks/$(date +%Y%m%d-%H%M%S)}"

echo "=== Building Project ==="
bash "${SCRIPT_DIR}/build.sh"

if [[ ! -d "${DATASET_DIR}" ]]; then
  echo "=== Fetching Benchmark Dataset ==="
  fetch_root="$(dirname "${DATASET_DIR}")"
  mkdir -p "${fetch_root}"
  bash "${SCRIPT_DIR}/fetch_clickbench.sh" "${fetch_root}" >/dev/null
fi

echo "=== Running Benchmarks ==="
bash "${SCRIPT_DIR}/bench_clickbench.sh" "${BUILD_DIR}" "${DATASET_DIR}" "${OUT_DIR}"
