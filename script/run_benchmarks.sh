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
"${SCRIPT_DIR}/build.sh"

echo "=== Running Benchmarks ==="
bash "${SCRIPT_DIR}/bench_clickbench.sh" "${BUILD_DIR}" "${DATASET_DIR}" "${OUT_DIR}"
