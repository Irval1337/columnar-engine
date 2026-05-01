#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/build-release}"
CMAKE_GENERATOR="${CMAKE_GENERATOR:-Ninja}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
COLUMNAR_BUILD_TESTS="${COLUMNAR_BUILD_TESTS:-OFF}"

cmake_args=(
  -S "${REPO_ROOT}"
  -B "${BUILD_DIR}"
  -G "${CMAKE_GENERATOR}"
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  -DCOLUMNAR_BUILD_TESTS="${COLUMNAR_BUILD_TESTS}"
)

append_cmake_arg_if_set() {
  local name="$1"
  local value="${!name:-}"
  if [[ -n "${value}" ]]; then
    cmake_args+=("-D${name}=${value}")
  fi
}

append_cmake_arg_if_set CMAKE_C_COMPILER
append_cmake_arg_if_set CMAKE_CXX_COMPILER


append_cmake_arg_if_set CMAKE_C_FLAGS
append_cmake_arg_if_set CMAKE_CXX_FLAGS
append_cmake_arg_if_set FETCHCONTENT_BASE_DIR

reset_stale_build_state() {
  rm -rf \
    "${BUILD_DIR}/CMakeFiles" \
    "${BUILD_DIR}/apps"

  rm -f \
    "${BUILD_DIR}/CMakeCache.txt" \
    "${BUILD_DIR}/build.ninja" \
    "${BUILD_DIR}/cmake_install.cmake" \
    "${BUILD_DIR}/rules.ninja"
}

echo "[build] configure: ${BUILD_DIR}"
if ! cmake "${cmake_args[@]}"; then
  echo "[build] configure failed, resetting stale build state"
  reset_stale_build_state
  cmake "${cmake_args[@]}"
fi

echo "[build] build converter and clickbench_runner"
cmake --build "${BUILD_DIR}" --target converter clickbench_runner -j

echo "[build] done: ${BUILD_DIR}"
