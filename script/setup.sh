#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND="${DEBIAN_FRONTEND:-noninteractive}"

have_toolchain() {
  command -v cmake >/dev/null 2>&1 &&
    command -v ninja >/dev/null 2>&1 &&
    command -v g++ >/dev/null 2>&1
}

install_apt_dependencies() {
  local apt=(apt-get)
  if [[ "$(id -u)" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
      return 1
    fi
    apt=(sudo apt-get)
  fi

  "${apt[@]}" update
  "${apt[@]}" install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    git \
    ninja-build \
    time \
    unzip
}

if ! have_toolchain; then
  if command -v apt-get >/dev/null 2>&1; then
    install_apt_dependencies
  fi
fi

missing=()
for tool in cmake ninja g++ git; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    missing+=("${tool}")
  fi
done

if [[ "${#missing[@]}" -ne 0 ]]; then
  echo "[setup] missing required tools: ${missing[*]}" >&2
  exit 1
fi

echo "[setup] toolchain is ready"
