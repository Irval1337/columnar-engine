#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${1:-build-release}"
BENCH_DIR="${BUILD_DIR}/benchmarks"

MIN_TIME="${BENCH_MIN_TIME:-0.1}"
REPS="${BENCH_REPS:-1}"

if [[ ! -d "$BENCH_DIR" ]]; then
  echo "[bench] No benchmarks dir: $BENCH_DIR"
  echo "[bench] Nothing to run."
  exit 0
fi

echo "[bench] Looking for benchmark executables in: $BENCH_DIR"

mapfile -d '' BENCHES < <(find "$BENCH_DIR" -maxdepth 1 -type f -executable -print0)

if [[ "${#BENCHES[@]}" -eq 0 ]]; then
  echo "[bench] No benchmark executables found."
  exit 0
fi

for b in "${BENCHES[@]}"; do
  echo "=================================================="
  echo "[bench] Running: $b"
  "$b" --benchmark_min_time="$MIN_TIME" --benchmark_repetitions="$REPS"
done

echo "=================================================="
echo "[bench] Done."
