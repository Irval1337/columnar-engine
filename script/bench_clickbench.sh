#!/usr/bin/env bash
set -euo pipefail

[[ $# -eq 3 ]] || {
  echo "usage: bench_clickbench.sh <converter-or-build-dir> <dataset-dir> <out-dir>" >&2
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONVERTER_ARG="$1"
DATASET_DIR="$2"
OUT_DIR="$3"
RUNS="${BENCH_RUNS:-3}"
DROP_CACHES="${BENCH_DROP_CACHES:-1}"

drop_page_cache() {
  [[ "$DROP_CACHES" == "1" ]] || return
  sync
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    echo 3 > /proc/sys/vm/drop_caches
  elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
  elif [[ -z "${BENCH_DROP_CACHES_WARNED:-}" ]]; then
    echo "[bench] warning: cannot drop page cache; results may depend on warm file cache" >&2
    BENCH_DROP_CACHES_WARNED=1
  fi
}

run_mode() {
  local mode="$1"
  local input="$2"
  local input_bytes="$3"
  local output_ext="$4"
  shift 4

  local times=()
  local warmup="$OUT_DIR/warmup.${output_ext}"
  local title="$mode"

  case "$mode" in
    csv2bruh) title="csv2bruh (CSV -> BRUH)" ;;
    bruh2csv) title="bruh2csv (BRUH -> CSV)" ;;
  esac

  echo
  echo "== ${title} =="
  echo "[bench] warmup"
  "$CONVERTER" "$@" --mode="$mode" --input="$input" --output="$warmup" >/dev/null
  rm -f "$warmup"

  for run in $(seq 1 "$RUNS"); do
    local out_file="$OUT_DIR/${mode}-run-${run}.${output_ext}"
    drop_page_cache
    local t seconds speed
    t="$(mktemp)"
    /usr/bin/time -f "%e" -o "$t" "$CONVERTER" "$@" --mode="$mode" --input="$input" --output="$out_file" >/dev/null
    seconds="$(cat "$t")"
    rm -f "$t"
    speed="$(awk -v b="$input_bytes" -v s="$seconds" 'BEGIN { printf "%.2f", b / 1024 / 1024 / s }')"
    times+=("$seconds")
    echo "[bench] run ${run}: seconds=${seconds} mib_per_s=${speed} output_bytes=$(stat -c%s "$out_file")"
  done

  python3 - "$mode" "$input_bytes" "${times[@]}" <<'PY'
import statistics
import sys
mode = sys.argv[1]
input_bytes = int(sys.argv[2])
times = [float(x) for x in sys.argv[3:]]
speeds = [input_bytes / 1024 / 1024 / t for t in times]
print(
    "[bench] summary: "
    f"runs={len(times)} "
    f"median_seconds={statistics.median(times):.3f} "
    f"best_seconds={min(times):.3f} "
    f"median_mib_per_s={statistics.median(speeds):.2f} "
    f"best_mib_per_s={max(speeds):.2f}"
)
PY
}

mkdir -p "$OUT_DIR"
if [[ -f "$CONVERTER_ARG" && -x "$CONVERTER_ARG" ]]; then
  CONVERTER="$CONVERTER_ARG"
else
  CONVERTER="${CONVERTER_ARG%/}/apps/converter/converter"
  if [[ ! -x "$CONVERTER" ]]; then
    echo "[bench] converter missing, building via script/build.sh into ${CONVERTER_ARG}" >&2
    (
      export BUILD_DIR="$CONVERTER_ARG"
      bash "${SCRIPT_DIR}/build.sh"
    )
  fi
  [[ -x "$CONVERTER" ]] || { echo "converter not found: $CONVERTER" >&2; exit 1; }
fi

[[ -d "$DATASET_DIR" ]] || { echo "dataset dir not found: $DATASET_DIR" >&2; exit 1; }
SCHEMA_PATH="$DATASET_DIR/schema.csv"
[[ -f "$SCHEMA_PATH" ]] || SCHEMA_PATH="$(dirname "$DATASET_DIR")/schema.csv"
CSV_PATH="$DATASET_DIR/data.csv"
[[ -f "$CSV_PATH" ]] || CSV_PATH="$DATASET_DIR/hits_sample.csv"
[[ -f "$SCHEMA_PATH" ]] || { echo "schema not found near dataset: $DATASET_DIR" >&2; exit 1; }
[[ -f "$CSV_PATH" ]] || { echo "csv not found in dataset: $DATASET_DIR" >&2; exit 1; }

STAGE_DIR="${BENCH_STAGE_DIR:-$(mktemp -d /tmp/clickbench-stage.XXXXXX)}"
mkdir -p "$STAGE_DIR"
cp "$SCHEMA_PATH" "$STAGE_DIR/schema.csv"
cp "$CSV_PATH" "$STAGE_DIR/data.csv"
trap "rm -rf '$STAGE_DIR'" EXIT

STAGED_SCHEMA="$STAGE_DIR/schema.csv"
STAGED_CSV="$STAGE_DIR/data.csv"
CSV_BYTES="$(stat -c%s "$STAGED_CSV")"
echo "[bench] runs=${RUNS}"
echo "[bench] drop_caches=${DROP_CACHES}"
echo "[bench] staged_input_dir=$(dirname "$STAGED_CSV")"
echo "== Reduced ClickBench =="

run_mode csv2bruh "$STAGED_CSV" "$CSV_BYTES" bruh --schema="$STAGED_SCHEMA"

INPUT_BRUH="$OUT_DIR/input.bruh"
drop_page_cache
"$CONVERTER" --mode=csv2bruh --schema="$STAGED_SCHEMA" --input="$STAGED_CSV" --output="$INPUT_BRUH" >/dev/null
BRUH_BYTES="$(stat -c%s "$INPUT_BRUH")"

run_mode bruh2csv "$INPUT_BRUH" "$BRUH_BYTES" csv
