#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:?usage: fetch_clickbench.sh <out-dir> [repo] [tag] [dataset-name]}"
DATASET_REPO="${2:-Irval1337/columnar-bench-data}"
DATASET_TAG="${3:-reduced-clickbench-v1}"
DATASET_NAME="${4:-reduced-clickbench-v1}"

mkdir -p "$OUT_DIR"

BASE_URL="https://github.com/${DATASET_REPO}/releases/download/${DATASET_TAG}"

curl -L --fail \
  -o "$OUT_DIR/${DATASET_NAME}.tar.zst" \
  "${BASE_URL}/${DATASET_NAME}.tar.zst"

curl -L --fail \
  -o "$OUT_DIR/${DATASET_NAME}.sha256" \
  "${BASE_URL}/${DATASET_NAME}.sha256"

curl -L --fail \
  -o "$OUT_DIR/schema.csv" \
  "${BASE_URL}/schema.csv"

(
  cd "$OUT_DIR"
  sha256sum -c "${DATASET_NAME}.sha256"
  tar --zstd -xf "${DATASET_NAME}.tar.zst"
)

echo "$OUT_DIR/${DATASET_NAME}"
