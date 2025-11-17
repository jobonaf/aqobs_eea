#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR=${WORK_DIR:-"$REPO_ROOT/work"}

METADATA_CSV=${METADATA_CSV:-"$WORK_DIR/metadata/stations_metadata.csv"}
PARQUET_DIR=${PARQUET_DIR:-"$WORK_DIR/eea_parquets"}
OUTPUT_CSV=${OUTPUT_CSV:-"$WORK_DIR/tmp/portability_bbox.csv"}
BBOX_VALUES=${BBOX_VALUES:-"12.3 13.95 45.58 46.67"}
VERBOSE_FLAG=${EXTRACT_VERBOSE:-"--verbose"}

mkdir -p "$PARQUET_DIR"
mkdir -p "$(dirname "$OUTPUT_CSV")"

IFS=' ' read -r MIN_LON MAX_LON MIN_LAT MAX_LAT <<< "$BBOX_VALUES"

if [[ ! -f "$METADATA_CSV" ]]; then
  echo "[error] Metadata CSV not found: $METADATA_CSV" >&2
  exit 1
fi

if [[ ! -d "$PARQUET_DIR" ]]; then
  echo "[error] Parquet directory not found: $PARQUET_DIR" >&2
  exit 1
fi

echo "[info] Running bbox extract check"
python "$REPO_ROOT/scripts/extract_eea_bbox.py" \
  --metadata "$METADATA_CSV" \
  --indir "$PARQUET_DIR" \
  --bbox "$MIN_LON" "$MAX_LON" "$MIN_LAT" "$MAX_LAT" \
  --out "$OUTPUT_CSV" \
  --check \
  $VERBOSE_FLAG

echo "[info] Extract check completed. Results (if any) stored next to: $OUTPUT_CSV"
