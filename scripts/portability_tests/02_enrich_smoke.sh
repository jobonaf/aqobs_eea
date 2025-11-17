#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

INPUT_CSV=${INPUT_CSV:-"$REPO_ROOT/tmp/portability_bbox.csv"}
METADATA_CSV=${METADATA_CSV:-"$REPO_ROOT/metadata/stations_metadata.csv"}
VOCAB_DIR=${VOCAB_DIR:-"$REPO_ROOT/eea_vocabularies"}
OUTPUT_CSV=${OUTPUT_CSV:-"$REPO_ROOT/tmp/portability_enriched.csv"}
VERBOSE_FLAG=${ENRICH_VERBOSE:-"--verbose"}

if [[ ! -f "$INPUT_CSV" ]]; then
  echo "[warning] Input CSV $INPUT_CSV not found; run 01_check_extract_bbox.sh first or set INPUT_CSV." >&2
fi

mkdir -p "$(dirname "$OUTPUT_CSV")"

echo "[info] Running enrichment smoke test"
python "$REPO_ROOT/scripts/enrich_eea_data.py" \
  --input "$INPUT_CSV" \
  --metadata "$METADATA_CSV" \
  --vocab-dir "$VOCAB_DIR" \
  --output "$OUTPUT_CSV" \
  $VERBOSE_FLAG

echo "[info] Enrichment finished. Output at: $OUTPUT_CSV"
