#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VOCAB_DIR=${VOCAB_DIR:-"$REPO_ROOT/eea_vocabularies"}
TEST_VOCAB=${TEST_VOCAB:-"pollutant.json"}
INPUT_CSV=${INPUT_CSV:-"$REPO_ROOT/tmp/portability_bbox.csv"}
METADATA_CSV=${METADATA_CSV:-"$REPO_ROOT/metadata/stations_metadata.csv"}
OUTPUT_CSV=${OUTPUT_CSV:-"$REPO_ROOT/tmp/portability_enriched_without_vocab.csv"}

TARGET_FILE="$VOCAB_DIR/$TEST_VOCAB"
BACKUP_FILE="$TARGET_FILE.bak"

if [[ ! -f "$TARGET_FILE" ]]; then
  echo "[error] Vocabulary file not found: $TARGET_FILE" >&2
  exit 1
fi

trap '[[ -f "$BACKUP_FILE" ]] && mv "$BACKUP_FILE" "$TARGET_FILE"' EXIT

mv "$TARGET_FILE" "$BACKUP_FILE"
echo "[info] Temporarily removed $TARGET_FILE to simulate missing vocab"

set +e
python "$REPO_ROOT/scripts/enrich_eea_data.py" \
  --input "$INPUT_CSV" \
  --metadata "$METADATA_CSV" \
  --vocab-dir "$VOCAB_DIR" \
  --output "$OUTPUT_CSV" \
  --verbose
STATUS=$?
set -e

if [[ $STATUS -ne 0 ]]; then
  echo "[warning] Enrichment failed when $TEST_VOCAB was missing (status $STATUS)"
else
  echo "[info] Enrichment completed despite missing $TEST_VOCAB; check logs for warning coverage."
fi
