#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUTPUT_DIR=${OUTPUT_DIR:-"$REPO_ROOT/metadata"}
OUTPUT_FILE=${OUTPUT_FILE:-"stations_metadata.csv"}
DOWNLOAD_URL=${DOWNLOAD_URL:-"https://discomap.eea.europa.eu/App/AQViewer/download?fqn=Airquality_Dissem.b2g.measurements&f=csv"}
FORCE_FLAG=${FORCE_FLAG:-""}

mkdir -p "$OUTPUT_DIR"

CMD=(
  python "$REPO_ROOT/scripts/download_eea_metadata.py"
  --output-dir "$OUTPUT_DIR"
  --output-filename "$OUTPUT_FILE"
  --url "$DOWNLOAD_URL"
)

if [[ -n "$FORCE_FLAG" ]]; then
  CMD+=("--force")
fi

echo "[info] Downloading metadata into $OUTPUT_DIR/$OUTPUT_FILE"
"${CMD[@]}"

echo "[info] Metadata download complete"
