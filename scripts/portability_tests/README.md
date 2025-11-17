# Portability Test Scripts

These bash utilities exercise the main parts of the ingest pipeline to ensure it runs consistently across environments.

## Usage
All scripts rely only on `bash` and the project’s Python dependencies. Each script accepts optional environment variables so you can point to custom inputs without modifying the file.

```
cd scripts/portability_tests
bash 01_check_extract_bbox.sh
bash 02_enrich_smoke.sh
bash 03_vocab_resilience.sh
```

Set environment variables inline when needed, e.g. `METADATA_CSV=/data/metadata.csv bash 01_check_extract_bbox.sh`.

| Script | Purpose |
| --- | --- |
| `01_check_extract_bbox.sh` | Runs `extract_eea_bbox.py --check` to confirm metadata/parquet alignment for a small bounding box. |
| `02_enrich_smoke.sh` | Executes `enrich_eea_data.py` against a lightweight CSV to confirm vocabulary lookups and coverage logging. |
| `03_vocab_resilience.sh` | Temporarily hides a vocabulary JSON to ensure the enrichment path logs warnings yet still completes.

## Defaults
- Metadata CSV: `metadata/stations_metadata.csv`
- Parquet directory: `eea_parquets`
- Input CSV for enrichment: `tmp/portability_bbox.csv` (produced by script 01)
- Vocabulary directory: `eea_vocabularies`

Adjust paths through the variables described inside each script.