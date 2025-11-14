# EEA Air Quality Data Workflow

Automated download and processing of EEA air quality data.

## Scripts
- `download_eea_e1a_e2a.py`: Hourly measurements (E1a/E2a datasets)
- `download_eea_metadata.py`: Station metadata and locations
- `download_eea_vocabularies.py`: Code lists (pollutants, units, methods)
- `eea_vocabularies.py`: Vocabulary mapping utilities
- `extract_eea_bbox.py`: Geographic filtering by bounding box
- `enrich_eea_data.py`: Enrich data with metadata and vocabulary labels
- `plot_eea_boxplots.R`: Generate annual boxplots

## Workflow
0. Activate environment: `module load miniconda3 R/3.5.2; conda activate eea_env`
1. Download measurements: `python download_eea_e1a_e2a.py --email your@email.org --aggregation hour`
2. Download metadata: `python download_eea_metadata.py`
3. Download vocabularies: `python download_eea_vocabularies.py`
4. Extract region: `python extract_eea_bbox.py --bbox 12.3 13.95 45.58 46.67`
5. Enrich data: `python enrich_eea_data.py --input eea_bbox.csv --output eea_enriched.csv --verbose`
6. Generate plots: `Rscript plot_eea_boxplots.R -i eea_enriched.csv -o boxplots.pdf`

## Output
- `eea_parquets/`: Measurement files
- `metadata/`: Station metadata
- `eea_vocabularies/`: Code lists
- `eea_bbox.csv`: Filtered measurements
- `eea_enriched.csv`: Data with station names and pollutant labels
- `boxplots.pdf`: Annual distribution plots

## Notes
- Email required for EEA API
- Coordinates: WGS84 (lon/lat)
- Time zone: UTC