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

## Python environment setup
Choose one of the following approaches before running the scripts:

### Option A: Virtual environment (recommended)
1. Install Python 3.11 (or newer). On Windows the default path is typically `C:\\Users\\<you>\\AppData\\Local\\Programs\\Python\\Python311\\python.exe`.
2. Create and activate a venv:
   ```bash
   python -m venv .venv
   # Windows PowerShell
   .venv\\Scripts\\Activate.ps1
   # macOS/Linux
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. If you are using VS Code, choose **Python: Select Interpreter** and point to `.venv\\Scripts\\python.exe` so the editor runs tasks inside the venv (other IDEs/editors can target the same interpreter path).

### Option B: Conda environment
1. Create a new environment (example):
   ```bash
   conda create -n eea-env python=3.11 -y
   conda activate eea-env
   ```
2. Install dependencies via pip inside the activated env:
   ```bash
   pip install -r requirements.txt
   ```
3. Select the conda interpreter in your IDE/editor (e.g., in VS Code use **Select Interpreter**, otherwise point tooling to `C:\\Users\\<you>\\miniconda3\\envs\\eea-env\\python.exe`).

### Option C: Global install (quick & dirty)
1. Ensure `python --version` returns 3.11+.
2. Run `pip install -r requirements.txt` (installs packages for your user/site-packages).
3. Be mindful this may conflict with other projects; prefer options A or B for isolation.

After the dependencies are installed, run the workflow scripts (metadata download, bbox extraction, enrichment, plotting, etc.) from the same interpreter.