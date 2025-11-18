# EEA Air Quality Data Workflow (`aqobs_eea`)

A toolkit for downloading, filtering, enriching and visualizing European Environment Agency (EEA) air quality observed data (AQ e-Reporting).

---

## Installation

You can set up the project using **Conda**, **Python venv**, or a minimal **quick-and-dirty** installation.

### Option A — Conda (recommended)  
```bash
conda create -n eea_env python=3.10  
conda activate eea_env  
pip install -r requirements.txt  
````

### Option B — Python venv

```bash
python3 -m venv eea_env  
source eea_env/bin/activate    # on Linux / macOS  
# or: eea_env\Scripts\activate # on Windows  
pip install -r requirements.txt  
```

### Option C — Quick and Dirty

```bash
pip install -r requirements.txt  
```

*(This installs packages directly into your system Python — not recommended for long-term or production use.)*

---

## Project Structure

Here are the main folders in the repository:

* `src/` — Python modules for the core functionality: utilities, vocabulary handling, metadata processing.
* `src/portability_tests/` — Tests to verify workflow portability across different environments (e.g., different OS, path styles).
* `scripts/` — Executable scripts that implement each stage of the data pipeline.

---

## Environment Setup and Workflow Execution

### Environment setup

1. Install dependencies (see **Installation** above).
2. (Optional) On an HPC / cluster system, you might load modules before activating the environment:

   ```bash
   module load miniconda3 R/3.5.2
   conda activate eea_env
   ```

### Data processing pipeline

Once the environment is ready, you can run in sequence:

1. Download raw measurements
2. Download station metadata
3. Download vocabularies
4. Filter and aggregate data by bounding box
5. Enrich data with metadata and labels
6. (Optional) Produce boxplot visualizations in R

---

## Script Options

Here is a more detailed overview of the main scripts and their command-line options.

### `download_eea_e1a_e2a.py`

Downloads EEA air quality data for the E1a or E2a datasets.

**Options:**

* `--email EMAIL` — (required) Your email, used for API authentication
* `--countries COUNTRIES` — List of country codes to filter by (optional)
* `--pollutants POLLUTANTS` — List of pollutant codes or names (optional)
* `--dataset {E1a,E2a}` — Which dataset to download (default as coded)
* `--cities CITIES` — List of city names to restrict download (optional)
* `--download-dir DOWNLOAD_DIR` — Directory to save downloaded files
* `--verbose` — Print detailed log output
* `--dry-run` — Show what would be done, without making requests
* `--force` — Re-download even if files already exist

---

### `download_eea_metadata.py`

Downloads and extracts station metadata from the EEA.

**Options:**

* `--url URL` — Custom URL for the metadata ZIP (default is the EEA measurement download link)
* `--output-dir OUTPUT_DIR` — Directory to save the metadata CSV
* `--output-filename OUTPUT_FILENAME` — Name of the resulting metadata file
* `--expected-file EXPECTED_FILE` — Name of the CSV file inside the ZIP (e.g. `DataExtract.csv`)
* `--verbose` — Enable verbose logging
* `--force` — Re-download even if the file already exists

---

### `download_eea_vocabularies.py`

Fetches EEA vocabularies (code lists) in raw JSON.

**Options:**

* `--vocabulary {pollutant,quality_flag,unit,aggregation_process,station_type,measurement_method,sampling_method,all}` — Which vocabulary types to download (default: all)
* `--force` — Force a refresh of cached vocabularies
* `--list` — List available vocabulary types and exit
* `--test` — Test the accessibility of all vocabularies via API
* `--verbose` — Enable verbose output

---

### `extract_eea_bbox.py`

Filters the downloaded measurement data by geographic bounding box (and optionally aggregates temporally).

**Options:**

* `--indir INDIR [INDIR …]` — Input folder(s) containing Parquet files (default: `eea_parquets/`)
* `--metadata METADATA` — Path to the metadata CSV (default: `metadata/stations_metadata.csv`)
* `--out OUT` — Output CSV for filtered data (default: `eea_bbox.csv`)
* `--bbox MIN_LON MAX_LON MIN_LAT MAX_LAT` — Geographic bounding box (WGS84)
* `--pollutants POLLUTANTS [ … ]` — Filter by pollutant code or name
* `--aggregation AGGREGATION` — Temporal aggregation type, e.g. `hour` or `day`
* `--start START` — Optional start date (YYYY-MM-DD)
* `--end END` — Optional end date (YYYY-MM-DD)
* `--verbose` — Enable detailed log output
* `--check` — Only check ID matching between metadata and measurements, do not export CSV
* `--debug-ids` — Print debug info about station ID matching

---

### `enrich_eea_data.py`

Combines filtered measurements with station metadata & vocabulary labels.

**Options:**

* `--input INPUT` — Input CSV produced by `extract_eea_bbox.py`
* `--output OUTPUT` — Output file for enriched data (default: chosen in script)
* `--metadata METADATA` — Path to station metadata CSV
* `--vocab-dir VOCAB_DIR` — Directory containing the vocabulary JSON files
* `--verbose` — Enable verbose output

---

## Notes

* A valid email is required for the EEA API.
* Coordinates for bounding box are in **WGS84** (lon, lat).
* Timestamps in downloaded data are in **UTC**, unless otherwise documented.
* For API details and code lists, consult the [EEA Air Quality API Documentation](https://eeadmz1-downloads-webapp.azurewebsites.net/content/documentation/How_To_Downloads.pdf).

---

## Contributing

If you'd like to add new features — e.g. custom filters, new visualizations, automated reporting — please:

1. Fork the repository
2. Create a feature branch: `git checkout -b my-feature`
3. Make your changes, add tests under `src/portability_tests/` if relevant
4. Submit a Pull Request
5. Ensure all tests pass before merging

