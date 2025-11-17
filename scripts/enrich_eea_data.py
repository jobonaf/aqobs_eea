#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrich_eea_data.py
Description: Enriches extracted EEA data with station metadata and vocabulary labels
Author: Giovanni Bonafè | ARPA-FVG
Created: 2025-11-04
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
import re

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from eea_vocabularies import EEAVocabularies, clean_samplingpoint_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

UUID_PATTERN = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', re.I)


def is_uuid_column(column_name: str, column_data: pd.Series) -> bool:
    """Check if a column contains UUID values."""
    if column_data.dtype != 'object':
        return False

    sample_values = column_data.dropna().head(10)
    if sample_values.empty:
        return False

    uuid_count = sum(1 for val in sample_values if UUID_PATTERN.match(str(val)))
    return uuid_count > len(sample_values) * 0.8


def load_metadata(metadata_path: str | Path) -> pd.DataFrame:
    """Load station metadata and create clean ID mapping."""
    metadata_path = Path(metadata_path)
    logger.info("Loading station metadata from %s", metadata_path)

    metadata = pd.read_csv(metadata_path, low_memory=False)
    if 'Sampling Point Id' not in metadata.columns:
        raise KeyError("Metadata file missing 'Sampling Point Id' column")

    metadata['SamplingPoint_clean'] = metadata['Sampling Point Id'].apply(clean_samplingpoint_id)

    useful_cols = [
        'SamplingPoint_clean',
        'Air Quality Station EoI Code',
        'Air Quality Station Name',
        'Longitude',
        'Latitude',
        'Air Quality Network',
        'Air Quality Station Area',
        'Countrycode',
    ]
    available_cols = [col for col in useful_cols if col in metadata.columns]
    metadata = metadata[available_cols].drop_duplicates('SamplingPoint_clean')

    logger.info("Loaded metadata for %d unique stations", len(metadata))
    return metadata


def build_vocab_manager(vocab_dir: str | Path) -> EEAVocabularies:
    manager = EEAVocabularies(cache_dir=Path(vocab_dir), auto_load=False)
    loaded = []
    for vocab in ("pollutant", "unit", "quality_flag"):
        if manager.load_vocabulary(vocab):
            loaded.append(vocab)
    if loaded:
        logger.info("Loaded vocabularies: %s", ", ".join(loaded))
    else:
        logger.warning("No vocabularies were loaded from %s", vocab_dir)
    return manager


def log_field_coverage(label: str, series: pd.Series | None) -> None:
    if series is None:
        return
    total = len(series)
    if total == 0:
        return
    available = series.notna().sum()
    logger.info("%s coverage: %.1f%% (%d/%d)", label, (available / total) * 100, available, total)


def enrich_data(input_csv: str, metadata_path: str, vocab_dir: str, output_csv: str) -> pd.DataFrame:
    """Main function to enrich EEA data with metadata and vocabulary labels."""
    logger.info("Loading input data from %s", input_csv)
    data = pd.read_csv(input_csv)
    logger.info("Loaded %d measurements", len(data))

    if 'Samplingpoint' not in data.columns:
        raise KeyError("Input data does not contain the 'Samplingpoint' column")

    uuid_columns = []
    for col in data.columns:
        if is_uuid_column(col, data[col]):
            uuid_columns.append(col)
            logger.info("Identified UUID column: %s", col)

    if uuid_columns:
        data = data.drop(columns=uuid_columns)
        logger.info("Removed %d UUID columns: %s", len(uuid_columns), ", ".join(uuid_columns))

    logger.info("Creating clean station IDs for joining...")
    data['SamplingPoint_clean'] = data['Samplingpoint'].apply(clean_samplingpoint_id)

    metadata = load_metadata(metadata_path)
    vocab_manager = build_vocab_manager(vocab_dir)

    pollutant_label_map = vocab_manager.get_label_mapping('pollutant')
    pollutant_notation_map = vocab_manager.get_notation_mapping('pollutant')
    unit_label_map = vocab_manager.get_label_mapping('unit')
    quality_flag_map = vocab_manager.get_label_mapping('quality_flag')

    logger.info("Enriching with station metadata...")
    data_enriched = data.merge(
        metadata,
        on='SamplingPoint_clean',
        how='left',
        suffixes=('', '_meta'),
    )

    logger.info("Adding pollutant information...")
    if pollutant_label_map:
        data_enriched['Pollutant_Name'] = data_enriched['Pollutant'].astype(str).map(pollutant_label_map)
    else:
        logger.warning("Pollutant vocabulary not available; skipping pollutant labels")

    if pollutant_notation_map:
        data_enriched['Pollutant_Code'] = data_enriched['Pollutant'].astype(str).map(pollutant_notation_map)

    if 'Unit' in data_enriched.columns and unit_label_map:
        data_enriched['Unit_Label'] = data_enriched['Unit'].astype(str).map(unit_label_map)

    if 'Verification' in data_enriched.columns and quality_flag_map:
        data_enriched['Verification_Label'] = (
            data_enriched['Verification'].astype(str).map(quality_flag_map)
        )

    base_cols = ['Samplingpoint', 'SamplingPoint_clean', 'Pollutant', 'Pollutant_Code', 'Pollutant_Name']
    station_cols = ['Air Quality Station EoI Code', 'Air Quality Station Name', 'Longitude', 'Latitude']
    measurement_cols = ['Start', 'End', 'Value', 'Unit', 'Unit_Label']
    quality_cols = ['Validity', 'Verification', 'Verification_Label', 'DataCapture']

    other_cols = [
        col
        for col in data_enriched.columns
        if col not in base_cols + station_cols + measurement_cols + quality_cols
        and not col.endswith('_meta')
        and not is_uuid_column(col, data_enriched[col])
    ]

    final_cols = []
    for group in (base_cols, station_cols, measurement_cols, quality_cols, other_cols):
        final_cols.extend([col for col in group if col in data_enriched.columns])

    data_enriched = data_enriched[final_cols]

    logger.info("Saving enriched data to %s", output_csv)
    data_enriched.to_csv(output_csv, index=False)

    logger.info("=" * 50)
    logger.info("ENRICHMENT SUMMARY")
    logger.info("=" * 50)
    logger.info("Input records: %d", len(data))
    logger.info("Output records: %d", len(data_enriched))

    if 'Air Quality Station Name' in data_enriched.columns:
        log_field_coverage("Station metadata", data_enriched['Air Quality Station Name'])
    if 'Pollutant_Name' in data_enriched.columns:
        log_field_coverage("Pollutant names", data_enriched['Pollutant_Name'])
    if 'Pollutant_Code' in data_enriched.columns:
        log_field_coverage("Pollutant codes", data_enriched['Pollutant_Code'])
    if 'Unit_Label' in data_enriched.columns:
        log_field_coverage("Unit labels", data_enriched['Unit_Label'])
    if 'Verification_Label' in data_enriched.columns:
        log_field_coverage("Quality flag labels", data_enriched['Verification_Label'])

    if 'Pollutant_Code' in data_enriched.columns:
        unique_pollutants = data_enriched[
            ['Pollutant', 'Pollutant_Code', 'Pollutant_Name']
        ].drop_duplicates()
        logger.info("Pollutants found:")
        for _, row in unique_pollutants.iterrows():
            logger.info(
                "  Code %s -> %s (%s)",
                row['Pollutant'],
                row['Pollutant_Code'] if pd.notna(row['Pollutant_Code']) else 'MISSING',
                row['Pollutant_Name'] if pd.notna(row['Pollutant_Name']) else 'MISSING',
            )

    logger.info("=" * 50)
    return data_enriched


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich EEA data with station metadata and vocabulary labels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input eea_bbox.csv --output eea_enriched.csv
  %(prog)s --input eea_bbox.csv --metadata metadata/stations_metadata.csv --vocab-dir eea_vocabularies --verbose
        """,
    )

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input CSV file with extracted EEA data",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="eea_enriched.csv",
        help="Output CSV file for enriched data (default: %(default)s)",
    )
    parser.add_argument(
        "--metadata",
        type=str,
        default="metadata/stations_metadata.csv",
        help="Station metadata CSV file (default: %(default)s)",
    )
    parser.add_argument(
        "--vocab-dir",
        type=str,
        default="eea_vocabularies",
        help="Directory with vocabulary JSON files (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        enrich_data(
            input_csv=str(input_path),
            metadata_path=args.metadata,
            vocab_dir=args.vocab_dir,
            output_csv=str(output_path),
        )
        logger.info("✓ Enrichment completed successfully: %s", output_path)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Enrichment failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
