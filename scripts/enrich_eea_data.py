#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrich_eea_data.py
Description: Enriches extracted EEA data with station metadata and vocabulary labels
Author: Giovanni Bonafè | ARPA-FVG
Created: 2025-11-04
"""

import pandas as pd
import json
import argparse
import logging
from pathlib import Path
import sys
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def clean_samplingpoint_id(sp_id):
    """
    Keep only the 'core' station ID, removing measurement type, number, date suffix.
    Same logic as extract_eea_bbox.py
    """
    if pd.isna(sp_id):
        return sp_id
    # Remove country prefix if present
    sp_id = sp_id.split('/')[-1]
    # Keep only the first part before first underscore (the station core)
    sp_id = sp_id.split('_')[0]
    return sp_id

def load_vocabulary(vocab_dir, vocab_type):
    """Load vocabulary from JSON file and create code-to-label and code-to-notation mappings"""
    vocab_path = Path(vocab_dir) / f"{vocab_type}.json"
    
    if not vocab_path.exists():
        logger.warning(f"Vocabulary file not found: {vocab_path}")
        return {}, {}
    
    try:
        with open(vocab_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract concepts and create mappings
        id_to_label = {}
        id_to_notation = {}
        
        for concept in data.get('concepts', []):
            concept_id = concept.get('@id')
            pref_label = concept.get('prefLabel', [{}])[0].get('@value', '')
            notation = concept.get('Notation', '')
            
            if concept_id and pref_label:
                id_to_label[concept_id] = pref_label
                if notation:
                    id_to_notation[concept_id] = notation
        
        logger.info(f"Loaded {len(id_to_label)} entries from {vocab_type} vocabulary")
        return id_to_label, id_to_notation
        
    except Exception as e:
        logger.error(f"Error loading vocabulary {vocab_type}: {e}")
        return {}, {}

def is_uuid_column(column_name, column_data):
    """Check if a column contains UUID values"""
    if not column_data.dtype == 'object':
        return False
    
    # Sample some values to check for UUID pattern
    sample_values = column_data.dropna().head(10)
    if len(sample_values) == 0:
        return False
    
    uuid_pattern = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', re.I)
    
    uuid_count = sum(1 for val in sample_values if uuid_pattern.match(str(val)))
    return uuid_count > len(sample_values) * 0.8  # If >80% of samples look like UUIDs

def load_metadata(metadata_path):
    """Load station metadata and create clean ID mapping"""
    logger.info(f"Loading station metadata from {metadata_path}")
    
    try:
        metadata = pd.read_csv(metadata_path, low_memory=False)
        
        # Create clean ID for joining (same logic as extraction script)
        metadata['SamplingPoint_clean'] = metadata['Sampling Point Id'].apply(clean_samplingpoint_id)
        
        # Select relevant columns for enrichment
        useful_cols = [
            'SamplingPoint_clean', 'Air Quality Station EoI Code', 
            'Air Quality Station Name', 'Longitude', 'Latitude',
            'Air Quality Network', 'Air Quality Station Area', 'Countrycode'
        ]
        
        # Keep only columns that exist
        available_cols = [col for col in useful_cols if col in metadata.columns]
        metadata = metadata[available_cols].drop_duplicates('SamplingPoint_clean')
        
        logger.info(f"Loaded metadata for {len(metadata)} unique stations")
        return metadata
        
    except Exception as e:
        logger.error(f"Error loading metadata: {e}")
        raise

def enrich_data(input_csv, metadata_path, vocab_dir, output_csv):
    """Main function to enrich EEA data with metadata and vocabulary labels"""
    
    # Load input data
    logger.info(f"Loading input data from {input_csv}")
    data = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(data)} measurements")
    
    # Identify and remove UUID columns before processing
    uuid_columns = []
    for col in data.columns:
        if is_uuid_column(col, data[col]):
            uuid_columns.append(col)
            logger.info(f"Identified UUID column: {col}")
    
    if uuid_columns:
        data = data.drop(columns=uuid_columns)
        logger.info(f"Removed {len(uuid_columns)} UUID columns: {', '.join(uuid_columns)}")
    
    # Create clean ID in input data for joining
    logger.info("Creating clean station IDs for joining...")
    data['SamplingPoint_clean'] = data['Samplingpoint'].apply(clean_samplingpoint_id)
    
    # Load metadata
    metadata = load_metadata(metadata_path)
    
    # Load vocabularies
    logger.info("Loading vocabularies...")
    pollutant_label_map, pollutant_notation_map = load_vocabulary(vocab_dir, 'pollutant')
    unit_map, _ = load_vocabulary(vocab_dir, 'unit')
    quality_flag_map, _ = load_vocabulary(vocab_dir, 'quality_flag')
    
    # Enrich with station metadata
    logger.info("Enriching with station metadata...")
    data_enriched = data.merge(
        metadata, 
        on='SamplingPoint_clean', 
        how='left',
        suffixes=('', '_meta')
    )
    
    # Add pollutant names and codes (only from vocabulary, no fallback)
    logger.info("Adding pollutant information...")
    data_enriched['Pollutant_Name'] = data_enriched['Pollutant'].astype(str).map(pollutant_label_map)
    data_enriched['Pollutant_Code'] = data_enriched['Pollutant'].astype(str).map(pollutant_notation_map)
    
    # Add unit labels if available
    if 'Unit' in data_enriched.columns:
        data_enriched['Unit_Label'] = data_enriched['Unit'].map(unit_map)
    
    # Add quality flag labels if available
    if 'Verification' in data_enriched.columns:
        data_enriched['Verification_Label'] = data_enriched['Verification'].astype(str).map(quality_flag_map)
    
    # Reorder columns for better readability
    base_cols = ['Samplingpoint', 'Samplingpoint_clean', 'Pollutant', 'Pollutant_Code', 'Pollutant_Name']
    station_cols = ['Air Quality Station EoI Code', 'Air Quality Station Name', 'Longitude', 'Latitude']
    measurement_cols = ['Start', 'End', 'Value', 'Unit', 'Unit_Label']
    quality_cols = ['Validity', 'Verification', 'Verification_Label', 'DataCapture']
    
    # Collect all other columns (excluding UUID columns and metadata duplicates)
    other_cols = [col for col in data_enriched.columns 
                  if col not in base_cols + station_cols + measurement_cols + quality_cols
                  and not col.endswith('_meta')
                  and not is_uuid_column(col, data_enriched[col])]
    
    # Ensure all columns exist before reordering
    final_cols = []
    for col_group in [base_cols, station_cols, measurement_cols, quality_cols, other_cols]:
        existing_cols = [col for col in col_group if col in data_enriched.columns]
        final_cols.extend(existing_cols)
    
    data_enriched = data_enriched[final_cols]
    
    # Save enriched data
    logger.info(f"Saving enriched data to {output_csv}")
    data_enriched.to_csv(output_csv, index=False)
    
    # Summary
    logger.info("=" * 50)
    logger.info("ENRICHMENT SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Input records: {len(data)}")
    logger.info(f"Output records: {len(data_enriched)}")
    
    # Count stations with metadata
    if 'Air Quality Station Name' in data_enriched.columns:
        stations_with_metadata = data_enriched['Air Quality Station Name'].notna().sum()
        logger.info(f"Records with station metadata: {stations_with_metadata}")
    
    # Count pollutants with names and codes
    if 'Pollutant_Name' in data_enriched.columns:
        pollutants_with_names = data_enriched['Pollutant_Name'].notna().sum()
        logger.info(f"Records with pollutant names: {pollutants_with_names}")
    
    if 'Pollutant_Code' in data_enriched.columns:
        pollutants_with_codes = data_enriched['Pollutant_Code'].notna().sum()
        logger.info(f"Records with pollutant codes: {pollutants_with_codes}")
    
    # Show unique pollutants found with their codes
    if 'Pollutant_Code' in data_enriched.columns:
        unique_pollutants = data_enriched[['Pollutant', 'Pollutant_Code', 'Pollutant_Name']].drop_duplicates()
        logger.info("Pollutants found:")
        for _, row in unique_pollutants.iterrows():
            code_display = row['Pollutant_Code'] if pd.notna(row['Pollutant_Code']) else "MISSING"
            name_display = row['Pollutant_Name'] if pd.notna(row['Pollutant_Name']) else "MISSING"
            logger.info(f"  Code {row['Pollutant']} -> {code_display} ({name_display})")
    
    logger.info("=" * 50)
    
    return data_enriched

def main():
    parser = argparse.ArgumentParser(
        description="Enrich EEA data with station metadata and vocabulary labels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input eea_bbox.csv --output eea_enriched.csv
  %(prog)s --input eea_bbox.csv --metadata metadata/stations_metadata.csv --vocab-dir eea_vocabularies --verbose
        """
    )
    
    parser.add_argument("--input", type=str, required=True,
                       help="Input CSV file with extracted EEA data")
    
    parser.add_argument("--output", type=str, default="eea_enriched.csv",
                       help="Output CSV file for enriched data (default: %(default)s)")
    
    parser.add_argument("--metadata", type=str, default="metadata/stations_metadata.csv",
                       help="Station metadata CSV file (default: %(default)s)")
    
    parser.add_argument("--vocab-dir", type=str, default="eea_vocabularies",
                       help="Directory with vocabulary JSON files (default: %(default)s)")
    
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Check if input file exists
        if not Path(args.input).exists():
            logger.error(f"Input file not found: {args.input}")
            sys.exit(1)
        
        # Create output directory if needed
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Run enrichment
        enrich_data(
            input_csv=args.input,
            metadata_path=args.metadata,
            vocab_dir=args.vocab_dir,
            output_csv=args.output
        )
        
        logger.info(f"✓ Enrichment completed successfully: {args.output}")
        
    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
