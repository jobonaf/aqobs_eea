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
    """Clean station ID for matching."""
    if pd.isna(sp_id):
        return sp_id
    sp_id = sp_id.split('/')[-1]
    sp_id = sp_id.split('_')[0]
    return sp_id

def load_vocabulary(vocab_dir, vocab_type):
    """Load vocabulary from JSON file and return both label and notation mappings."""
    vocab_path = Path(vocab_dir) / f"{vocab_type}.json"
    
    if not vocab_path.exists():
        logger.warning(f"Vocabulary file not found: {vocab_path}")
        return {}, {}
    
    try:
        with open(vocab_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        label_map = {}
        notation_map = {}
        for concept in data.get('concepts', []):
            concept_id = concept.get('@id')
            pref_label = concept.get('prefLabel', [{}])[0].get('@value', '')
            notation = concept.get('Notation', '')
            
            if concept_id and pref_label:
                label_map[concept_id] = pref_label
                if notation:
                    notation_map[concept_id] = notation
        
        logger.info(f"Loaded {len(label_map)} entries from {vocab_type} vocabulary")
        return label_map, notation_map
        
    except Exception as e:
        logger.error(f"Error loading vocabulary {vocab_type}: {e}")
        return {}, {}

def is_uuid_column(column_name, column_data):
    """Check if column contains UUIDs."""
    if column_data.dtype != 'object':
        return False
    
    sample_values = column_data.dropna().head(10)
    if len(sample_values) == 0:
        return False
    
    uuid_pattern = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', re.I)
    uuid_count = sum(1 for val in sample_values if uuid_pattern.match(str(val)))
    return uuid_count > len(sample_values) * 0.8

def load_metadata(metadata_path):
    """Load station metadata."""
    logger.info(f"Loading station metadata from {metadata_path}")
    
    try:
        metadata = pd.read_csv(metadata_path, low_memory=False)
        metadata['SamplingPoint_clean'] = metadata['Sampling Point Id'].apply(clean_samplingpoint_id)
        
        useful_cols = [
            'SamplingPoint_clean', 'Air Quality Station EoI Code', 
            'Air Quality Station Name', 'Longitude', 'Latitude',
            'Air Quality Network', 'Air Quality Station Area', 'Countrycode'
        ]
        
        available_cols = [col for col in useful_cols if col in metadata.columns]
        metadata = metadata[available_cols].drop_duplicates('SamplingPoint_clean')
        
        logger.info(f"Loaded metadata for {len(metadata)} unique stations")
        return metadata
        
    except Exception as e:
        logger.error(f"Error loading metadata: {e}")
        raise

def enrich_data(input_csv, metadata_path, vocab_dir, output_csv, verbose=False):
    """Main enrichment function."""
    
    # Load input data
    logger.info(f"Loading input data from {input_csv}")
    data = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(data)} measurements")
    
    # Remove UUID columns
    uuid_columns = []
    for col in data.columns:
        if is_uuid_column(col, data[col]):
            uuid_columns.append(col)
    
    if uuid_columns:
        data = data.drop(columns=uuid_columns)
        logger.info(f"Removed UUID columns: {', '.join(uuid_columns)}")
    
    # Create clean IDs for joining
    data['SamplingPoint_clean'] = data['Samplingpoint'].apply(clean_samplingpoint_id)
    
    # Load metadata
    metadata = load_metadata(metadata_path)
    
    # Load vocabularies
    logger.info("Loading vocabularies...")
    pollutant_label_map, pollutant_notation_map = load_vocabulary(vocab_dir, 'pollutant')
    unit_label_map, _ = load_vocabulary(vocab_dir, 'unit')
    quality_flag_map, _ = load_vocabulary(vocab_dir, 'quality_flag')
    
    # Debug: check pollutant mapping
    if verbose and pollutant_label_map:
        if '5' in pollutant_label_map:
            logger.info(f"Found PM10 (code '5'): {pollutant_label_map['5']}")
    
    # Merge with metadata
    logger.info("Enriching with station metadata...")
    data_enriched = data.merge(
        metadata, 
        on='SamplingPoint_clean', 
        how='left',
        suffixes=('', '_meta')
    )
    
    # Add pollutant information - CONVERT TO STRING FOR MAPPING
    logger.info("Adding pollutant information...")
    pollutant_codes_str = data_enriched['Pollutant'].astype(str)
    data_enriched['Pollutant_Name'] = pollutant_codes_str.map(pollutant_label_map)
    data_enriched['Pollutant_Code'] = pollutant_codes_str.map(pollutant_notation_map)
    
    # Add unit labels
    if 'Unit' in data_enriched.columns and unit_label_map:
        data_enriched['Unit_Label'] = data_enriched['Unit'].map(unit_label_map)
    
    # Add quality flag labels  
    if 'Verification' in data_enriched.columns and quality_flag_map:
        data_enriched['Verification_Label'] = data_enriched['Verification'].astype(str).map(quality_flag_map)
    
    # Reorder columns - MANTIENI LA STRUTTURA ORIGINALE
    base_cols = ['Samplingpoint', 'Samplingpoint_clean', 'Pollutant', 'Pollutant_Code', 'Pollutant_Name']
    station_cols = ['Air Quality Station EoI Code', 'Air Quality Station Name', 'Longitude', 'Latitude']
    measurement_cols = ['Start', 'End', 'Value', 'Unit', 'Unit_Label']
    quality_cols = ['Validity', 'Verification', 'Verification_Label', 'DataCapture', 'AggType', 'ResultTime']
    
    other_cols = [col for col in data_enriched.columns 
                  if col not in base_cols + station_cols + measurement_cols + quality_cols
                  and not col.endswith('_meta')]
    
    final_cols = []
    for col_group in [base_cols, station_cols, measurement_cols, quality_cols, other_cols]:
        existing_cols = [col for col in col_group if col in data_enriched.columns]
        final_cols.extend(existing_cols)
    
    data_enriched = data_enriched[final_cols]
    
    # Save results
    logger.info(f"Saving enriched data to {output_csv}")
    data_enriched.to_csv(output_csv, index=False)
    
    # Summary
    logger.info("=" * 50)
    logger.info("ENRICHMENT SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Input records: {len(data)}")
    logger.info(f"Output records: {len(data_enriched)}")
    
    if 'Air Quality Station Name' in data_enriched.columns:
        stations_with_metadata = data_enriched['Air Quality Station Name'].notna().sum()
        logger.info(f"Records with station metadata: {stations_with_metadata}")
    
    if 'Pollutant_Name' in data_enriched.columns:
        pollutants_with_names = data_enriched['Pollutant_Name'].notna().sum()
        logger.info(f"Records with pollutant names: {pollutants_with_names}")
    
    if 'Pollutant_Code' in data_enriched.columns:
        pollutants_with_codes = data_enriched['Pollutant_Code'].notna().sum()
        logger.info(f"Records with pollutant codes: {pollutants_with_codes}")
    
    # Show unique pollutants
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
        description="Enrich EEA data with station metadata and vocabulary labels"
    )
    
    parser.add_argument("--input", type=str, required=True,
                       help="Input CSV file with extracted EEA data")
    
    parser.add_argument("--output", type=str, default="eea_enriched.csv",
                       help="Output CSV file for enriched data")
    
    parser.add_argument("--metadata", type=str, default="metadata/stations_metadata.csv",
                       help="Station metadata CSV file")
    
    parser.add_argument("--vocab-dir", type=str, default="eea_vocabularies",
                       help="Directory with vocabulary JSON files")
    
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if not Path(args.input).exists():
            logger.error(f"Input file not found: {args.input}")
            sys.exit(1)
        
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        enrich_data(
            input_csv=args.input,
            metadata_path=args.metadata,
            vocab_dir=args.vocab_dir,
            output_csv=args.output,
            verbose=args.verbose
        )
        
        logger.info(f"✓ Enrichment completed: {args.output}")
        
    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
