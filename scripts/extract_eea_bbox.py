#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_eea_bbox.py
Description: Extracts EEA air quality measurements for stations within a bounding box,
             optionally filtering by pollutants and time range, and writes a single CSV.
Author: Giovanni Bonafè | ARPA-FVG
Created: 2025-11-04
"""

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import sys
import re

def extract_station_id(sp_id):
    """
    Extract station ID from Samplingpoint ID.
    Format: "IT/SPO.IT1823A_5_BETA_2016-10-13_00:00:00" -> "IT1823A"
    """
    if pd.isna(sp_id):
        return sp_id
    
    sp_id = str(sp_id)
    
    # Pattern for IT/SPO.IT1823A_5_BETA_2016-10-13_00:00:00
    match = re.search(r'[A-Z]{2}/SPO\.([A-Z]{2}\d+[A-Z]?)_', sp_id)
    if match:
        return match.group(1)
    
    # Fallback pattern for other countries
    match = re.search(r'([A-Z]{2}\d+[A-Z]?)_', sp_id)
    if match:
        return match.group(1)
    
    # Ultimate fallback: try to extract country code + station number
    match = re.search(r'([A-Z]{2}\d+[A-Z]?)', sp_id)
    if match:
        return match.group(1)
    
    return sp_id

def load_metadata(path, verbose=False):
    """Load and validate station metadata."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Metadata file not found: {path}")
    
    print(f"Loading station metadata from {path}...")
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        raise ValueError(f"Error reading metadata file {path}: {e}")
    
    # Check required columns
    required_cols = ['Sampling Point Id', 'Longitude', 'Latitude', 'Air Quality Station EoI Code', 'Air Pollutant']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Metadata file missing required columns: {missing_cols}")
    
    # Create cleaned station IDs from Sampling Point Id
    df['Station_clean'] = df['Sampling Point Id'].apply(extract_station_id)
    
    if verbose:
        print(f"Loaded {len(df)} stations from metadata")
        print(f"Sample Station ID conversions:")
        sample_data = df[['Sampling Point Id', 'Station_clean', 'Air Quality Station EoI Code']].head(10)
        for _, row in sample_data.iterrows():
            print(f"  '{row['Sampling Point Id']}' -> '{row['Station_clean']}' (EoI: '{row['Air Quality Station EoI Code']}')")
        print(f"Unique pollutants in metadata: {df['Air Pollutant'].unique()}")
    
    return df

def filter_stations_bbox(df, bbox, verbose=False):
    """Filter stations within bounding box with validation."""
    min_lon, max_lon, min_lat, max_lat = bbox
    
    # Validate bbox coordinates
    if min_lon >= max_lon:
        raise ValueError(f"Invalid longitude range: min_lon ({min_lon}) >= max_lon ({max_lon})")
    if min_lat >= max_lat:
        raise ValueError(f"Invalid latitude range: min_lat ({min_lat}) >= max_lat ({max_lat})")
    
    print(f"Filtering stations within bbox [{min_lon:.4f}, {max_lon:.4f}, {min_lat:.4f}, {max_lat:.4f}]...")
    
    df_filtered = df[
        (df['Longitude'] >= min_lon) & 
        (df['Longitude'] <= max_lon) &
        (df['Latitude'] >= min_lat) & 
        (df['Latitude'] <= max_lat)
    ].copy()
    
    print(f"{len(df_filtered)} stations selected.")
    
    if verbose and len(df_filtered) > 0:
        print(f"First 10 Station IDs in bbox: {df_filtered['Station_clean'].head(10).tolist()}")
        print(f"Pollutants in bbox: {df_filtered['Air Pollutant'].unique().tolist()}")
    
    return df_filtered

def map_pollutant_codes(pollutants):
    """
    Map pollutant names to codes used in parquet files.
    Based on EEA documentation and common codes.
    """
    pollutant_map = {
        'PM10': 5,
        'PM2.5': 6001,
        'NO2': 8,
        'O3': 7,
        'SO2': 1,
        'CO': 10,
        'NO': 38,
        'NOX': 9,
        'BENZENE': 20,
        'C6H6': 20,
    }
    
    if pollutants is None:
        return None
    
    mapped = []
    for poll in pollutants:
        if poll.upper() in pollutant_map:
            mapped.append(pollutant_map[poll.upper()])
        else:
            # Try to convert directly to int if it's already a code
            try:
                mapped.append(int(poll))
            except ValueError:
                print(f"Warning: Unknown pollutant '{poll}', using as-is")
                mapped.append(poll)
    
    return mapped

def process_parquet_file(path, station_ids, pollutants=None, start=None, end=None, verbose=False):
    """Process a single parquet file."""
    try:
        df_parquet = pq.read_table(path).to_pandas()
    except Exception as e:
        print(f"Warning: Could not read parquet file {path}: {e}")
        return pd.DataFrame()
    
    # Check for required columns
    if 'Samplingpoint' not in df_parquet.columns:
        print(f"Warning: No Samplingpoint column in {path.name}")
        return pd.DataFrame()
    
    # Extract station IDs from Samplingpoint
    df_parquet['Station_clean'] = df_parquet['Samplingpoint'].apply(extract_station_id)

    if verbose:
        print(f"  {len(df_parquet)} records in {path.name}")
        unique_station_ids = df_parquet['Station_clean'].unique().tolist()
        print(f"  Station IDs in file: {unique_station_ids}")

    # Filter by cleaned Station ID
    df_filtered = df_parquet[df_parquet['Station_clean'].isin(station_ids)].copy()
    
    if df_filtered.empty:
        if verbose:
            print(f"  No matching stations found in {path.name}")
        return df_filtered
    
    # Optional pollutant filter - map names to codes
    if pollutants is not None and 'Pollutant' in df_filtered.columns:
        before_filter = len(df_filtered)
        df_filtered = df_filtered[df_filtered['Pollutant'].isin(pollutants)]
        if verbose:
            print(f"  Pollutant filter: {before_filter} -> {len(df_filtered)} records")
            if not df_filtered.empty:
                print(f"  Pollutants found: {df_filtered['Pollutant'].unique().tolist()}")
    
    # Optional time filter
    if start is not None and 'End' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['End'] >= start]
    if end is not None and 'Start' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['Start'] <= end]
    
    if verbose and not df_filtered.empty:
        matching_ids = df_filtered['Station_clean'].unique().tolist()
        print(f"  Matching Station IDs: {matching_ids}")
        print(f"  Found {len(df_filtered)} matching records")
    
    return df_filtered

def debug_id_matching(metadata_df, parquet_dirs, bbox, pollutants=None, verbose=False):
    """Debug function to analyze ID matching issues."""
    print("\n" + "=" * 60)
    print("DEBUG ID MATCHING ANALYSIS")
    print("=" * 60)
    
    # Filter stations in bbox
    filtered_stations = filter_stations_bbox(metadata_df, bbox, verbose=verbose)
    metadata_station_ids = set(filtered_stations['Station_clean'].tolist())
    
    print(f"\nMetadata stations in bbox: {len(metadata_station_ids)}")
    if verbose:
        print(f"Metadata Station IDs: {sorted(metadata_station_ids)}")
        print(f"Pollutants in metadata: {filtered_stations['Air Pollutant'].unique().tolist()}")
    
    # Collect all parquet station IDs from all directories
    all_parquet_station_ids = set()
    parquet_files_by_dir = {}
    
    for parquet_dir in parquet_dirs:
        parquet_files = list(Path(parquet_dir).glob("*.parquet"))
        parquet_files_by_dir[parquet_dir] = []
        
        for pq_file in parquet_files[:10]:  # Sample first 10 files per directory
            try:
                df_parquet = pq.read_table(pq_file).to_pandas()
                if 'Samplingpoint' in df_parquet.columns:
                    parquet_station_ids = set(df_parquet['Samplingpoint'].apply(extract_station_id).tolist())
                    all_parquet_station_ids.update(parquet_station_ids)
                    parquet_files_by_dir[parquet_dir].append((pq_file.name, parquet_station_ids))
                    
            except Exception as e:
                print(f"  Error reading {pq_file}: {e}")
    
    print(f"\nTotal unique parquet Station IDs: {len(all_parquet_station_ids)}")
    
    # Analyze matches
    matching_ids = metadata_station_ids.intersection(all_parquet_station_ids)
    missing_in_parquet = metadata_station_ids - all_parquet_station_ids
    extra_in_parquet = all_parquet_station_ids - metadata_station_ids
    
    print(f"\nMatching Station IDs: {len(matching_ids)}")
    if matching_ids:
        print(f"  Matching: {sorted(matching_ids)}")
    
    print(f"Station IDs in metadata but not in parquet files: {len(missing_in_parquet)}")
    if missing_in_parquet:
        print(f"  Missing: {sorted(missing_in_parquet)}")
    
    print(f"Station IDs in parquet files but not in metadata: {len(extra_in_parquet)}")
    
    # Analysis by directory
    print(f"\nAnalysis by directory:")
    for dir_name, files in parquet_files_by_dir.items():
        dir_ids = set()
        for pq_file, pq_ids in files:
            dir_ids.update(pq_ids)
        
        dir_matches = metadata_station_ids.intersection(dir_ids)
        print(f"  {Path(dir_name).name}: {len(files)} files sampled, {len(dir_ids)} IDs, {len(dir_matches)} matches")

def main():
    parser = argparse.ArgumentParser(
        description="Extract EEA air quality measurements for stations within a bounding box",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--indir", type=str, nargs='+', default=["eea_hourly", "eea_daily"], 
                       help="Input folder(s) with Parquet files (default: %(default)s)")
    parser.add_argument("--metadata", type=str, default="metadata/stations_metadata.csv", 
                       help="Station metadata CSV file (default: %(default)s)")
    parser.add_argument("--out", type=str, default="eea_bbox.csv", 
                       help="Output CSV file (default: %(default)s)")
    parser.add_argument("--bbox", type=float, nargs=4, metavar=('MIN_LON', 'MAX_LON', 'MIN_LAT', 'MAX_LAT'),
                       default=[12.3, 13.95, 45.58, 46.67],
                       help="Bounding box coordinates (default: %(default)s)")
    parser.add_argument("--pollutants", nargs="+", default=None, 
                       help="Optional list of pollutant codes or names (e.g., NO2 PM10 O3 or 8 5 7)")
    parser.add_argument("--start", type=str, default=None, 
                       help="Optional start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, 
                       help="Optional end date (YYYY-MM-DD)")
    parser.add_argument("--verbose", action="store_true", 
                       help="Enable verbose output")
    parser.add_argument("--check", action="store_true", 
                       help="Check only ID matches without exporting CSV")
    parser.add_argument("--debug-ids", action="store_true",
                       help="Debug ID matching between metadata and parquet files")
    
    args = parser.parse_args()

    try:
        # Load and filter metadata
        metadata_df = load_metadata(args.metadata, verbose=args.verbose)
        filtered_stations = filter_stations_bbox(metadata_df, args.bbox, verbose=args.verbose)
        
        if len(filtered_stations) == 0:
            print("No stations found in the specified bounding box.")
            return
        
        # Use Station IDs for matching
        station_ids = filtered_stations['Station_clean'].unique().tolist()
        print(f"\nTarget station IDs: {len(station_ids)} stations")
        if args.verbose:
            print(f"Target stations: {station_ids}")

        # Map pollutant names to codes if provided
        pollutant_codes = map_pollutant_codes(args.pollutants)
        if args.pollutants and pollutant_codes:
            print(f"Pollutants filter: {args.pollutants} -> codes {pollutant_codes}")

        # Debug ID matching if requested
        if args.debug_ids:
            debug_id_matching(metadata_df, args.indir, args.bbox, pollutants=pollutant_codes, verbose=args.verbose)
            return

        # Process all input directories
        all_data = []
        total_files_processed = 0
        
        for input_dir in args.indir:
            indir_path = Path(input_dir)
            if not indir_path.exists():
                print(f"Warning: Input directory not found: {input_dir}")
                continue
            
            parquet_files = list(indir_path.glob("*.parquet"))
            print(f"\nProcessing {len(parquet_files)} files from {input_dir}...")
            
            for pq_file in sorted(parquet_files):
                if args.verbose:
                    print(f"Processing {pq_file.name}...")
                
                df_filtered = process_parquet_file(
                    pq_file, station_ids,
                    pollutants=pollutant_codes,
                    start=args.start, end=args.end,
                    verbose=args.verbose
                )
                
                if not df_filtered.empty:
                    all_data.append(df_filtered)
                
                total_files_processed += 1

        print(f"\nProcessed {total_files_processed} files total")

        # Output results
        if args.check:
            print("\n" + "=" * 50)
            print("CHECK RESULTS:")
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                total_matching_ids = combined_data['Station_clean'].unique().tolist()
                print(f"✓ Found {len(total_matching_ids)} matching Station IDs out of {len(station_ids)} target stations")
                print(f"✓ Total records found: {len(combined_data)}")
                
                missing_ids = [sid for sid in station_ids if sid not in total_matching_ids]
                if missing_ids:
                    print(f"✗ Missing Station IDs: {len(missing_ids)} stations")
                    if args.verbose:
                        print(f"  Missing: {missing_ids}")
                
                if 'Pollutant' in combined_data.columns:
                    pollutants_found = combined_data['Pollutant'].unique().tolist()
                    print(f"✓ Pollutants found: {pollutants_found}")
            else:
                print("✗ No matching records found for any station")
            return

        # Export data
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            
            # Create output directory if needed
            output_path = Path(args.out)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            result_df.to_csv(args.out, index=False)
            print(f"\n✓ Successfully saved {len(result_df)} records to {args.out}")
            
            # Summary statistics
            unique_stations = result_df['Station_clean'].nunique()
            print(f"✓ Data covers {unique_stations} unique stations")
            
            if 'Pollutant' in result_df.columns:
                pollutants_found = result_df['Pollutant'].unique()
                print(f"✓ Pollutants found: {', '.join(map(str, pollutants_found))}")
                
        else:
            print("\n✗ No data found for selected stations and filters")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
