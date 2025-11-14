#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
explore_parquet_structure.py
Description: Explore the structure of EEA parquet files to understand ID formats
Author: Giovanni Bonafè
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import sys

def explore_parquet_files(indir, sample_size=5):
    """Explore the structure of parquet files."""
    indir_path = Path(indir)
    parquet_files = list(indir_path.glob("*.parquet"))
    
    print(f"Found {len(parquet_files)} parquet files in {indir}")
    print("=" * 80)
    
    for i, pq_file in enumerate(parquet_files[:sample_size]):
        print(f"\n{i+1}. Analyzing {pq_file.name}:")
        print("-" * 40)
        
        try:
            # Read first few rows to understand structure
            df = pq.read_table(pq_file).to_pandas()
            
            print(f"   Shape: {df.shape}")
            print(f"   Columns: {list(df.columns)}")
            
            # Check key columns
            key_cols = ['Samplingpoint', 'AirQualityStation', 'Pollutant']
            for col in key_cols:
                if col in df.columns:
                    unique_vals = df[col].unique()[:10]  # First 10 unique values
                    print(f"   {col} (first 10 unique): {unique_vals.tolist()}")
            
            # Show data types
            print(f"   Dtypes:")
            for col in df.columns:
                print(f"     {col}: {df[col].dtype}")
                
            # Show first few rows of key columns
            if 'Samplingpoint' in df.columns:
                print(f"   First 5 Samplingpoint values:")
                for val in df['Samplingpoint'].head().tolist():
                    print(f"     {val}")
                    
        except Exception as e:
            print(f"   Error reading file: {e}")

def compare_id_formats(metadata_file, parquet_dir):
    """Compare ID formats between metadata and parquet files."""
    print("\n" + "=" * 80)
    print("COMPARING ID FORMATS")
    print("=" * 80)
    
    # Load metadata
    metadata_df = pd.read_csv(metadata_file, low_memory=False)
    print(f"Metadata columns: {list(metadata_df.columns)}")
    
    # Show some Sampling Point Ids from metadata
    print(f"\nMetadata Sampling Point Ids (first 10):")
    for sp_id in metadata_df['Sampling Point Id'].head(10):
        print(f"  {sp_id}")
    
    # Check parquet files
    parquet_files = list(Path(parquet_dir).glob("*.parquet"))
    
    for pq_file in parquet_files[:3]:  # First 3 files
        print(f"\nParquet file: {pq_file.name}")
        try:
            df = pq.read_table(pq_file).to_pandas()
            if 'Samplingpoint' in df.columns:
                print(f"  Samplingpoint values (first 5):")
                for val in df['Samplingpoint'].head().tolist():
                    print(f"    {val}")
            if 'AirQualityStation' in df.columns:
                print(f"  AirQualityStation values (first 5):")
                for val in df['AirQualityStation'].head().tolist():
                    print(f"    {val}")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python explore_parquet_structure.py <parquet_directory>")
        print("Optional: also set metadata path in script")
        sys.exit(1)
    
    parquet_dir = sys.argv[1]
    explore_parquet_files(parquet_dir)
    
    # Uncomment to compare with metadata
    # metadata_file = "metadata/stations_metadata.csv"  # Change path if needed
    # if Path(metadata_file).exists():
    #     compare_id_formats(metadata_file, parquet_dir)
