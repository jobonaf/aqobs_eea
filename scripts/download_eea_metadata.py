#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_station_metadata.py
Description: Download and extract EEA air quality station metadata (DataExtract.csv)
Author: Giovanni Bonafè | ARPA FVG
Created: 2025-11-04
Last update: 2025-11-04
Version: 2.0
Dependencies: requests, zipfile, io, os, argparse, logging
Notes: Downloads ZIP from EEA DISCOMAP and extracts CSV as stations_metadata.csv
"""

import os
import io
import zipfile
import requests
import argparse
import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_URL = "https://discomap.eea.europa.eu/App/AQViewer/download?fqn=Airquality_Dissem.b2g.measurements&f=csv"
DEFAULT_OUTPUT_DIR = "metadata"
DEFAULT_FILENAME = "stations_metadata.csv"

def setup_output_dir(output_dir):
    """Create output directory if it doesn't exist."""
    output_path = Path(output_dir)
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_path.absolute()}")
        return output_path
    except Exception as e:
        logger.error(f"Could not create output directory {output_dir}: {e}")
        raise

def download_file(url, timeout=30, max_retries=3):
    """Download file with retry logic and progress tracking."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Download attempt {attempt + 1}/{max_retries}")
            logger.debug(f"Downloading from: {url}")
            
            response = requests.get(url, stream=True, timeout=timeout)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('content-type', '')
            if 'zip' not in content_type and 'application/octet-stream' not in content_type:
                logger.warning(f"Unexpected content type: {content_type}")
            
            # Get file size for progress tracking
            total_size = int(response.headers.get('content-length', 0))
            logger.info(f"Download size: {total_size / 1024 / 1024:.2f} MB" if total_size else "Download size: unknown")
            
            return response.content
            
        except requests.exceptions.Timeout:
            logger.warning(f"Download timeout (attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            if e.response.status_code == 404:
                raise FileNotFoundError(f"Metadata file not found at URL: {url}")
            if 400 <= e.response.status_code < 500:
                break  # Don't retry client errors
        except Exception as e:
            logger.error(f"Unexpected error during download: {e}")
        
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # Exponential backoff
            logger.info(f"Waiting {wait_time}s before retry...")
            import time
            time.sleep(wait_time)
    
    raise Exception(f"Failed to download after {max_retries} attempts")

def extract_csv_from_zip(zip_content, output_path, expected_filename=None):
    """Extract CSV file from ZIP content with flexible filename matching."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            # List all files in ZIP
            members = z.namelist()
            logger.debug(f"Files in ZIP: {members}")
            
            # Find CSV files
            csv_files = [m for m in members if m.lower().endswith('.csv')]
            
            if not csv_files:
                raise FileNotFoundError("No CSV files found in the downloaded ZIP archive")
            
            # Try to find expected filename first, otherwise use first CSV
            target_file = None
            if expected_filename:
                target_file = next((m for m in csv_files if expected_filename.lower() in m.lower()), None)
            
            if not target_file:
                target_file = csv_files[0]
                logger.info(f"Using first CSV file found: {target_file}")
            
            # Extract the file
            logger.info(f"Extracting: {target_file}")
            with z.open(target_file) as source:
                content = source.read()
            
            # Save to file
            with open(output_path, 'wb') as dest:
                dest.write(content)
            
            # Verify extraction
            if output_path.exists() and output_path.stat().st_size > 0:
                file_size_mb = output_path.stat().st_size / 1024 / 1024
                logger.info(f"✓ Extracted {target_file} → {output_path.name} ({file_size_mb:.2f} MB)")
                return output_path
            else:
                raise Exception("Extracted file is empty or missing")
                
    except zipfile.BadZipFile:
        raise ValueError("Downloaded file is not a valid ZIP archive")
    except Exception as e:
        raise Exception(f"Error extracting CSV from ZIP: {e}")

def validate_csv_file(csv_path, min_size=1000):
    """Basic validation of the extracted CSV file."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Extracted CSV file not found: {csv_path}")
    
    file_size = csv_path.stat().st_size
    if file_size < min_size:
        raise ValueError(f"CSV file seems too small: {file_size} bytes (expected at least {min_size})")
    
    logger.info(f"CSV validation passed: {file_size} bytes")
    return True

def download_station_metadata(
    url=DEFAULT_URL,
    output_dir=DEFAULT_OUTPUT_DIR,
    output_filename=DEFAULT_FILENAME,
    expected_zip_filename=None,
    verbose=False,
    force=False
):
    """
    Download and extract EEA air quality stations metadata.
    
    Args:
        url: Download URL for the metadata ZIP
        output_dir: Output directory for the CSV file
        output_filename: Name for the output CSV file
        expected_zip_filename: Expected filename in ZIP (optional)
        verbose: Enable verbose logging
        force: Force re-download even if file exists
    
    Returns:
        Path to the extracted CSV file
    """
    
    # Set logging level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Setup output directory
    output_path = setup_output_dir(output_dir)
    csv_output_path = output_path / output_filename
    
    # Check if file already exists
    if csv_output_path.exists() and not force:
        file_size_mb = csv_output_path.stat().st_size / 1024 / 1024
        logger.info(f"Metadata file already exists: {csv_output_path} ({file_size_mb:.2f} MB)")
        logger.info("Use --force to re-download")
        return csv_output_path
    
    logger.info("=" * 60)
    logger.info("EEA Station Metadata Download")
    logger.info("=" * 60)
    logger.info(f"Source URL: {url}")
    logger.info(f"Output file: {csv_output_path}")
    logger.info(f"Force download: {force}")
    logger.info("=" * 60)
    
    try:
        # Download the ZIP file
        logger.info("Starting download...")
        zip_content = download_file(url)
        logger.info("✓ Download completed successfully")
        
        # Extract CSV from ZIP
        logger.info("Extracting CSV from ZIP...")
        csv_path = extract_csv_from_zip(zip_content, csv_output_path, expected_zip_filename)
        
        # Validate the extracted CSV
        validate_csv_file(csv_path)
        
        # Summary
        logger.info("=" * 60)
        logger.info("METADATA DOWNLOAD COMPLETED SUCCESSFULLY")
        logger.info(f"Output: {csv_output_path.absolute()}")
        logger.info(f"Size: {csv_output_path.stat().st_size / 1024 / 1024:.2f} MB")
        logger.info("=" * 60)
        
        return csv_output_path
        
    except Exception as e:
        # Clean up on failure
        if csv_output_path.exists():
            csv_output_path.unlink()
            logger.debug("Cleaned up partial output file")
        logger.error(f"Metadata download failed: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description="Download and extract EEA air quality station metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  %(prog)s
  %(prog)s --output-dir /data/metadata --verbose
  %(prog)s --url "https://custom.url/for/metadata" --force
  %(prog)s --output-filename custom_metadata.csv --expected-file DataExtract.csv

Default URL: {DEFAULT_URL}
        """
    )
    
    parser.add_argument("--url", type=str, default=DEFAULT_URL,
                       help=f"Download URL for metadata ZIP (default: %(default)s)")
    
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR,
                       help=f"Output directory (default: %(default)s)")
    
    parser.add_argument("--output-filename", type=str, default=DEFAULT_FILENAME,
                       help=f"Output CSV filename (default: %(default)s)")
    
    parser.add_argument("--expected-file", type=str, default=None,
                       help="Expected filename in ZIP (e.g., DataExtract.csv)")
    
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose debug output")
    
    parser.add_argument("--force", action="store_true",
                       help="Force re-download even if file exists")
    
    args = parser.parse_args()
    
    try:
        csv_path = download_station_metadata(
            url=args.url,
            output_dir=args.output_dir,
            output_filename=args.output_filename,
            expected_zip_filename=args.expected_file,
            verbose=args.verbose,
            force=args.force
        )
        
        logger.info(f"Metadata ready: {csv_path}")
        
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
