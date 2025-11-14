#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_eea_vocabularies.py
Description: Downloads EEA vocabularies without parsing
Author: Giovanni Bonafè | ARPA-FVG
Created: 2025-11-04
"""

import requests
import json
from pathlib import Path
from datetime import datetime, timedelta
import time
import argparse
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# EIONET vocabulary URLs - JSON format
VOCABULARIES = {
    'pollutant': 'https://dd.eionet.europa.eu/vocabulary/aq/pollutant/json',
    'quality_flag': 'https://dd.eionet.europa.eu/vocabulary/aq/observationverification/json',
    'unit': 'https://dd.eionet.europa.eu/vocabulary/uom/concentration/json',
    'aggregation_process': 'https://dd.eionet.europa.eu/vocabulary/aq/aggregationprocess/json',
    'station_type': 'https://dd.eionet.europa.eu/vocabulary/aq/stationclassification/json',
    'measurement_method': 'https://dd.eionet.europa.eu/vocabulary/aq/measurementmethod/json',
    'sampling_method': 'https://dd.eionet.europa.eu/vocabulary/aq/samplingmethod/json',
}

CACHE_DIR = Path("eea_vocabularies")
CACHE_DURATION = timedelta(days=7)

def ensure_cache_dir():
    """Create cache directory if it doesn't exist"""
    CACHE_DIR.mkdir(exist_ok=True)
    logger.debug(f"Cache directory: {CACHE_DIR.absolute()}")

def is_cache_valid(cache_file: Path) -> bool:
    """Check if cache file is still valid"""
    if not cache_file.exists():
        return False
    
    file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
    cache_age = datetime.now() - file_time
    is_valid = cache_age < CACHE_DURATION
    
    if is_valid:
        logger.debug(f"Cache valid: {cache_file.name} ({cache_age.days}d old)")
    else:
        logger.debug(f"Cache expired: {cache_file.name} ({cache_age.days}d old)")
    
    return is_valid

def download_vocabulary(url: str, max_retries: int = 3) -> dict:
    """Download vocabulary as JSON with retry logic"""
    for attempt in range(max_retries):
        try:
            logger.debug(f"Download attempt {attempt + 1}/{max_retries}: {url}")
            
            response = requests.get(url, timeout=30)
            
            if response.status_code == 500:
                logger.warning(f"Server error 500 for {url}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            
            # Validate JSON content
            json_data = response.json()
            logger.debug(f"Download successful: {len(response.content)} bytes")
            return json_data
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}/{max_retries}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error on attempt {attempt + 1}/{max_retries}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            if 400 <= e.response.status_code < 500:
                break  # Don't retry client errors
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt
            logger.info(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
    
    raise Exception(f"Failed to download vocabulary after {max_retries} attempts")

def download_and_save_vocabulary(vocabulary_type: str, force_refresh: bool = False) -> bool:
    """Download vocabulary and save raw JSON file. Returns True if successful."""
    ensure_cache_dir()
    
    cache_file = CACHE_DIR / f"{vocabulary_type}.json"
    
    # Return if cache is valid and not forced to refresh
    if not force_refresh and is_cache_valid(cache_file):
        file_size = cache_file.stat().st_size
        logger.info(f"Using cached {vocabulary_type} ({file_size / 1024:.1f} KB)")
        return True
    
    # Download vocabulary
    logger.info(f"Downloading {vocabulary_type} vocabulary...")
    url = VOCABULARIES.get(vocabulary_type)
    if not url:
        logger.error(f"Unknown vocabulary type: {vocabulary_type}")
        return False
    
    try:
        json_data = download_vocabulary(url)
        
        # Save raw JSON data
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        file_size = cache_file.stat().st_size
        logger.info(f"Downloaded and saved {vocabulary_type} ({file_size / 1024:.1f} KB)")
        return True
        
    except Exception as e:
        logger.error(f"Download failed for {vocabulary_type}: {e}")
        # Clean up partial file if it exists
        if cache_file.exists():
            cache_file.unlink()
        return False

def test_vocabulary_access():
    """Test which vocabularies are accessible"""
    logger.info("Testing vocabulary accessibility...")
    logger.info("=" * 50)
    
    results = {}
    
    for vocab_type, url in VOCABULARIES.items():
        logger.info(f"Testing {vocab_type}...")
        logger.debug(f"URL: {url}")
        
        try:
            response = requests.head(url, timeout=10)
            status = response.status_code
            if response.status_code == 200:
                logger.info(f"✓ HTTP {status} - ACCESSIBLE")
                results[vocab_type] = True
            else:
                logger.warning(f"✗ HTTP {status} - PROBLEM")
                results[vocab_type] = False
        except Exception as e:
            logger.error(f"✗ ERROR - {e}")
            results[vocab_type] = False
    
    logger.info("=" * 50)
    logger.info("ACCESSIBILITY SUMMARY:")
    accessible = [k for k, v in results.items() if v]
    problematic = [k for k, v in results.items() if not v]
    
    if accessible:
        logger.info(f"✓ Accessible vocabularies ({len(accessible)}): {', '.join(accessible)}")
    if problematic:
        logger.error(f"✗ Problematic vocabularies ({len(problematic)}): {', '.join(problematic)}")
    
    return results

def list_all_vocabularies():
    """List all available vocabulary types"""
    vocab_list = list(VOCABULARIES.keys())
    logger.info("Available vocabularies:")
    for vocab in vocab_list:
        logger.info(f"  - {vocab}")
    return vocab_list

def main():
    """Main function to download all vocabularies"""
    parser = argparse.ArgumentParser(
        description="Download EEA vocabularies (raw JSON format)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --vocabulary pollutant --force
  %(prog)s --vocabulary unit quality_flag --verbose
  %(prog)s --test
  %(prog)s --list

Cache duration: 7 days
        """
    )
    
    parser.add_argument("--vocabulary", nargs="+", 
                       choices=list(VOCABULARIES.keys()) + ['all'],
                       default=['all'],
                       help="Vocabulary to download (default: all)")
    
    parser.add_argument("--force", action="store_true", 
                       help="Force refresh of cached vocabularies")
    
    parser.add_argument("--list", action="store_true",
                       help="List available vocabulary types")
    
    parser.add_argument("--test", action="store_true",
                       help="Test accessibility of all vocabularies")
    
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose debug output")
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.list:
        list_all_vocabularies()
        return
    
    if args.test:
        test_vocabulary_access()
        return
    
    # Determine which vocabularies to process
    if 'all' in args.vocabulary:
        vocabularies_to_process = list(VOCABULARIES.keys())
    else:
        vocabularies_to_process = args.vocabulary
    
    logger.info("=" * 50)
    logger.info("EEA VOCABULARY DOWNLOAD")
    logger.info("=" * 50)
    logger.info(f"Vocabularies to process: {', '.join(vocabularies_to_process)}")
    logger.info(f"Force refresh: {args.force}")
    logger.info(f"Cache directory: {CACHE_DIR.absolute()}")
    logger.info("=" * 50)
    
    successful = []
    failed = []
    
    for vocab_type in vocabularies_to_process:
        logger.info(f"Processing {vocab_type}...")
        success = download_and_save_vocabulary(vocab_type, force_refresh=args.force)
        
        if success:
            successful.append(vocab_type)
        else:
            failed.append(vocab_type)
    
    # Summary
    logger.info("=" * 50)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 50)
    
    if successful:
        logger.info(f"✓ Successful downloads ({len(successful)}): {', '.join(successful)}")
    
    if failed:
        logger.error(f"✗ Failed downloads ({len(failed)}): {', '.join(failed)}")
        sys.exit(1)
    else:
        logger.info("✓ All vocabularies downloaded successfully")
        
        # Show cache info
        cache_files = list(CACHE_DIR.glob("*.json"))
        if cache_files:
            total_size = sum(f.stat().st_size for f in cache_files)
            logger.info(f"Total cache size: {total_size / 1024:.1f} KB across {len(cache_files)} files")

if __name__ == "__main__":
    main()
