#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_eea_e1a_e2a.py
Description: Downloads EEA air quality data (E1a/E2a datasets)
Author: Giovanni Bonafè | ARPA FVG
Created: 2025-11-04
Last update: 2025-11-17
Version: 1.3
"""

import os
import sys
import requests
import argparse
import logging
from pathlib import Path
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
ENDPOINT = "ParquetFile/urls"
DEFAULT_DOWNLOAD_DIR = "./eea_parquets"

def setup_download_dir(download_dir):
    download_path = Path(download_dir)
    download_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Download directory: {download_path.absolute()}")
    return download_path

def make_api_request(api_url, endpoint, request_body, max_retries=3):
    url = f"{api_url}{endpoint}"
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=request_body, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request attempt {attempt+1}/{max_retries} failed: {e}")
        if attempt < max_retries - 1:
            wait_time = 2**attempt
            time.sleep(wait_time)
    raise Exception(f"Failed API request after {max_retries} attempts")

def download_file(url, file_path, chunk_size=8192):
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
        if file_path.exists() and file_path.stat().st_size > 0:
            file_size_mb = file_path.stat().st_size / 1024 / 1024
            logger.info(f"✓ Downloaded: {file_path.name} ({file_size_mb:.1f} MB)")
            return True
        else:
            file_path.unlink(missing_ok=True)
            return False
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        file_path.unlink(missing_ok=True)
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Download EEA air quality data (E1a/E2a datasets)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Valid datasets: E1a (2), E2a (1)
"""
    )

    parser.add_argument("--email", type=str, required=True, help="Email for API")
    parser.add_argument("--countries", nargs="+", default=["IT"])
    parser.add_argument("--pollutants", nargs="+", default=["PM10"])
    parser.add_argument("--dataset", choices=["E1a", "E2a"], default="E2a")
    parser.add_argument("--cities", nargs="+", default=[])
    parser.add_argument("--download-dir", type=str, default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # CORRETTO: E1a=2, E2a=1
    dataset_codes = {"E1a": 2, "E2a": 1}
    download_dir = setup_download_dir(args.download_dir)
    
    request_body = {
        "countries": args.countries,
        "cities": args.cities,
        "pollutants": args.pollutants,
        "dataset": dataset_codes[args.dataset],
        "email": args.email,
    }

    logger.info("="*60)
    logger.info(f"Dataset: {args.dataset} (code: {dataset_codes[args.dataset]})")
    logger.info(f"Countries: {args.countries}")
    logger.info(f"Pollutants: {args.pollutants}")
    logger.info(f"Download directory: {download_dir}")
    logger.info("="*60)

    total_files = downloaded_files = skipped_files = failed_files = 0

    logger.info("Requesting file list...")
    try:
        response = make_api_request(API_URL, ENDPOINT, request_body)
        urls = [u.strip() for u in response.text.split("\n")[1:] if u.strip()]
        logger.info(f"Found {len(urls)} files")
        
        if not urls:
            logger.warning("No files found with current filters")
            
        for url in urls:
            total_files += 1
            filename = url.split("/")[-1]
            file_path = download_dir / filename

            if file_path.exists() and not args.force:
                skipped_files += 1
                continue

            if args.dry_run:
                logger.info(f"[DRY RUN] Would download: {filename}")
                downloaded_files += 1
            else:
                if download_file(url, file_path):
                    downloaded_files += 1
                else:
                    failed_files += 1

    except Exception as e:
        logger.error(f"Failed to fetch file list: {e}")

    logger.info("="*60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info(f"Total files available: {total_files}")
    logger.info(f"Files successfully downloaded: {downloaded_files}")
    logger.info(f"Files skipped (already exist): {skipped_files}")
    logger.info(f"Files failed: {failed_files}")
    logger.info(f"Final directory: {download_dir.absolute()}")
    
    if args.dry_run:
        logger.info("DRY RUN COMPLETED - No files were downloaded")

if __name__ == "__main__":
    main()
