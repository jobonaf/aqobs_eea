#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_eea_e1a_e2a.py
Description: Downloads EEA hourly or daily air quality data (E1a/E2a datasets)
Author: Giovanni Bonafè | ARPA FVG
Created: 2025-11-04
Last update: 2025-11-13
Version: 1.2
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
        total_size = int(response.headers.get("content-length", 0))
        downloaded_size = 0
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
        if file_path.exists() and file_path.stat().st_size > 0:
            logger.info(f"✓ Downloaded: {file_path.name}")
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
        description="Download EEA hourly or daily air quality data (E1a/E2a)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Valid datasets: E1a (1), E2a (2)
Aggregation types: hour, day, all
"""
    )

    parser.add_argument("--email", type=str, required=True, help="Email for API")
    parser.add_argument("--countries", nargs="+", default=["IT"])
    parser.add_argument("--pollutants", nargs="+", default=["PM10"])
    parser.add_argument("--dataset", choices=["E1a", "E2a"], default="E2a")
    parser.add_argument("--cities", nargs="+", default=[])
    parser.add_argument("--download-dir", type=str, default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument("--aggregation", choices=["hour", "day", "all"], default="hour",
                        help="Aggregation type: hour, day, or all (default: hour)")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    dataset_codes = {"E1a": 2, "E2a": 1} 
    download_dir = setup_download_dir(args.download_dir)
    request_body_base = {
        "countries": args.countries,
        "cities": args.cities,
        "pollutants": args.pollutants,
        "dataset": dataset_codes[args.dataset],
        "email": args.email,
    }

    logger.info("="*60)
    logger.info(f"Dataset: {args.dataset}, Countries: {args.countries}, Pollutants: {args.pollutants}")
    logger.info(f"Download directory: {download_dir}")
    logger.info(f"Aggregation type: {args.aggregation}")
    logger.info("="*60)

    total_files = downloaded_files = skipped_files = failed_files = 0
    aggregations = ["hour", "day"] if args.aggregation == "all" else [args.aggregation]

    seen_files = set()  # per evitare duplicati

    for agg in aggregations:
        request_body = request_body_base.copy()
        request_body["aggregationType"] = agg
        logger.info(f"Fetching list of files for aggregation: {agg}")
        try:
            response = make_api_request(API_URL, ENDPOINT, request_body)
            urls = [u.strip() for u in response.text.split("\n")[1:] if u.strip()]
            logger.info(f"Found {len(urls)} files for aggregation {agg}")
        except Exception as e:
            logger.error(f"Failed to fetch files for {agg}: {e}")
            continue

        for url in urls:
            filename = url.split("/")[-1]
            if filename in seen_files:
                continue
            seen_files.add(filename)
            total_files += 1
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
