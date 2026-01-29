import csv
import os
import re
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm
import aiohttp
from karpet import Karpet
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from scripts.utils.date_utils import get_canonical_data_date
from prefect.variables import Variable
from itertools import islice

# Load environment variables
load_dotenv()

# Configuration
LOCAL_DATA_DIR = Path("data")
seeds_path=Path("seeds")
CRYPTOLIST_PATH = seeds_path/"cryptolist.txt"

def sanitize_text(text: str) -> str:
    """
    Cleans text by converting to lowercase, removing newlines, HTML tags, 
    and replacing problematic characters for CSV/SQL loading (e.g., internal quotes, commas).
    """
    if not isinstance(text, str):
        return text  # Return non-string values as is (e.g., NaNs)

    text = text.lower()
    
    # Remove newline characters
    text = text.replace('\n', ' ').replace('\r', ' ')
    
    # Remove HTML tags (using regex)
    text = re.sub('<.*?>', '', text) 

    # Replace double quotes with single quotes to prevent CSV parsing issues
    text = text.replace('"', "'")
    
    return text

@task(name="Scrape Raw Data and Save to CSV")
def scrape_raw_data_to_csv(data_date: str | None = None) -> Path:
    """
    Scrapes crypto news and saves the results to a local RAW CSV file.
    Uses a canonical data_date suffix (YYYYMMDD_HHMMSS) for the filename so downstream steps can locate it.
    
    Returns:
        Path: The local Path of the saved RAW CSV file.
    """
    logger = get_run_logger()
    CRYPTO_READ_LIMIT = int(
    Variable.get("news_read_limit", default=10)
    )
    try:
        k = Karpet() 
    except Exception as e:
        logger.error(f"❌ Failed to initialize Karpet client: {e}")
        raise

    # Check if cryptolist file exists
    if not CRYPTOLIST_PATH.exists():
        logger.error(f"❌ Cryptocurrency list file not found at: {CRYPTOLIST_PATH}")
        raise FileNotFoundError(f"Missing required file: {CRYPTOLIST_PATH}")

    with open(CRYPTOLIST_PATH, "r") as file:
        cryptocurrencies = [line.strip() for line in islice(file, CRYPTO_READ_LIMIT)]
        #cryptocurrencies = [line.strip() for line in file.readlines()]

    run_suffix = get_canonical_data_date(data_date)
    raw_file_name = f"news_raw_{run_suffix}.csv"
    raw_file_path = LOCAL_DATA_DIR / raw_file_name
    
    all_news = []
    logger.info(f"Starting news retrieval for {len(cryptocurrencies)} currencies...")
    
    with tqdm(total=len(cryptocurrencies), desc="Data Retrieval", leave=False) as pbar:
        for cryptocurrency in cryptocurrencies:
            try:
                news = k.fetch_news(cryptocurrency)
                for article in news:
                    article["cryptocurrency"] = cryptocurrency
                all_news.extend(news)
            except (aiohttp.client_exceptions.ClientConnectorCertificateError, 
                    aiohttp.client_exceptions.ClientConnectorError,
                    aiohttp.client_exceptions.ClientPayloadError,
                    aiohttp.client_exceptions.ClientError,
                    ConnectionError,
                    Exception) as e:
                logger.warning(f"⚠️ Ignoring error for {cryptocurrency}: {type(e).__name__} - {e}")
                continue
            pbar.update(1)

    # Ensure the 'data' directory exists
    raw_file_path.parent.mkdir(exist_ok=True, parents=True)

    # Save RAW results to CSV file
    headers = ["cryptocurrency", "url", "title", "description", "date", "image"]
    with open(raw_file_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(all_news)

    logger.info(f"✅ RAW results saved locally to CSV file: {raw_file_path} with {len(all_news)} articles.")
    return raw_file_path

@flow(name="2_news_data_scrapper")
def crypto_news_scraper(data_date: str | None = None):
    """Prefect flow to scrape news and upload to S3.
    Accepts an optional canonical data_date (YYYYMMDD_HHMMSS) to align with batch runs.
    """
    logger = get_run_logger()
    raw_file_path = None
    try:
        logger.info("🚀 Starting crypto news scraper to S3...")
        
        # 1. Scrape news and save to local CSV
        raw_file_path = scrape_raw_data_to_csv(data_date=data_date)
        logger.info("✅ News scraped and saved locally CSV")
        
    except Exception as e:
        logger.error(f"❌ Flow failed during execution: {e}")
        raise
        
    finally:
        if raw_file_path and raw_file_path.exists():
            logger.info(f"📁 Local file kept at: {raw_file_path}")
        elif raw_file_path is None:
            logger.info("Clean-up skipped: raw_file_path was not successfully assigned.")

if __name__ == "__main__":
    crypto_news_scraper()
