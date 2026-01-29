"""
Prefect flow to scrape crypto news and upload to S3/MinIO.
This script gathers news data and stores it in the data lake (S3/MinIO).
"""

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

# Load environment variables
load_dotenv()

# Configuration
LOCAL_DATA_DIR = Path("data")
CRYPTOLIST_PATH = LOCAL_DATA_DIR / "cryptolist.txt"

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

def get_s3_client():
    """Create and return a boto3 S3 client for MinIO."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=False
    )


@task(name="Upload File to S3/MinIO")
def upload_to_s3(local_file_path: Path, s3_folder: str = "raw-data/crypto_news") -> bool:
    """
    Upload a local file to S3/MinIO.
    
    Args:
        local_file_path: Path to the local file to upload
        s3_folder: S3 folder path to upload to (default: raw-data/crypto_news)
        
    Returns:
        bool: True if upload successful, False otherwise
    """
    logger = get_run_logger()
    
    try:
        s3_client = get_s3_client()
        
        # S3 key uses the folder and file name
        s3_key = f"{s3_folder}/{local_file_path.name}"
        
        # Upload file
        s3_client.upload_file(
            str(local_file_path),
            MINIO_BUCKET,
            s3_key
        )
        
        logger.info(f"✅ Uploaded RAW file to s3://{MINIO_BUCKET}/{s3_key}")
        return True
        
    except ClientError as e:
        logger.error(f"❌ Failed to upload to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during S3 upload: {e}")
        return False


@task(name="Upload Fake Data CSV Files to S3/MinIO")
def upload_fake_data_to_s3(data_date: str) -> dict:
    """
    Upload all fake data CSV files to S3/MinIO.
    
    Args:
        data_date: Canonical date string used in filenames (YYYYMMDD_HHMMSS)
        
    Returns:
        dict: Dictionary with upload status for each file
    """
    logger = get_run_logger()
    
    # Define file names and their S3 folders
    files_config = {
        'personal_transactions': {
            'filename': f"fake_personal_customers_transactions_{data_date}.csv",
            's3_folder': 'raw-data/transactions/personal'
        },
        'corporate_transactions': {
            'filename': f"fake_corporate_customers_transactions_{data_date}.csv",
            's3_folder': 'raw-data/transactions/corporate'
        },
        'customers': {
            'filename': f"fake_customers_{data_date}.csv",
            's3_folder': 'raw-data/customers'
        },
        'corporates': {
            'filename': f"fake_corporates_{data_date}.csv",
            's3_folder': 'raw-data/corporates'
        },
        'news': {
            'filename': f"news_raw_{data_date}.csv",
            's3_folder': 'raw-data/crypto_news'
        }
    }
    
    results = {}
    
    for file_type, config in files_config.items():
        file_path = LOCAL_DATA_DIR / config['filename']
        
        if not file_path.exists():
            logger.warning(f"⚠️ File not found: {file_path}")
            results[file_type] = False
            continue
        
        try:
            s3_client = get_s3_client()
            s3_key = f"{config['s3_folder']}/{config['filename']}"
            
            s3_client.upload_file(
                str(file_path),
                MINIO_BUCKET,
                s3_key
            )
            
            logger.info(f"✅ Uploaded {file_type} to s3://{MINIO_BUCKET}/{s3_key}")
            results[file_type] = True
            
        except ClientError as e:
            logger.error(f"❌ Failed to upload {file_type} to S3: {e}")
            results[file_type] = False
        except Exception as e:
            logger.error(f"❌ Unexpected error uploading {file_type}: {e}")
            results[file_type] = False
    
    return results


@flow(name="3_data_to_s3_flow")
def data_to_s3_flow(data_date: str = None):
    """
    Prefect flow to upload fake data and news CSV files to S3/MinIO.
    
    Args:
        data_date: Optional canonical data_date (YYYYMMDD_HHMMSS). If not provided, a new one is generated.
    """
    logger = get_run_logger()
    
    try:
        logger.info("🚀 Starting data upload to S3/MinIO...")
        
        run_suffix = get_canonical_data_date(data_date)
        logger.info(f"📅 Using data date: {run_suffix}")
        
        # Upload all fake data files
        upload_results = upload_fake_data_to_s3(run_suffix)
        
        # Check results and log summary
        successful_uploads = sum(1 for success in upload_results.values() if success)
        total_files = len(upload_results)
        
        logger.info(f"📊 Upload Summary: {successful_uploads}/{total_files} files uploaded successfully")
        
        for file_type, success in upload_results.items():
            status = "✅" if success else "❌"
            logger.info(f"  {status} {file_type}: {'Success' if success else 'Failed'}")
        
        if successful_uploads == total_files:
            logger.info("✅ All files uploaded successfully to S3/MinIO.")
            return True
        elif successful_uploads > 0:
            logger.warning(f"⚠️ Partial success: {successful_uploads}/{total_files} files uploaded.")
            return True
        else:
            logger.error("❌ Pipeline failed: No files were uploaded.")
            return False
        
    except Exception as e:
        logger.error(f"❌ Flow failed during execution: {e}")
        raise

if __name__ == "__main__":
    data_to_s3_flow()
