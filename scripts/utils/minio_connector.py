"""
MinIO connector utility for market data ingestion flows.
This module provides a clean interface for MinIO operations.
"""

import os
from pathlib import Path
from typing import Optional
from prefect import get_run_logger
from prefect_aws.s3 import S3Bucket
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MINIO_BLOCK_NAME = os.getenv("MINIO_BUCKET","minio-stock-bucket")


def get_s3_bucket() -> Optional[S3Bucket]:
    """
    Get the S3Bucket block for MinIO.
    
    Returns:
        S3Bucket: MinIO S3Bucket block or None if failed
    """
    logger = get_run_logger()
    
    try:
        s3_bucket = S3Bucket.load(MINIO_BLOCK_NAME)
        logger.info(f"✅ Loaded S3Bucket block: {MINIO_BLOCK_NAME}")
        return s3_bucket
    except Exception as e:
        logger.error(f"❌ Failed to load S3Bucket block: {e}")
        return None


def upload_file_to_minio(local_file_path: Path, minio_key: str) -> bool:
    """
    Upload a local file to MinIO.
    
    Args:
        local_file_path (Path): Path to local file
        minio_key (str): MinIO object key (path in bucket)
        
    Returns:
        bool: True if upload successful, False otherwise
    """
    logger = get_run_logger()
    
    if not local_file_path.exists():
        logger.error(f"❌ Local file not found: {local_file_path}")
        return False
    
    s3_bucket = get_s3_bucket()
    if not s3_bucket:
        return False
    
    try:
        s3_bucket.upload_from_path(local_file_path, minio_key)
        logger.info(f"✅ Uploaded file to s3://{s3_bucket.bucket_name}/{minio_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to upload to MinIO: {e}")
        return False


def download_file_from_minio(minio_key: str, local_file_path: Path) -> bool:
    """
    Download a file from MinIO to local path.
    
    Args:
        minio_key (str): MinIO object key
        local_file_path (Path): Local destination path
        
    Returns:
        bool: True if download successful, False otherwise
    """
    logger = get_run_logger()
    
    s3_bucket = get_s3_bucket()
    if not s3_bucket:
        return False
    
    try:
        # Ensure parent directory exists
        local_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        s3_bucket.download_object_to_path(minio_key, local_file_path)
        logger.info(f"✅ Downloaded file from s3://{s3_bucket.bucket_name}/{minio_key} to {local_file_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to download from MinIO: {e}")
        return False


def list_minio_objects(prefix: str = "") -> list:
    """
    List objects in MinIO bucket with optional prefix.
    
    Args:
        prefix (str): Object key prefix to filter
        
    Returns:
        list: List of object keys
    """
    logger = get_run_logger()
    
    s3_bucket = get_s3_bucket()
    if not s3_bucket:
        return []
    
    try:
        objects = s3_bucket.list_objects(prefix=prefix)
        logger.info(f"✅ Listed {len(objects)} objects with prefix '{prefix}'")
        return objects
    except Exception as e:
        logger.error(f"❌ Failed to list MinIO objects: {e}")
        return []


def delete_minio_object(minio_key: str) -> bool:
    """
    Delete an object from MinIO.
    
    Args:
        minio_key (str): MinIO object key to delete
        
    Returns:
        bool: True if deletion successful, False otherwise
    """
    logger = get_run_logger()
    
    s3_bucket = get_s3_bucket()
    if not s3_bucket:
        return False
    
    try:
        s3_bucket.delete_object(minio_key)
        logger.info(f"✅ Deleted object s3://{s3_bucket.bucket_name}/{minio_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to delete MinIO object: {e}")
        return False


