import os
from pathlib import Path
from typing import Optional
from prefect_aws.s3 import S3Bucket
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MINIO_BLOCK_NAME = "minio-stock-bucket"


def get_s3_bucket() -> Optional[S3Bucket]:
    """
    Get the S3Bucket block for MinIO.
    
    Returns:
        S3Bucket: MinIO S3Bucket block or None if failed
    """
    
    try:
        s3_bucket = S3Bucket.load(MINIO_BLOCK_NAME)
        print(f"✅ Loaded S3Bucket block: {MINIO_BLOCK_NAME}")
        return s3_bucket
    except Exception as e:
        print(f"❌ Failed to load S3Bucket block: {e}")
        return None
get_s3_bucket()