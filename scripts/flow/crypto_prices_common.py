"""
Common tasks and utilities for crypto price ingestion flows by source.
- Loads cryptolist
- Builds canonical data_date and filenames
- Saves pandas DataFrame to CSV with consistent columns
- Uploads to MinIO and Snowflake stage
- Creates and loads PostgreSQL raw tables per source
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from prefect import task, get_run_logger

from scripts.utils.date_utils import get_canonical_data_date
from scripts.utils.minio_connector import upload_file_to_minio
from scripts.utils.snowflake_connector import upload_file_to_stage

# Load envs
load_dotenv()

# Paths
LOCAL_DATA_DIR = Path("data")
CRYPTOLIST_PATH = Path("seeds/cryptolist.txt")

# Snowflake stage config
SNOWFLAKE_SCHEMA_STAGING = os.getenv("SNOWFLAKE_SCHEMA_STAGING", os.getenv("SNOWFLAKE_SCHEMA", "SC_STOCK_DATA_STAGING"))
SNOWFLAKE_STAGE_STAGING = os.getenv("SNOWFLAKE_STAGE_STAGING", "MINIO_RAW_STAGE")

# Postgres configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", os.getenv("TSDB_HOST", "timescaledb"))
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", os.getenv("POSTGRES_DATABASE", "stock_data"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "T23")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "minioadmin")


@task(name="Load Cryptocurrency List")
def load_crypto_list() -> List[str]:
    logger = get_run_logger()
    if not CRYPTOLIST_PATH.exists():
        logger.error(f"Cryptocurrency list file not found at: {CRYPTOLIST_PATH}")
        raise FileNotFoundError(f"Missing required file: {CRYPTOLIST_PATH}")
    with open(CRYPTOLIST_PATH, "r") as f:
        cryptocurrencies = [line.strip().lower() for line in f.readlines() if line.strip()]
    logger.info(f"Loaded {len(cryptocurrencies)} cryptocurrencies from {CRYPTOLIST_PATH}")
    return cryptocurrencies


def build_output_filename(source: str, data_date: Optional[str] = None) -> Path:
    dd = get_canonical_data_date(data_date)
    fname = f"crypto_{source}_{dd}.csv"
    return LOCAL_DATA_DIR / fname


@task(name="Save Source Data to CSV")
def save_source_csv(df: pd.DataFrame, source: str, data_date: Optional[str] = None) -> Path:
    """Save DataFrame to data/crypto_{source}_{data_date}.csv.
    Required columns for all sources:
      symbol, base_currency, quote_currency, price, volume, source, observed_at
    """
    logger = get_run_logger()
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Ensure required columns exist
    required = [
        "symbol", "base_currency", "quote_currency", "price", "volume", "source", "observed_at"
    ]
    for c in required:
        if c not in df.columns:
            df[c] = None
    df = df[required]

    out_path = build_output_filename(source, data_date)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"Saved {len(df)} {source} rows to {out_path}")
    return out_path


@task(name="Upload CSV to MinIO and Snowflake Stage")
def upload_minio_and_stage(csv_path: Path, source: str) -> bool:
    """Upload file to MinIO at raw-data/crypto/{source}/ and to Snowflake stage.
    Returns True if both uploads succeed.
    """
    logger = get_run_logger()
    # 1) MinIO
    minio_key = f"raw-data/crypto/{source}/{csv_path.name}"
    if not upload_file_to_minio(csv_path, minio_key):
        logger.error("Failed to upload to MinIO")
        return False

    # 2) Snowflake stage (PUT)
    stage_name = f"{SNOWFLAKE_SCHEMA_STAGING}.{SNOWFLAKE_STAGE_STAGING}"
    if not upload_file_to_stage(str(csv_path), stage_name):
        logger.error("Failed to upload to Snowflake stage")
        return False

    logger.info("Uploaded CSV to MinIO and Snowflake stage successfully")
    return True


def _pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD
    )


@task(name="Ensure Postgres Table")
def ensure_postgres_table(source: str):
    """Create raw_cryptoprices_{source} table if not exists with a small common schema."""
    logger = get_run_logger()
    table = f"raw_cryptoprices_{source}"
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        symbol VARCHAR(50),
        base_currency VARCHAR(50),
        quote_currency VARCHAR(50),
        price NUMERIC(20,8),
        volume NUMERIC(28,8),
        source VARCHAR(50),
        observed_at TIMESTAMP,
        load_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn = None
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        logger.info(f"✅ Ensured PostgreSQL table exists: {table}")
    except Exception as e:
        logger.error(f"❌ Failed creating PostgreSQL table {table}: {e}")
        raise
    finally:
        if conn:
            conn.close()


@task(name="Load CSV into Postgres")
def load_csv_to_postgres(csv_path: Path, source: str):
    """COPY CSV rows into raw_cryptoprices_{source}. Assumes header row present."""
    logger = get_run_logger()
    table = f"raw_cryptoprices_{source}"
    conn = None
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            with open(csv_path, "r") as f:
                next(f)  # skip header
                cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", f)
        conn.commit()
        logger.info(f"✅ Loaded {csv_path.name} into {table}")
    except Exception as e:
        logger.error(f"❌ Failed loading CSV to {table}: {e}")
        raise
    finally:
        if conn:
            conn.close()
