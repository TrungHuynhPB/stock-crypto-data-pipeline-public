"""
Common tasks and utilities for stock price ingestion flows by source.

This module mirrors the crypto ingestion helpers in `scripts/data_generation/a2_0_crypto_common.py`
but targets the raw stock table defined in `models/sources.yml`:
  raw.raw_stock_prices

Responsibilities:
- Load stock tickers from seeds/stocklist.txt
- Build canonical data_date and filenames
- Save pandas DataFrame to CSV with consistent columns
- Upload CSV to MinIO and Snowflake stage
- Ensure and load PostgreSQL raw tables per source
- Ensure and load Snowflake raw tables per source

Output CSV schema (lowercase in file; Snowflake COPY maps to uppercase):
  ticker, date, open_price, high_price, low_price, close_price, adj_close_price,
  volume, dividends, stock_splits, company_name, sector, industry, market_cap,
  pe_ratio, week_52_high, week_52_low, avg_volume, source, observed_at
timestamp
Notes
- `load_timestamp` is added by Postgres default and Snowflake COPY (CURRENT_TIMESTAMP).
- Column `observed_at` represents event timestamp from source (we use current time).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from prefect import get_run_logger, task

from scripts.utils.date_utils import get_canonical_data_date
from scripts.utils.minio_connector import upload_file_to_minio
from scripts.utils.snowflake_connector import upload_file_to_stage
import scripts.utils.snowflake_connector as sf_utils

# Load envs
load_dotenv()

# Paths
LOCAL_DATA_DIR = Path("data")
STOCKLIST_PATH = Path("seeds/stocklist.txt")

# Snowflake stage/table config
SNOWFLAKE_SCHEMA_STAGING = os.getenv(
    "SNOWFLAKE_SCHEMA_STAGING", os.getenv("SNOWFLAKE_SCHEMA", "SC_T23")
)
SNOWFLAKE_STAGE_STAGING = os.getenv("SNOWFLAKE_STAGE_STAGING", "MINIO_RAW_STAGE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DB_T23")

# Postgres configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", os.getenv("TSDB_HOST", "timescaledb"))
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", os.getenv("POSTGRES_DATABASE", "stock_data"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "T23")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


@task(name="Load Stock Ticker List")
def load_stock_list() -> List[str]:
    """Load stock tickers from `seeds/stocklist.txt`.

    Returns uppercase tickers.
    """
    logger = get_run_logger()
    if not STOCKLIST_PATH.exists():
        logger.error(f"Stock list file not found at: {STOCKLIST_PATH}")
        raise FileNotFoundError(f"Missing required file: {STOCKLIST_PATH}")

    with open(STOCKLIST_PATH, "r", encoding="utf-8") as f:
        tickers = [line.strip().upper() for line in f.readlines() if line.strip()]

    # Deduplicate while preserving order
    seen = set()
    tickers_deduped: List[str] = []
    for t in tickers:
        if t not in seen:
            tickers_deduped.append(t)
            seen.add(t)

    logger.info(f"Loaded {len(tickers_deduped)} tickers from {STOCKLIST_PATH}")
    return tickers_deduped


def build_output_filename(source: str, data_date: Optional[str] = None) -> Path:
    dd = get_canonical_data_date(data_date)
    fname = f"stock_{source}_{dd}.csv"
    return LOCAL_DATA_DIR / fname


STOCK_REQUIRED_COLUMNS: list[str] = [
    "ticker",
    "date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adj_close_price",
    "volume",
    "dividends",
    "stock_splits",
    "company_name",
    "sector",
    "industry",
    "market_cap",
    "pe_ratio",
    "week_52_high",
    "week_52_low",
    "avg_volume",
    "source",
    "observed_at",
]


@task(name="Save Stock Source Data to CSV")
def save_source_csv(df: pd.DataFrame, source: str, data_date: Optional[str] = None) -> Path:
    """Save DataFrame to data/stock_{source}_{data_date}.csv.

    Ensures a consistent column set and order.
    """
    logger = get_run_logger()
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for c in STOCK_REQUIRED_COLUMNS:
        if c not in df.columns:
            df[c] = None

    df = df[STOCK_REQUIRED_COLUMNS]

    out_path = build_output_filename(source, data_date)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"Saved {len(df)} {source} rows to {out_path}")
    return out_path


@task(name="Upload Stock CSV to MinIO and Snowflake Stage")
def upload_minio_and_stage(csv_path: Path, source: str) -> bool:
    """Upload file to MinIO at raw-data/stock/{source}/ and to Snowflake stage."""
    logger = get_run_logger()

    # 1) MinIO
    minio_key = f"raw-data/stock/{source}/{csv_path.name}"
    if not upload_file_to_minio(csv_path, minio_key):
        logger.error("Failed to upload to MinIO")
        return False

    # 2) Snowflake stage (PUT)
    stage_name = f"{SNOWFLAKE_SCHEMA_STAGING}.{SNOWFLAKE_STAGE_STAGING}"
    if not upload_file_to_stage(str(csv_path), stage_name):
        logger.error("Failed to upload to Snowflake stage")
        return False

    logger.info("Uploaded stock CSV to MinIO and Snowflake stage successfully")
    return True


def _pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


@task(name="Ensure Postgres Stock Table")
def ensure_postgres_table(source: str):
    """Create raw_stock_prices_{source} table if it doesn't exist."""
    logger = get_run_logger()
    table = f"raw_stock_prices_{source}"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        ticker VARCHAR(20) NOT NULL,
        date DATE NOT NULL,
        open_price NUMERIC(20,8),
        high_price NUMERIC(20,8),
        low_price NUMERIC(20,8),
        close_price NUMERIC(20,8),
        adj_close_price NUMERIC(20,8),
        volume NUMERIC(28,8),
        dividends NUMERIC(20,8),
        stock_splits NUMERIC(20,8),
        company_name TEXT,
        sector TEXT,
        industry TEXT,
        market_cap NUMERIC(28,8),
        pe_ratio NUMERIC(20,8),
        week_52_high NUMERIC(20,8),
        week_52_low NUMERIC(20,8),
        avg_volume NUMERIC(28,8),
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


@task(name="Load Stock CSV into Postgres")
def load_csv_to_postgres(csv_path: Path, source: str):
    """COPY CSV rows into raw_stock_prices_{source}. Assumes header row present."""
    logger = get_run_logger()
    table = f"raw_stock_prices_{source}"

    columns = "(" + ", ".join(STOCK_REQUIRED_COLUMNS) + ")"

    conn = None
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            with open(csv_path, "r", encoding="utf-8") as f:
                next(f)  # skip header
                copy_sql = f"COPY {table} {columns} FROM STDIN WITH CSV"
                cur.copy_expert(copy_sql, f)
        conn.commit()
        logger.info(f"✅ Loaded {csv_path.name} into {table}")
    except Exception as e:
        logger.error(f"❌ Failed loading CSV to {table}: {e}")
        raise
    finally:
        if conn:
            conn.close()


@task(name="Ensure Snowflake Stock Table")
def ensure_snowflake_table(source: str) -> bool:
    logger = get_run_logger()

    table_name_raw = f"RAW_STOCK_PRICES_{source.upper()}"
    full_table_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_STAGING}.{table_name_raw}"

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        TICKER VARCHAR(20) NOT NULL,
        DATE DATE NOT NULL,
        OPEN_PRICE NUMBER(20,8),
        HIGH_PRICE NUMBER(20,8),
        LOW_PRICE NUMBER(20,8),
        CLOSE_PRICE NUMBER(20,8),
        ADJ_CLOSE_PRICE NUMBER(20,8),
        VOLUME NUMBER(28,8),
        DIVIDENDS NUMBER(20,8),
        STOCK_SPLITS NUMBER(20,8),
        COMPANY_NAME VARCHAR,
        SECTOR VARCHAR,
        INDUSTRY VARCHAR,
        MARKET_CAP NUMBER(28,8),
        PE_RATIO NUMBER(20,8),
        WEEK_52_HIGH NUMBER(20,8),
        WEEK_52_LOW NUMBER(20,8),
        AVG_VOLUME NUMBER(28,8),
        SOURCE VARCHAR(50),
        OBSERVED_AT TIMESTAMP_TZ,
        LOAD_TIMESTAMP TIMESTAMP_TZ DEFAULT CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())
    );
    """

    success = sf_utils.create_table_if_not_exists(full_table_name, create_sql)
    if not success:
        logger.error(f"❌ Failed to ensure Snowflake table existence: {full_table_name}")
    return success


@task(name="Load Stock CSV into Snowflake")
def load_csv_into_snowflake(csv_path: Path, source: str) -> bool:
    logger = get_run_logger()

    table_name_raw = f"raw_stock_prices_{source.upper()}"
    full_table_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_STAGING}.{table_name_raw}"
    stage_name = f"{SNOWFLAKE_SCHEMA_STAGING}.{SNOWFLAKE_STAGE_STAGING}"

    file_name = csv_path.name

    final_copy_sql = f"""
    COPY INTO {full_table_name}
    (
        TICKER, DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, ADJ_CLOSE_PRICE,
        VOLUME, DIVIDENDS, STOCK_SPLITS, COMPANY_NAME, SECTOR, INDUSTRY, MARKET_CAP,
        PE_RATIO, WEEK_52_HIGH, WEEK_52_LOW, AVG_VOLUME, SOURCE, OBSERVED_AT, LOAD_TIMESTAMP
    )
    FROM (
        SELECT
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12, $13, $14,
            $15, $16, $17, $18, $19, $20,
            CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())
        FROM @{stage_name}/{file_name}
    )
    FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
    ON_ERROR = 'CONTINUE';
    """

    success = sf_utils.execute_non_query(final_copy_sql)
    if success:
        logger.info(f"✅ Successfully copied {file_name} from stage into {full_table_name}")
    else:
        logger.error(f"❌ Failed loading CSV into Snowflake table {full_table_name}")

    return success
