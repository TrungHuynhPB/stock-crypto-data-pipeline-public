"""scripts.data_generation.a1_4_batch_s3_to_snowflake

Prefect flow to copy batch CSV files from S3/MinIO to a Snowflake stage and load them into
Snowflake raw tables.

Behavior:
- Downloads each expected batch CSV from MinIO/S3
- PUTs it into the Snowflake stage (SNOWFLAKE_SCHEMA_STAGING.SNOWFLAKE_STAGE_STAGING)
- Creates the corresponding raw table in Snowflake (SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA)
  if it doesn't exist
- COPY INTO a TEMP table (stage -> temp), then MERGE INTO the target raw table

Target raw tables created/loaded:
- RAW_TRANSACTIONS (both personal + corporate transaction files)
- RAW_CUSTOMERS
- RAW_CORPORATES
- RAW_NEWS
"""

import csv
import os
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError
import snowflake.connector
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

import scripts.utils.snowflake_connector as sf_utils
from scripts.utils.date_utils import get_canonical_data_date

# Load environment variables
load_dotenv()

# Configuration
LOCAL_DATA_DIR = Path("data")

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

# Snowflake config
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DB_T23")
# Raw schema used by dbt sources (models/sources.yml uses SNOWFLAKE_SCHEMA)
SNOWFLAKE_SCHEMA_RAW = os.getenv("SNOWFLAKE_SCHEMA", "SC_T23")
# Stage schema/stage name where files are PUT
SNOWFLAKE_SCHEMA_STAGING = os.getenv("SNOWFLAKE_SCHEMA_STAGING", SNOWFLAKE_SCHEMA_RAW)
SNOWFLAKE_STAGE_STAGING = os.getenv("SNOWFLAKE_STAGE_STAGING", "MINIO_RAW_STAGE")

# Raw table names
SF_TABLE_TRANSACTIONS = "RAW_TRANSACTIONS"
SF_TABLE_CUSTOMERS = "RAW_CUSTOMERS"
SF_TABLE_CORPORATES = "RAW_CORPORATES"
SF_TABLE_NEWS = "RAW_NEWS"

FILE_TYPE_TO_TARGET_TABLE = {
    "personal_transactions": SF_TABLE_TRANSACTIONS,
    "corporate_transactions": SF_TABLE_TRANSACTIONS,
    "customers": SF_TABLE_CUSTOMERS,
    "corporates": SF_TABLE_CORPORATES,
    "news": SF_TABLE_NEWS,
}

# CSV schemas (column order in files)
TX_COLUMNS = [
    "transaction_id",
    "customer_id",
    "asset_type",
    "asset_symbol",
    "transaction_type",
    "quantity",
    "price_per_unit",
    "transaction_amount",
    "fee_amount",
    "transaction_timestamp",
    "data_date",
    "customer_tier",
    "customer_risk_tolerance",
    "customer_type",
    "load_timestamp",
    "data_source",
]

CUSTOMER_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "gender",
    "age_group",
    "country",
    "registration_date",
    "customer_tier",
    "risk_tolerance",
    "customer_type",
    "company_id",
    "load_timestamp",
]

CORPORATE_COLUMNS = [
    "company_id",
    "company_name",
    "company_type",
    "company_email",
    "country",
    "year_founded",
    "tax_number",
    "office_primary_location",
    "registration_date",
    "load_timestamp",
]

NEWS_COLUMNS = [
    "cryptocurrency",
    "url",
    "title",
    "description",
    "date",
    "image",
]

FILE_TYPE_TO_COLUMNS = {
    "personal_transactions": TX_COLUMNS,
    "corporate_transactions": TX_COLUMNS,
    "customers": CUSTOMER_COLUMNS,
    "corporates": CORPORATE_COLUMNS,
    "news": NEWS_COLUMNS,
}

# Merge keys for de-dup/upsert
# For transactions we treat TRANSACTION_ID + LOAD_TIMESTAMP as unique (append-only by load time)
FILE_TYPE_TO_MERGE_KEYS = {
    "personal_transactions": ["TRANSACTION_ID", "LOAD_TIMESTAMP"],
    "corporate_transactions": ["TRANSACTION_ID", "LOAD_TIMESTAMP"],
    "customers": ["CUSTOMER_ID"],
    "corporates": ["COMPANY_ID"],
    "news": ["URL", "DATE", "CRYPTOCURRENCY"],
}



def _sf_full_table_name(table: str) -> str:
    return f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_RAW}.{table}"


def _sf_stage_name() -> str:
    return f"{SNOWFLAKE_SCHEMA_STAGING}.{SNOWFLAKE_STAGE_STAGING}"


def _escape_stage_pattern(file_name: str) -> str:
    """Build a Snowflake regex PATTERN that matches the staged file, with optional .gz suffix."""
    # Snowflake PATTERN uses regex; escape the filename and allow optional .gz from AUTO_COMPRESS
    escaped = re.escape(file_name)
    # re.escape uses backslashes; ensure they survive SQL string literal
    escaped = escaped.replace("\\", "\\\\")
    return f".*{escaped}(\\\\.gz)?$"


def _infer_run_ts_yyyymmddhhmmss(file_name: str) -> Optional[str]:
    """Infer canonical run timestamp from filenames like *_YYYYMMDD_HHMMSS.csv."""
    m = re.search(r"_(\d{8})_(\d{6})\.csv$", file_name)
    if not m:
        return None
    return f"{m.group(1)}{m.group(2)}"  # YYYYMMDDHHMMSS


def get_s3_client():
    """Create and return a boto3 S3 client for MinIO."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=False
    )


@task(name="Download File from S3/MinIO")
def download_from_s3(s3_key: str, local_file_path: Path) -> bool:
    """
    Download a file from S3/MinIO.
    
    Args:
        s3_key: S3 key (path) of the file to download
        local_file_path: Local path where the file will be saved
        
    Returns:
        bool: True if download successful, False otherwise
    """
    logger = get_run_logger()
    
    try:
        s3_client = get_s3_client()
        
        # Ensure directory exists
        local_file_path.parent.mkdir(exist_ok=True, parents=True)
        
        # Download file
        s3_client.download_file(
            MINIO_BUCKET,
            s3_key,
            str(local_file_path)
        )
        
        logger.info(f"✅ Downloaded file from s3://{MINIO_BUCKET}/{s3_key} to {local_file_path}")
        return True
        
    except ClientError as e:
        logger.error(f"❌ Failed to download from S3: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during S3 download: {e}")
        return False


@task(name="List S3 Files")
def list_s3_files(prefix: str = "raw-data/crypto_news/") -> list:
    """
    List files in S3 with the given prefix.
    
    Args:
        prefix: S3 prefix to filter files
        
    Returns:
        list: List of S3 keys
    """
    logger = get_run_logger()
    
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        return []
        
    except Exception as e:
        logger.error(f"❌ Error listing S3 files: {e}")
        return []




@task(name="Upload CSV to Snowflake Stage")
def upload_csv_to_snowflake_stage(csv_file_path: Path) -> bool:
    """Upload CSV file to Snowflake stage."""
    logger = get_run_logger()

    # Ensure local file path is absolute, which PUT requires
    local_file_path = csv_file_path.resolve()

    if not local_file_path.exists():
        logger.error(f"❌ Local file not found for upload: {local_file_path}")
        return False

    # Delegate to shared utility (handles UPLOADED/SKIPPED semantics)
    return sf_utils.upload_file_to_stage(str(local_file_path), _sf_stage_name())


def _build_create_table_sql(file_type: str) -> str:
    """DDL for raw tables. These are aligned to the CSVs produced by a1_1/a1_2."""
    table = FILE_TYPE_TO_TARGET_TABLE[file_type]
    full_table = _sf_full_table_name(table)

    if file_type in ("personal_transactions", "corporate_transactions"):
        return f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            TRANSACTION_ID VARCHAR,
            CUSTOMER_ID VARCHAR,
            ASSET_TYPE VARCHAR,
            ASSET_SYMBOL VARCHAR,
            TRANSACTION_TYPE VARCHAR,
            QUANTITY NUMBER(20,8),
            PRICE_PER_UNIT NUMBER(20,8),
            TRANSACTION_AMOUNT NUMBER(20,2),
            FEE_AMOUNT NUMBER(20,2),
            TRANSACTION_TIMESTAMP TIMESTAMP_NTZ,
            DATA_DATE TIMESTAMP_NTZ,
            CUSTOMER_TIER VARCHAR,
            CUSTOMER_RISK_TOLERANCE VARCHAR,
            CUSTOMER_TYPE VARCHAR,
            LOAD_TIMESTAMP TIMESTAMP_NTZ,
            DATA_SOURCE VARCHAR,
            -- ingestion timestamp in warehouse
            INGESTED_AT TIMESTAMP_TZ DEFAULT CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())
        );
        """

    if file_type == "customers":
        return f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            CUSTOMER_ID VARCHAR,
            FIRST_NAME VARCHAR,
            LAST_NAME VARCHAR,
            EMAIL VARCHAR,
            GENDER VARCHAR,
            AGE_GROUP VARCHAR,
            COUNTRY VARCHAR,
            REGISTRATION_DATE DATE,
            CUSTOMER_TIER VARCHAR,
            RISK_TOLERANCE VARCHAR,
            CUSTOMER_TYPE VARCHAR,
            COMPANY_ID VARCHAR,
            LOAD_TIMESTAMP TIMESTAMP_NTZ,
            INGESTED_AT TIMESTAMP_TZ DEFAULT CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())
        );
        """

    if file_type == "corporates":
        return f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            COMPANY_ID VARCHAR,
            COMPANY_NAME VARCHAR,
            COMPANY_TYPE VARCHAR,
            COMPANY_EMAIL VARCHAR,
            COUNTRY VARCHAR,
            YEAR_FOUNDED NUMBER(10,0),
            TAX_NUMBER VARCHAR,
            OFFICE_PRIMARY_LOCATION VARCHAR,
            REGISTRATION_DATE DATE,
            LOAD_TIMESTAMP TIMESTAMP_NTZ,
            INGESTED_AT TIMESTAMP_TZ DEFAULT CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())
        );
        """

    if file_type == "news":
        return f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            CRYPTOCURRENCY VARCHAR,
            URL VARCHAR,
            TITLE VARCHAR,
            DESCRIPTION VARCHAR,
            DATE TIMESTAMP_TZ,
            IMAGE VARCHAR,
            INGESTED_AT TIMESTAMP_TZ DEFAULT CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())
        );
        """

    raise ValueError(f"Unsupported file_type for DDL: {file_type}")


@task(name="Ensure Snowflake Raw Table")
def ensure_snowflake_raw_table(file_type: str) -> bool:
    logger = get_run_logger()
    table = FILE_TYPE_TO_TARGET_TABLE.get(file_type)
    if not table:
        logger.error(f"Unknown file_type (no target table mapping): {file_type}")
        return False

    full_table = _sf_full_table_name(table)
    create_sql = _build_create_table_sql(file_type)

    ok = sf_utils.create_table_if_not_exists(full_table, create_sql)
    if not ok:
        logger.error(f"❌ Failed to ensure Snowflake table existence: {full_table}")
    return ok


def _temp_table_name(file_type: str, file_name: str) -> str:
    """Return a TEMP table name (unqualified) safe for one run."""
    ts = _infer_run_ts_yyyymmddhhmmss(file_name) or datetime.utcnow().strftime("%Y%m%d%H%M%S")
    # keep it short-ish; temp tables are session-scoped anyway
    return f"TMP_{FILE_TYPE_TO_TARGET_TABLE[file_type]}_{ts}"


def _copy_into_temp_sql(file_type: str, temp_table_unqualified: str, file_name: str) -> str:
    """COPY INTO temp table using stage PATTERN (handles .gz)."""
    cols = FILE_TYPE_TO_COLUMNS[file_type]

    # Build SELECT list in positional order, with type casts.
    # Note: COPY doesn't allow casting directly in column list, so we use the FROM (SELECT ...) form.
    def sel(i: int) -> str:
        return f"${i}"

    if file_type in ("personal_transactions", "corporate_transactions"):
        select_exprs = [
            f"{sel(1)} AS TRANSACTION_ID",
            f"{sel(2)} AS CUSTOMER_ID",
            f"{sel(3)} AS ASSET_TYPE",
            f"{sel(4)} AS ASSET_SYMBOL",
            f"{sel(5)} AS TRANSACTION_TYPE",
            f"TRY_TO_NUMBER({sel(6)}) AS QUANTITY",
            f"TRY_TO_NUMBER({sel(7)}) AS PRICE_PER_UNIT",
            f"TRY_TO_NUMBER({sel(8)}) AS TRANSACTION_AMOUNT",
            f"TRY_TO_NUMBER({sel(9)}) AS FEE_AMOUNT",
            f"TRY_TO_TIMESTAMP_NTZ({sel(10)}) AS TRANSACTION_TIMESTAMP",
            f"TRY_TO_TIMESTAMP_NTZ({sel(11)}) AS DATA_DATE",
            f"{sel(12)} AS CUSTOMER_TIER",
            f"{sel(13)} AS CUSTOMER_RISK_TOLERANCE",
            f"{sel(14)} AS CUSTOMER_TYPE",
            f"TRY_TO_TIMESTAMP_NTZ({sel(15)}) AS LOAD_TIMESTAMP",
            f"{sel(16)} AS DATA_SOURCE",
            "CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP()) AS INGESTED_AT",
        ]
        target_cols = "(TRANSACTION_ID, CUSTOMER_ID, ASSET_TYPE, ASSET_SYMBOL, TRANSACTION_TYPE, QUANTITY, PRICE_PER_UNIT, TRANSACTION_AMOUNT, FEE_AMOUNT, TRANSACTION_TIMESTAMP, DATA_DATE, CUSTOMER_TIER, CUSTOMER_RISK_TOLERANCE, CUSTOMER_TYPE, LOAD_TIMESTAMP, DATA_SOURCE, INGESTED_AT)"

    elif file_type == "customers":
        select_exprs = [
            f"{sel(1)} AS CUSTOMER_ID",
            f"{sel(2)} AS FIRST_NAME",
            f"{sel(3)} AS LAST_NAME",
            f"{sel(4)} AS EMAIL",
            f"{sel(5)} AS GENDER",
            f"{sel(6)} AS AGE_GROUP",
            f"{sel(7)} AS COUNTRY",
            f"TRY_TO_DATE({sel(8)}) AS REGISTRATION_DATE",
            f"{sel(9)} AS CUSTOMER_TIER",
            f"{sel(10)} AS RISK_TOLERANCE",
            f"{sel(11)} AS CUSTOMER_TYPE",
            f"{sel(12)} AS COMPANY_ID",
            f"TRY_TO_TIMESTAMP_NTZ({sel(13)}) AS LOAD_TIMESTAMP",
            "CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP()) AS INGESTED_AT",
        ]
        target_cols = "(CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, GENDER, AGE_GROUP, COUNTRY, REGISTRATION_DATE, CUSTOMER_TIER, RISK_TOLERANCE, CUSTOMER_TYPE, COMPANY_ID, LOAD_TIMESTAMP, INGESTED_AT)"

    elif file_type == "corporates":
        select_exprs = [
            f"{sel(1)} AS COMPANY_ID",
            f"{sel(2)} AS COMPANY_NAME",
            f"{sel(3)} AS COMPANY_TYPE",
            f"{sel(4)} AS COMPANY_EMAIL",
            f"{sel(5)} AS COUNTRY",
            f"TRY_TO_NUMBER({sel(6)}) AS YEAR_FOUNDED",
            f"{sel(7)} AS TAX_NUMBER",
            f"{sel(8)} AS OFFICE_PRIMARY_LOCATION",
            f"TRY_TO_DATE({sel(9)}) AS REGISTRATION_DATE",
            f"TRY_TO_TIMESTAMP_NTZ({sel(10)}) AS LOAD_TIMESTAMP",
            "CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP()) AS INGESTED_AT",
        ]
        target_cols = "(COMPANY_ID, COMPANY_NAME, COMPANY_TYPE, COMPANY_EMAIL, COUNTRY, YEAR_FOUNDED, TAX_NUMBER, OFFICE_PRIMARY_LOCATION, REGISTRATION_DATE, LOAD_TIMESTAMP, INGESTED_AT)"

    elif file_type == "news":
        select_exprs = [
            f"{sel(1)} AS CRYPTOCURRENCY",
            f"{sel(2)} AS URL",
            f"{sel(3)} AS TITLE",
            f"{sel(4)} AS DESCRIPTION",
            f"TRY_TO_TIMESTAMP_TZ({sel(5)}) AS DATE",
            f"{sel(6)} AS IMAGE",
            "CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP()) AS INGESTED_AT",
        ]
        target_cols = "(CRYPTOCURRENCY, URL, TITLE, DESCRIPTION, DATE, IMAGE, INGESTED_AT)"

    else:
        raise ValueError(f"Unsupported file_type: {file_type}")

    stage = _sf_stage_name()
    pattern = _escape_stage_pattern(file_name)

    return f"""
    COPY INTO {temp_table_unqualified}
    {target_cols}
    FROM (
        SELECT
            {", ".join(select_exprs)}
        FROM @{stage}
        (PATTERN => '{pattern}')
    )
    FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
    ON_ERROR = 'CONTINUE';
    """


def _merge_sql(file_type: str, temp_table_unqualified: str) -> str:
    target_table = _sf_full_table_name(FILE_TYPE_TO_TARGET_TABLE[file_type])
    merge_keys = FILE_TYPE_TO_MERGE_KEYS[file_type]

    # Determine updatable/insertable columns (exclude INGESTED_AT from updates, but set on insert)
    if file_type in ("personal_transactions", "corporate_transactions"):
        all_cols = [
            "TRANSACTION_ID",
            "CUSTOMER_ID",
            "ASSET_TYPE",
            "ASSET_SYMBOL",
            "TRANSACTION_TYPE",
            "QUANTITY",
            "PRICE_PER_UNIT",
            "TRANSACTION_AMOUNT",
            "FEE_AMOUNT",
            "TRANSACTION_TIMESTAMP",
            "DATA_DATE",
            "CUSTOMER_TIER",
            "CUSTOMER_RISK_TOLERANCE",
            "CUSTOMER_TYPE",
            "LOAD_TIMESTAMP",
            "DATA_SOURCE",
            "INGESTED_AT",
        ]
    elif file_type == "customers":
        all_cols = [
            "CUSTOMER_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "EMAIL",
            "GENDER",
            "AGE_GROUP",
            "COUNTRY",
            "REGISTRATION_DATE",
            "CUSTOMER_TIER",
            "RISK_TOLERANCE",
            "CUSTOMER_TYPE",
            "COMPANY_ID",
            "LOAD_TIMESTAMP",
            "INGESTED_AT",
        ]
    elif file_type == "corporates":
        all_cols = [
            "COMPANY_ID",
            "COMPANY_NAME",
            "COMPANY_TYPE",
            "COMPANY_EMAIL",
            "COUNTRY",
            "YEAR_FOUNDED",
            "TAX_NUMBER",
            "OFFICE_PRIMARY_LOCATION",
            "REGISTRATION_DATE",
            "LOAD_TIMESTAMP",
            "INGESTED_AT",
        ]
    elif file_type == "news":
        all_cols = [
            "CRYPTOCURRENCY",
            "URL",
            "TITLE",
            "DESCRIPTION",
            "DATE",
            "IMAGE",
            "INGESTED_AT",
        ]
    else:
        raise ValueError(f"Unsupported file_type: {file_type}")

    on_expr = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    update_cols = [c for c in all_cols if c not in merge_keys and c != "INGESTED_AT"]
    update_set = ",\n                        ".join([f"{c} = source.{c}" for c in update_cols] + ["INGESTED_AT = source.INGESTED_AT"])

    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"source.{c}" for c in all_cols])

    return f"""
    MERGE INTO {target_table} AS target
    USING {temp_table_unqualified} AS source
    ON {on_expr}
    WHEN MATCHED THEN
        UPDATE SET
                        {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals});
    """


@task(name="Load Staged CSV into Snowflake Raw Table")
def load_staged_file_into_snowflake_raw(file_type: str, file_name: str) -> bool:
    """Create table if needed and MERGE staged file into it."""
    logger = get_run_logger()

    if file_type not in FILE_TYPE_TO_TARGET_TABLE:
        logger.error(f"Unknown file_type: {file_type}")
        return False

    if not ensure_snowflake_raw_table(file_type):
        return False

    temp_table = _temp_table_name(file_type, file_name)

    # Use a single connection/session so TEMP table exists for COPY+MERGE.
    try:
        with sf_utils.get_snowflake_connection() as conn:
            cur = conn.cursor()

            # Create TEMP table with same structure as target
            target_full = _sf_full_table_name(FILE_TYPE_TO_TARGET_TABLE[file_type])
            cur.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} LIKE {target_full};")

            # COPY file from stage into temp
            copy_sql = _copy_into_temp_sql(file_type, temp_table, file_name)
            logger.info(f"Executing COPY for {file_type} from stage file {file_name}")
            cur.execute(copy_sql)

            # MERGE from temp into target
            merge_sql = _merge_sql(file_type, temp_table)
            logger.info(f"Executing MERGE for {file_type} into {target_full}")
            cur.execute(merge_sql)

        logger.info(f"✅ Loaded {file_name} into {target_full} via MERGE")
        return True

    except Exception as e:
        logger.error(f"❌ Failed loading staged file into Snowflake for {file_type}: {e}")
        return False


@flow(name="4_batch_s3_to_snowflake")
def crypto_news_s3_to_snowflake_flow(s3_key: Optional[str] = None, data_date: Optional[str] = None):
    """
    Copy batch CSV files (transactions, customers, corporates, news) from S3/MinIO to Snowflake stage.

    Args:
        s3_key: Deprecated. Kept for compatibility and ignored.
        data_date: Optional canonical date string in format YYYYMMDD_HHMMSS. If not provided, uses most recent files.
    """
    logger = get_run_logger()
    local_paths = []
    try:
        logger.info("🚀 Starting batch S3 -> Snowflake staging pipeline...")

        files_config = {
            'personal_transactions': {
                's3_folder': 'raw-data/transactions/personal',
                'filename_tmpl': 'fake_personal_customers_transactions_{date}.csv'
            },
            'corporate_transactions': {
                's3_folder': 'raw-data/transactions/corporate',
                'filename_tmpl': 'fake_corporate_customers_transactions_{date}.csv'
            },
            'customers': {
                's3_folder': 'raw-data/customers',
                'filename_tmpl': 'fake_customers_{date}.csv'
            },
            'corporates': {
                's3_folder': 'raw-data/corporates',
                'filename_tmpl': 'fake_corporates_{date}.csv'
            },
            'news': {
                's3_folder': 'raw-data/crypto_news',
                'filename_tmpl': 'news_raw_{date}.csv'
            }
        }

        overall_success = True

        for file_type, cfg in files_config.items():
            prefix = f"{cfg['s3_folder']}/"

            if data_date:
                s3_key_resolved = f"{prefix}{cfg['filename_tmpl'].format(date=data_date)}"
                logger.info(f"📥 Using provided date for {file_type}: {s3_key_resolved}")
            else:
                s3_files = list_s3_files(prefix)
                if not s3_files:
                    logger.error(f"❌ No files found in S3 with prefix '{prefix}'")
                    overall_success = False
                    continue
                s3_key_resolved = sorted(s3_files)[-1]
                logger.info(f"📥 Selected latest for {file_type}: {s3_key_resolved}")

            local_file = LOCAL_DATA_DIR / Path(s3_key_resolved).name
            if not download_from_s3(s3_key_resolved, local_file):
                logger.error(f"❌ Failed to download {file_type} from S3: {s3_key_resolved}")
                overall_success = False
                continue

            if not upload_csv_to_snowflake_stage(local_file):
                logger.error(f"❌ Failed to stage {file_type} to Snowflake: {local_file}")
                overall_success = False
                continue

            # After staging, load into raw Snowflake tables (create-if-not-exists + MERGE)
            if not load_staged_file_into_snowflake_raw(file_type, local_file.name):
                logger.error(f"❌ Failed to load staged file into raw table for {file_type}: {local_file.name}")
                overall_success = False
                continue

            local_paths.append(local_file)
            logger.info(f"✅ {file_type} copied to Snowflake stage and merged into raw tables.")

        if overall_success:
            logger.info(f"✅ All {len(files_config)} batch files staged to Snowflake successfully.")
            return True
        else:
            logger.warning("⚠️ Completed with some failures. Check logs above.")
            return False

    except Exception as e:
        logger.error(f"❌ Flow failed during execution: {e}")
        raise
    finally:
        for p in local_paths:
            if p and p.exists():
                logger.info(f"📁 Local copy kept at: {p}")


if __name__ == "__main__":
    #import sys
    #data_date = sys.argv[1] if len(sys.argv) > 1 else None
    data_date = get_canonical_data_date()
    crypto_news_s3_to_snowflake_flow(data_date=data_date)
