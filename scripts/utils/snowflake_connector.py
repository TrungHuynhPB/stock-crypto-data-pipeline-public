"""
Snowflake connector utility for market data ingestion flows.
This module provides a clean interface for Snowflake operations using manual connections.
"""

import os
import snowflake.connector
from contextlib import contextmanager
from typing import Optional, List, Tuple, Any
from prefect import get_run_logger
from dotenv import load_dotenv
import pandas as pd
# Load environment variables
load_dotenv()


@contextmanager
def get_snowflake_connection():
    """
    Context manager for Snowflake connections.
    Automatically handles connection setup and cleanup.
    
    Yields:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    """
    conn = None
    logger = get_run_logger()
    
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="SNOWFLAKE_JWT",
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            #private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        logger.info("✅ Snowflake connection established")
        yield conn
        
    except Exception as e:
        logger.error(f"❌ Snowflake connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("🔌 Snowflake connection closed")


def create_table_if_not_exists(table_name: str, create_sql: str) -> bool:
    """
    Create a Snowflake table if it doesn't exist.
    
    Args:
        table_name (str): Full table name (database.schema.table)
        create_sql (str): CREATE TABLE SQL statement
        
    Returns:
        bool: True if table was created or already exists, False otherwise
    """
    logger = get_run_logger()
    
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_sql)
            logger.info(f"✅ Table {table_name} created/verified successfully")
            return True
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {e}")
        return False


def upload_file_to_stage(local_file_path: str, stage_name: str) -> bool:
    """
    Upload a local file to Snowflake stage using PUT command.
    
    Args:
        local_file_path (str): Path to local file to upload
        stage_name (str): Stage name (schema.stage)
        
    Returns:
        bool: True if upload successful, False otherwise
    """
    logger = get_run_logger()
    
    # Ensure local file path is absolute
    local_file_path = os.path.abspath(local_file_path)
    
    if not os.path.exists(local_file_path):
        logger.error(f"❌ Local file not found: {local_file_path}")
        return False
    
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            
            # PUT command
            put_command = f"""
            PUT file://{local_file_path} @{stage_name}
            AUTO_COMPRESS = TRUE
            """
            cursor.execute(put_command)

            # Get upload results (may return multiple rows)
            rows = cursor.fetchall()
            if not rows:
                logger.warning("⚠️ PUT returned no result rows.")
                return False

            def _norm_status(v):
                try:
                    if isinstance(v, (bytes, bytearray)):
                        v = v.decode()
                    return str(v or "").upper()
                except Exception:
                    return ""

            statuses = [(r[0], r[1], _norm_status(r[6] if len(r) > 6 else None)) for r in rows]
            non_ok = [s for s in statuses if s[2] not in ("UPLOADED", "SKIPPED")]
            if non_ok:
                logger.warning(f"⚠️ Upload failed or status unknown. Result rows: {statuses}")
                return False

            uploaded = [s for s in statuses if s[2] == "UPLOADED"]
            skipped = [s for s in statuses if s[2] == "SKIPPED"]
            if uploaded:
                logger.info(f"✅ Uploaded to stage: {[f'{u[0]} as {u[1]}' for u in uploaded]}")
            if skipped:
                logger.info(f"ℹ️ Skipped (already present): {[f'{s[0]} as {s[1]}' for s in skipped]}")
            return True
                
    except Exception as e:
        logger.error(f"❌ File upload failed: {e}")
        return False


def copy_data_from_stage(
    table_name: str, 
    stage_path: str, 
    columns_list: str,
    file_format: str = "CSV"
) -> bool:
    """
    Copy data from Snowflake stage to table using COPY INTO command.
    
    Args:
        table_name (str): Target table name (database.schema.table)
        stage_path (str): Stage path with file pattern
        columns_list (str): Column list for COPY INTO
        file_format (str): File format (default: CSV)
        
    Returns:
        bool: True if copy successful, False otherwise
    """
    logger = get_run_logger()
    
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            
            # COPY INTO command
            copy_sql = f"""
            COPY INTO {table_name} {columns_list}
            FROM {stage_path}
            FILE_FORMAT = (TYPE = {file_format}, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
            ON_ERROR = 'CONTINUE';
            """
            
            logger.info(f"Executing COPY INTO command for {table_name}")
            cursor.execute(copy_sql)
            results = cursor.fetchall()
            
            logger.info(f"COPY INTO results: {results}")
            
            # Check if copy was successful
            if results and any('LOADED' in str(row).upper() for row in results):
                logger.info(f"✅ Successfully loaded data into {table_name}")
                return True
            else:
                logger.warning(f"⚠️ No rows loaded or soft error occurred")
                return False
                
    except Exception as e:
        logger.error(f"❌ COPY INTO failed: {e}")
        return False


def execute_query(query: str) -> List[Tuple[Any, ...]]:
    """
    Execute a SQL query and return results.
    
    Args:
        query (str): SQL query to execute
        
    Returns:
        List[Tuple[Any, ...]]: Query results
    """
    logger = get_run_logger()
    
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            logger.info(f"✅ Query executed successfully, returned {len(results)} rows")
            return results
    except Exception as e:
        logger.error(f"❌ Query execution failed: {e}")
        raise


def check_table_exists(table_name: str) -> bool:
    """
    Check if a Snowflake table exists.
    
    Args:
        table_name (str): Full table name (database.schema.table)
        
    Returns:
        bool: True if table exists, False otherwise
    """
    logger = get_run_logger()
    
    try:
        # Parse table name
        parts = table_name.split('.')
        if len(parts) != 3:
            logger.error(f"Invalid table name format: {table_name}. Expected: database.schema.table")
            return False
        
        database, schema, table = parts
        
        # Check if table exists
        check_sql = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_CATALOG = '{database}'
        AND TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        """
        
        results = execute_query(check_sql)
        exists = results[0][0] > 0 if results else False
        
        status = '✅ Exists' if exists else '❌ Does not exist'
        logger.info(f"Table {table_name}: {status}")
        return exists
        
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        return False

def execute_non_query(query: str) -> bool:
    """
    Execute a non-query SQL command (like ALTER SESSION, CREATE TABLE)
    and return True for success, False for failure.
    """
    logger = get_run_logger()
    
    try:
        # We don't care about the results, just that it completes without error
        execute_query(query)
        logger.info(f"✅ Non-query executed successfully: {query[:50]}...")
        return True
    except Exception:
        # The underlying execute_query already logged the error
        return False
