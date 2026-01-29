"""
Kafka consumer for loading customer & transaction data into PostgreSQL.
Consumes 4 topics:
- customers
- corporates
- transactions_customers (personal transactions)
- transactions_corporates (corporate transactions)
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict
from contextlib import contextmanager

from kafka import KafkaConsumer
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# UTC+7 timezone (e.g., Asia/Bangkok, Indochina Time)
UTC_PLUS_7 = timezone(timedelta(hours=7))

# Try to import pandas for Timestamp handling
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

load_dotenv()
logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Suppress Kafka library verbose logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.coordinator").setLevel(logging.WARNING)
logging.getLogger("kafka.consumer").setLevel(logging.WARNING)
logging.getLogger("kafka.consumer.subscription_state").setLevel(logging.WARNING)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
# Hardcoded topic names matching producer
KAFKA_TOPIC_CORPORATES = "raw_corporates"
KAFKA_TOPIC_CUSTOMERS = "raw_customers"
KAFKA_TOPIC_TRANSACTION_PERSONAL = "raw_transaction_personal"
KAFKA_TOPIC_TRANSACTION_CORPORATE = "raw_transaction_corporate"
BATCH_SIZE = int(os.getenv('KAFKA_CONSUMER_BATCH_SIZE', '100'))
BATCH_TIMEOUT_SECONDS = int(os.getenv('KAFKA_CONSUMER_BATCH_TIMEOUT', '30'))

POSTGRES_HOST = os.getenv("TSDB_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Table names - using the names specified by user
TABLE_TRANSACTION_CUSTOMERS = "raw_transaction_personal"
TABLE_TRANSACTION_CORPORATES = "raw_transaction_corporate"
TABLE_CUSTOMERS = "raw_customers"
TABLE_CORPORATES = "raw_corporates"


@contextmanager
def get_postgres_connection():
    """Context manager for PostgreSQL connections."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        yield conn
    except Exception as e:
        logger.error(f"❌ PostgreSQL connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def create_postgres_tables():
    """Create PostgreSQL tables if they don't exist."""
    try:
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            
            # Create schema if needed
            if POSTGRES_SCHEMA != "public":
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                    .format(sql.Identifier(POSTGRES_SCHEMA))
                )

            # Transaction tables (split by customer type)
            tx_cust_table = sql.Identifier(POSTGRES_SCHEMA, TABLE_TRANSACTION_CUSTOMERS)
            tx_corp_table = sql.Identifier(POSTGRES_SCHEMA, TABLE_TRANSACTION_CORPORATES)
            
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    transaction_id VARCHAR(100),
                    customer_id VARCHAR(100),
                    asset_type VARCHAR(20),
                    asset_symbol VARCHAR(50),
                    transaction_type VARCHAR(10),
                    quantity NUMERIC(20,8),
                    price_per_unit NUMERIC(20,8),
                    transaction_amount NUMERIC(20,2),
                    fee_amount NUMERIC(20,2),
                    transaction_timestamp TIMESTAMP,
                    data_date DATE,
                    customer_tier VARCHAR(20),
                    customer_risk_tolerance VARCHAR(20),
                    customer_type VARCHAR(20),
                    data_source VARCHAR(50),
                    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(20) NOT NULL DEFAULT 'KAFKA_DATA',
                    PRIMARY KEY (transaction_id, load_timestamp)
                )
            """).format(tx_cust_table))
            
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    transaction_id VARCHAR(100),
                    customer_id VARCHAR(100),
                    asset_type VARCHAR(20),
                    asset_symbol VARCHAR(50),
                    transaction_type VARCHAR(10),
                    quantity NUMERIC(20,8),
                    price_per_unit NUMERIC(20,8),
                    transaction_amount NUMERIC(20,2),
                    fee_amount NUMERIC(20,2),
                    transaction_timestamp TIMESTAMP,
                    data_date DATE,
                    customer_tier VARCHAR(20),
                    customer_risk_tolerance VARCHAR(20),
                    customer_type VARCHAR(20),
                    data_source VARCHAR(50),
                    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(20) NOT NULL DEFAULT 'KAFKA_DATA',
                    PRIMARY KEY (transaction_id, load_timestamp)
                )
            """).format(tx_corp_table))
            
            # Customers table
            customers_table = sql.Identifier(POSTGRES_SCHEMA, TABLE_CUSTOMERS)
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    customer_id VARCHAR(100),
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(255),
                    gender VARCHAR(10),
                    age_group VARCHAR(20),
                    country VARCHAR(50),
                    registration_date DATE,
                    customer_tier VARCHAR(20),
                    risk_tolerance VARCHAR(20),
                    customer_type VARCHAR(20),
                    company_id VARCHAR(100),
                    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(20) NOT NULL DEFAULT 'KAFKA_DATA',
                    PRIMARY KEY (customer_id, load_timestamp)
                )
            """).format(customers_table))
            
            # Corporates table
            corporates_table = sql.Identifier(POSTGRES_SCHEMA, TABLE_CORPORATES)
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    company_id VARCHAR(100),
                    company_name VARCHAR(255),
                    company_type VARCHAR(50),
                    company_email VARCHAR(255),
                    country VARCHAR(50),
                    year_founded INT,
                    tax_number VARCHAR(100),
                    office_primary_location TEXT,
                    registration_date DATE,
                    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(20) NOT NULL DEFAULT 'KAFKA_DATA',
                    PRIMARY KEY (company_id, load_timestamp)
                )
            """).format(corporates_table))
            
            conn.commit()
            return True
            
    except Exception as e:
        logger.error(f"❌ Failed to create PostgreSQL tables: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def json_deserializer(msg_bytes: bytes) -> Dict:
    """Simple JSON deserializer for Kafka messages."""
    if msg_bytes is None:
        return {}
    return json.loads(msg_bytes.decode('utf-8'))


def convert_to_utc_plus_7(ts):
    """Convert a timestamp to UTC+7 timezone and return as naive datetime (for PostgreSQL TIMESTAMP)."""
    if ts is None:
        return datetime.now(UTC_PLUS_7).replace(tzinfo=None)
    
    # Handle pandas Timestamp
    if HAS_PANDAS and isinstance(ts, pd.Timestamp):
        if ts.tz is None:
            # Naive timestamp, assume it's already UTC+7 time
            return ts.to_pydatetime()
        elif str(ts.tz) == 'UTC' or 'UTC' in str(ts.tz):
            # Convert from UTC to UTC+7
            ts_utc7 = ts.tz_convert('Asia/Bangkok')
            return ts_utc7.to_pydatetime().replace(tzinfo=None)
        else:
            # Already in a timezone, convert to UTC+7
            ts_utc7 = ts.tz_convert('Asia/Bangkok')
            return ts_utc7.to_pydatetime().replace(tzinfo=None)
    
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except:
            return datetime.now(UTC_PLUS_7).replace(tzinfo=None)
    
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            # Assume UTC if no timezone info
            ts = ts.replace(tzinfo=timezone.utc)
        elif ts.tzinfo == timezone.utc:
            pass  # Already UTC
        # Convert to UTC+7, then remove timezone info (naive datetime with UTC+7 time)
        ts_utc7 = ts.astimezone(UTC_PLUS_7)
        return ts_utc7.replace(tzinfo=None)  # Return naive datetime with UTC+7 time
    
    return datetime.now(UTC_PLUS_7).replace(tzinfo=None)


def insert_transactions_batch(rows: List[Dict], table_name: str) -> bool:
    """Insert a batch of transactions into PostgreSQL."""
    if not rows:
        return True
    
    try:
        with get_postgres_connection() as conn:
            cur = conn.cursor()
            
            values = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                
                # Handle load_timestamp - convert to UTC+7
                load_ts = convert_to_utc_plus_7(r.get("load_timestamp"))
                
                # Handle transaction_timestamp - convert to UTC+7
                txn_ts = r.get("transaction_timestamp")
                if txn_ts is not None:
                    txn_ts = convert_to_utc_plus_7(txn_ts)
                
                values.append((
                    r.get("transaction_id"),
                    r.get("customer_id"),
                    r.get("asset_type"),
                    r.get("asset_symbol"),
                    r.get("transaction_type"),
                    r.get("quantity"),
                    r.get("price_per_unit"),
                    r.get("transaction_amount"),
                    r.get("fee_amount"),
                    txn_ts,  # Use converted timestamp
                    r.get("data_date"),
                    r.get("customer_tier"),
                    r.get("customer_risk_tolerance"),
                    r.get("customer_type"),
                    r.get("data_source"),
                    load_ts,  # Use converted timestamp
                    "KAFKA_DATA",
                ))
            
            if not values:
                return True
            
            table_id = sql.Identifier(POSTGRES_SCHEMA, table_name)
            insert_sql = sql.SQL("""
                INSERT INTO {} (
                    transaction_id, customer_id, asset_type, asset_symbol, transaction_type,
                    quantity, price_per_unit, transaction_amount, fee_amount,
                    transaction_timestamp, data_date, customer_tier,
                    customer_risk_tolerance, customer_type, data_source,
                    load_timestamp, source
                ) VALUES %s
                ON CONFLICT (transaction_id, load_timestamp) DO NOTHING
            """).format(table_id)
            
            execute_values(cur, insert_sql, values)
            conn.commit()
            
            # Log inserted IDs with table name prefix
            table_prefix = "transaction_personal" if table_name == TABLE_TRANSACTION_CUSTOMERS else "transaction_corporate"
            for r in rows:
                if isinstance(r, dict) and r.get('transaction_id'):
                    logger.info(f"{table_prefix} {r.get('transaction_id')} inserted")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Failed to insert transactions batch: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def insert_customers_batch(rows: List[Dict]) -> bool:
    """Insert a batch of customers into PostgreSQL."""
    if not rows:
        return True
    
    try:
        with get_postgres_connection() as conn:
            cur = conn.cursor()
            
            values = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                
                # Handle load_timestamp - convert to UTC+7
                load_ts = convert_to_utc_plus_7(r.get("load_timestamp"))
                
                values.append((
                    r.get("customer_id"),
                    r.get("first_name"),
                    r.get("last_name"),
                    r.get("email"),
                    r.get("gender"),
                    r.get("age_group"),
                    r.get("country"),
                    r.get("registration_date"),
                    r.get("customer_tier"),
                    r.get("risk_tolerance"),
                    r.get("customer_type"),
                    r.get("company_id"),
                    load_ts,
                    "KAFKA_DATA",
                ))
            
            if not values:
                return True
            
            table_id = sql.Identifier(POSTGRES_SCHEMA, TABLE_CUSTOMERS)
            insert_sql = sql.SQL("""
                INSERT INTO {} (
                    customer_id, first_name, last_name, email, gender, age_group, country,
                    registration_date, customer_tier, risk_tolerance, customer_type, company_id,
                    load_timestamp, source
                ) VALUES %s
                ON CONFLICT (customer_id, load_timestamp) DO NOTHING
            """).format(table_id)
            
            execute_values(cur, insert_sql, values)
            conn.commit()
            
            # Log inserted IDs
            for r in rows:
                if isinstance(r, dict) and r.get('customer_id'):
                    logger.info(f"customer {r.get('customer_id')} inserted")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Failed to insert customers batch: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def insert_corporates_batch(rows: List[Dict]) -> bool:
    """Insert a batch of corporates into PostgreSQL."""
    if not rows:
        return True
    
    try:
        with get_postgres_connection() as conn:
            cur = conn.cursor()
            
            values = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                
                # Handle load_timestamp - convert to UTC+7
                load_ts = convert_to_utc_plus_7(r.get("load_timestamp"))
                
                values.append((
                    r.get("company_id"),
                    r.get("company_name"),
                    r.get("company_type"),
                    r.get("company_email"),
                    r.get("country"),
                    r.get("year_founded"),
                    r.get("tax_number"),
                    r.get("office_primary_location"),
                    r.get("registration_date"),
                    load_ts,
                    "KAFKA_DATA",
                ))
            
            if not values:
                return True
            
            table_id = sql.Identifier(POSTGRES_SCHEMA, TABLE_CORPORATES)
            insert_sql = sql.SQL("""
                INSERT INTO {} (
                    company_id, company_name, company_type, company_email, country, year_founded,
                    tax_number, office_primary_location, registration_date, load_timestamp, source
                ) VALUES %s
                ON CONFLICT (company_id, load_timestamp) DO NOTHING
            """).format(table_id)
            
            execute_values(cur, insert_sql, values)
            conn.commit()
            
            # Log inserted IDs
            for r in rows:
                if isinstance(r, dict) and r.get('company_id'):
                    logger.info(f"corporate {r.get('company_id')} inserted")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Failed to insert corporates batch: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def consume_and_load():
    """Main consumer loop."""
    logger.info("Starting Kafka consumer...")
    logger.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Connecting to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    if not create_postgres_tables():
        logger.error("Failed to create PostgreSQL tables. Exiting.")
        return
    
    logger.info("PostgreSQL tables created/verified successfully")
    logger.info("Starting to consume messages from Kafka topics...")
    logger.info(f"Topics: {KAFKA_TOPIC_TRANSACTION_PERSONAL}, {KAFKA_TOPIC_TRANSACTION_CORPORATE}, {KAFKA_TOPIC_CUSTOMERS}, {KAFKA_TOPIC_CORPORATES}")

    # Create consumers with JSON deserializer - simple and clean
    consumers = {
        "txn_cust": KafkaConsumer(
            KAFKA_TOPIC_TRANSACTION_PERSONAL,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="txn-customer-consumer",
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=json_deserializer,
        ),
        "txn_corp": KafkaConsumer(
            KAFKA_TOPIC_TRANSACTION_CORPORATE,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="txn-corporate-consumer",
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=json_deserializer,
        ),
        "customers": KafkaConsumer(
            KAFKA_TOPIC_CUSTOMERS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="customers-consumer-latest",
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=json_deserializer,
        ),
        "corporates": KafkaConsumer(
            KAFKA_TOPIC_CORPORATES,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="corporates-consumer-latest",
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=json_deserializer,
        ),
    }

    txn_cust_batch, txn_corp_batch = [], []
    customer_batch, corporate_batch = [], []
    last_flush = time.time()

    try:
        while True:
            # Poll all topics
            txn_cust_records = consumers["txn_cust"].poll(500)
            if txn_cust_records:
                for records in txn_cust_records.values():
                    for r in records:
                        # r.value is already a dict thanks to value_deserializer
                        if r.value and isinstance(r.value, dict):
                            txn_cust_batch.append(r.value)

            txn_corp_records = consumers["txn_corp"].poll(500)
            if txn_corp_records:
                for records in txn_corp_records.values():
                    for r in records:
                        # r.value is already a dict thanks to value_deserializer
                        if r.value and isinstance(r.value, dict):
                            txn_corp_batch.append(r.value)

            customer_records = consumers["customers"].poll(500)
            if customer_records:
                for records in customer_records.values():
                    for r in records:
                        # r.value is already a dict thanks to value_deserializer
                        if r.value and isinstance(r.value, dict):
                            customer_batch.append(r.value)
                
                # Flush customer batch immediately after receiving messages
                if customer_batch:
                    insert_customers_batch(customer_batch)
                    customer_batch.clear()

            corporate_records = consumers["corporates"].poll(500)
            if corporate_records:
                for records in corporate_records.values():
                    for r in records:
                        # r.value is already a dict thanks to value_deserializer
                        if r.value and isinstance(r.value, dict):
                            corporate_batch.append(r.value)
                
                # Flush corporate batch immediately after receiving messages
                if corporate_batch:
                    insert_corporates_batch(corporate_batch)
                    corporate_batch.clear()

            # Flush condition for transactions (customer/corporate batches are flushed immediately above)
            now = time.time()
            if (
                len(txn_cust_batch) >= BATCH_SIZE
                or len(txn_corp_batch) >= BATCH_SIZE
                or (now - last_flush) >= BATCH_TIMEOUT_SECONDS
            ):
                if txn_cust_batch:
                    insert_transactions_batch(txn_cust_batch, TABLE_TRANSACTION_CUSTOMERS)
                    txn_cust_batch.clear()

                if txn_corp_batch:
                    insert_transactions_batch(txn_corp_batch, TABLE_TRANSACTION_CORPORATES)
                    txn_corp_batch.clear()

                last_flush = now

            time.sleep(0.1)

    except KeyboardInterrupt:
        pass
    finally:
        for c in consumers.values():
            c.close()


if __name__ == "__main__":
    consume_and_load()
