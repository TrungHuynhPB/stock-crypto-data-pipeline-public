# scripts/utils/db_connector.py

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Returns an active psycopg2 connection object."""
    DB_HOST = os.getenv("TSDB_HOST", "timescaledb")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASS = os.getenv("POSTGRES_PASSWORD")

    try:
        conn = psycopg2.connect(
            host=DB_HOST, 
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASS
        )
        return conn
    except psycopg2.Error as e:
        print(f"ERROR: Could not connect to TimescaleDB. {e}")
        raise
