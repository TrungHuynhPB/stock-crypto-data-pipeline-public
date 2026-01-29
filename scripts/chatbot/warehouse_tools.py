"""scripts.chatbot.warehouse_tools

Warehouse query tools for the AI chatbot.

Human-in-the-loop (HITL) design
-------------------------------
These tools *prepare* SQL and return it for approval instead of executing immediately.

Why?
- Prevent accidental / expensive queries
- Make the generated SQL transparent to the user
- Allow explicit approval before hitting Snowflake

Execution is done via the helper functions:
- :func:`execute_pending_query`
- :func:`cancel_pending_query`

The CLI (scripts/chatbot/chatbot.py) is responsible for prompting the user.
"""

import os
import json
import uuid
import time
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
from langchain_core.tools import tool
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from contextlib import contextmanager

load_dotenv()

# Get Snowflake schema from environment
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DB_T23")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "SC_T23")
MARTS_SCHEMA = "MART"  # Assuming marts are in a separate schema
SEMANTIC_SCHEMA = "SEM" 

@contextmanager
def get_chatbot_snowflake_connection():
    """
    Context manager for Snowflake connections in chatbot (standalone, no Prefect).
    This is a standalone version that doesn't require Prefect context.
    
    Yields:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    """
    conn = None
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="SNOWFLAKE_JWT",
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        yield conn
    except Exception as e:
        raise Exception(f"Snowflake connection failed: {str(e)}")
    finally:
        if conn:
            conn.close()


@dataclass(frozen=True)
class PendingQuery:
    """A query awaiting human approval."""

    query_id: str
    tool_name: str
    sql: str
    created_at: float


_PENDING_QUERIES: Dict[str, PendingQuery] = {}


def _register_pending_query(*, tool_name: str, sql: str) -> PendingQuery:
    query_id = str(uuid.uuid4())
    pq = PendingQuery(query_id=query_id, tool_name=tool_name, sql=sql.strip(), created_at=time.time())
    _PENDING_QUERIES[query_id] = pq
    return pq


def get_pending_query(query_id: str) -> Optional[PendingQuery]:
    """Get a pending query without executing it."""

    return _PENDING_QUERIES.get(query_id)


def cancel_pending_query(query_id: str) -> bool:
    """Cancel (delete) a pending query."""

    return _PENDING_QUERIES.pop(query_id, None) is not None


def execute_pending_query(query_id: str) -> str:
    """Execute a previously prepared query.

    Returns:
        JSON string of rows (records) or a friendly message.
    """

    pq = _PENDING_QUERIES.pop(query_id, None)
    if pq is None:
        return "No pending query found (it may have been executed or cancelled already)."

    try:
        with get_chatbot_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(pq.sql)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if not results:
                return "Query executed successfully but returned no rows."

            df = pd.DataFrame(results, columns=columns)
            return df.to_json(orient="records", date_format="iso")
    except Exception as e:
        return f"Error executing query: {str(e)}"


def _pending_response(pq: PendingQuery) -> str:
    """Standard JSON payload returned by tools when approval is required."""

    return json.dumps(
        {
            "status": "PENDING_APPROVAL",
            "query": asdict(pq),
        }
    )


@tool
def query_transactions(
    customer_id: Optional[str] = None,
    customer_name: Optional[str] = None,
    asset_symbol: Optional[str] = None,
    transaction_type: Optional[str] = None,
    limit: int = 10
) -> str:
    """
    Query transaction data from the batch-loaded fact_transactions table.
    
    Args:
        customer_id: Filter by specific customer ID (optional)
        customer_name: Filter by customer name (first name, last name, or full name) - use this when you have a name from an ID image (optional)
        asset_symbol: Filter by asset symbol like 'BTC', 'AAPL' (optional)
        transaction_type: Filter by transaction type like 'BUY', 'SELL' (optional)
        limit: Maximum number of results to return (default: 10)
    
    Returns:
        JSON string with transaction data including customer_id, customer name, asset_symbol, 
        transaction_type, transaction_amount, fee_amount, and transaction_timestamp
    """
    try:
        # Build WHERE clause
        conditions = []
        if customer_id:
            conditions.append(f"c.customer_id = '{customer_id}'")
        if customer_name:
            # Search by name - handle both full name and partial name matches
            name_parts = customer_name.strip().split()
            if len(name_parts) >= 2:
                # Full name: "John Doe" -> search for first_name='John' AND last_name='Doe'
                first_name = name_parts[0]
                last_name = " ".join(name_parts[1:])
                conditions.append(f"TRIM(c.first_name) ILIKE '%{first_name}%' AND TRIM(c.last_name) ILIKE '%{last_name}%'")
            else:
                # Single name: search in both first_name and last_name
                name = name_parts[0]
                conditions.append(f"TRIM(c.first_name) ILIKE ('%{name}%') OR TRIP(c.last_name) ILIKE '%{name}%'")
        if asset_symbol:
            conditions.append(f"h.asset_symbol = '{asset_symbol}'")
        if transaction_type:
            conditions.append(f"t.transaction_type = '{transaction_type.upper()}'")

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            h.asset_symbol,
            h.asset_type,
            t.transaction_type,
            t.transaction_amount,
            t.fee_amount,
            t.transaction_timestamp,
            t.data_date,
            c.customer_tier,
            c.country
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.fct_transactions t
        JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.dim_customer c
            ON t.customer_hk = c.customer_hk
        JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.dim_asset h
            ON t.asset_hk = h.asset_hk
        {where_clause}
        ORDER BY t.transaction_timestamp DESC
        LIMIT {limit}
        """

        pq = _register_pending_query(tool_name="query_transactions", sql=sql)
        return _pending_response(pq)
    except Exception as e:
        return f"Error preparing transactions query: {str(e)}"


@tool
def query_asset_prices(
    asset_symbol: Optional[str] = None,
    asset_type: Optional[str] = None,
    days: int = 30,
    limit: int = 10
) -> str:
    """
    Query real-time asset price data from fct_asset_prices table.
    This includes both crypto and stock prices from real-time sources (yfinance, CoinGecko, Binance).
    
    Args:
        asset_symbol: Filter by asset symbol like 'BTC', 'ETH', 'AAPL', 'TSLA' (optional)
        asset_type: Filter by asset type 'crypto' or 'stock' (optional)
        days: Number of recent days to retrieve (default: 30)
        limit: Maximum number of results to return (default: 10)
    
    Returns:
        JSON string with price data including asset_symbol, asset_type, observed_at, 
        price, volume, price_source, and asset_class
    """
    try:
        conditions = [f"observed_at >= DATEADD(day, -{days}, CURRENT_DATE())"]

        if asset_symbol:
            conditions.append(f"asset_symbol = '{asset_symbol}'")
        if asset_type:
            conditions.append(f"asset_type = '{asset_type}'")

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
        SELECT
            asset_symbol,
            asset_type,
            observed_at,
            price,
            volume,
            price_source,
            asset_class,
            price_date
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.fct_asset_prices
        {where_clause}
        ORDER BY observed_at DESC
        LIMIT {limit}
        """

        pq = _register_pending_query(tool_name="query_asset_prices", sql=sql)
        return _pending_response(pq)
    except Exception as e:
        return f"Error preparing asset prices query: {str(e)}"


@tool
def query_transaction_summary(
    group_by: str = "asset_symbol",
    limit: int = 20
) -> str:
    """
    Get aggregated transaction summary statistics.
    
    Args:
        group_by: Group by 'asset_symbol', 'customer_tier', 'country', or 'transaction_type' (default: 'asset_symbol')
        limit: Maximum number of groups to return (default: 20)
    
    Returns:
        JSON string with aggregated transaction statistics
    """
    try:
        valid_groups = ["asset_symbol", "customer_tier", "country", "transaction_type"]
        if group_by not in valid_groups:
            return f"Invalid group_by. Must be one of: {', '.join(valid_groups)}"

        sql = f"""
        SELECT
            {group_by},
            COUNT(*) as transaction_count,
            SUM(t.transaction_amount) as total_amount,
            AVG(t.transaction_amount) as avg_amount,
            SUM(t.fee_amount) as total_fees,
            COUNT(DISTINCT t.customer_hk) as unique_customers
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.fct_transactions t
        JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.dim_customer c
            ON t.customer_hk = c.customer_hk
        JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.dim_asset h
            ON t.asset_hk = h.asset_hk
        GROUP BY {group_by}
        ORDER BY total_amount DESC
        LIMIT {limit}
        """

        pq = _register_pending_query(tool_name="query_transaction_summary", sql=sql)
        return _pending_response(pq)
    except Exception as e:
        return f"Error preparing transaction summary query: {str(e)}"


@tool
def query_price_trends(
    asset_symbol: str,
    days: int = 30
) -> str:
    """
    Get price trends for a specific asset over time.
    
    Args:
        asset_symbol: Asset symbol like 'BTC', 'ETH', 'AAPL', 'TSLA'
        days: Number of days to analyze (default: 30)
    
    Returns:
        JSON string with price trends including date, price, volume, and price changes
    """
    try:
        sql = f"""
        SELECT
            asset_symbol,
            price_date,
            observed_at,
            price,
            volume,
            price_source,
            LAG(price) OVER (ORDER BY observed_at) as previous_price,
            price - LAG(price) OVER (ORDER BY observed_at) as price_change,
            ((price - LAG(price) OVER (ORDER BY observed_at)) / LAG(price) OVER (ORDER BY observed_at)) * 100 as price_change_pct
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.fct_asset_prices
        WHERE asset_symbol = '{asset_symbol}'
            AND observed_at >= DATEADD(day, -{days}, CURRENT_DATE())
        ORDER BY observed_at ASC
        """

        pq = _register_pending_query(tool_name="query_price_trends", sql=sql)
        return _pending_response(pq)
    except Exception as e:
        return f"Error preparing price trends query: {str(e)}"


@tool
def query_news_events(
    asset_symbol: Optional[str] = None,
    limit: int = 20
) -> str:
    """
    Query news events related to assets.
    
    Args:
        asset_symbol: Filter by asset symbol (optional)
        limit: Maximum number of results (default: 20)
    
    Returns:
        JSON string with news events including title, description, published_at, and asset_symbol
    """
    try:
        where_clause = ""
        if asset_symbol:
            where_clause = f"WHERE h.asset_symbol = '{asset_symbol}'"

        sql = f"""
        SELECT
            h.asset_symbol,
            n.title,
            n.description,
            n.published_at,
            n.ingestion_source
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.fct_news_events n
        JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.dim_asset h
            ON n.asset_hk = h.asset_hk
        {where_clause}
        ORDER BY n.published_at DESC
        LIMIT {limit}
        """

        pq = _register_pending_query(tool_name="query_news_events", sql=sql)
        return _pending_response(pq)
    except Exception as e:
        return f"Error preparing news events query: {str(e)}"


@tool
def query_customer_by_name(customer_name: str, limit: int = 10) -> str:
    """
    Search for customers by name (first name, last name, or full name or just Name).
    Use this when you have extracted a customer name from an ID image and want to find their customer information.
    
    Args:
        customer_name: Customer name to search for (can be first name, last name, or full name like "John Doe")
        limit: Maximum number of results to return (default: 10)
    
    Returns:
        JSON string with customer information including customer_id, first_name, last_name, email, country, customer_tier
    """
    try:
        # Handle name search - split if full name provided
        name_parts = customer_name.strip().split()
        if len(name_parts) >= 2:
            # Full name: "John Doe" -> search for first_name='John' AND last_name='Doe'
            first_name = name_parts[0]
            last_name = " ".join(name_parts[1:])
            where_clause = f"WHERE TRIM(c.first_name) ILIKE '%{first_name}%' AND TRIM(c.last_name) LIKE '%{last_name}%'"
        else:
            # Single name: search in both first_name and last_name
            name = name_parts[0]
            where_clause = f"WHERE TRIM(c.first_name) LIKE '%{name}%' OR TRIM(c.last_name) LIKE '%{name}%'"

        sql = f"""
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email_addr,
            c.country,
            c.customer_tier,
            c.risk_tolerance,
            c.company_name
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}_{MARTS_SCHEMA}.dim_customer c
        {where_clause}
        ORDER BY c.customer_id
        LIMIT {limit}
        """

        pq = _register_pending_query(tool_name="query_customer_by_name", sql=sql)
        return _pending_response(pq)
    except Exception as e:
        return f"Error preparing customer search query: {str(e)}"


# List of all warehouse tools
WAREHOUSE_TOOLS = [
    query_transactions,
    query_asset_prices,
    query_transaction_summary,
    query_price_trends,
    query_news_events,
    query_customer_by_name,
]
