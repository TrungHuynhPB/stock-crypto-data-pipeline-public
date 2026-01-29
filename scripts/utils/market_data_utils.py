"""
Utility functions for market data ingestion flows.
This module provides helper functions for testing, validation, and data processing.
"""

import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import yfinance as yf

from prefect import get_run_logger


def test_api_connectivity() -> Dict[str, bool]:
    """
    Test connectivity to all required APIs.
    
    Returns:
        Dict[str, bool]: Dictionary with API names and their connectivity status
    """
    logger = get_run_logger()
    results = {}
    
    # Test Binance API
    try:
        response = requests.get("https://api.binance.com/api/v3/ping", timeout=10)
        results['binance'] = response.status_code == 200
        logger.info(f"Binance API: {'✅ Connected' if results['binance'] else '❌ Failed'}")
    except Exception as e:
        results['binance'] = False
        logger.error(f"Binance API: ❌ Failed - {e}")
    
    # Test CoinGecko API
    try:
        response = requests.get("https://api.coingecko.com/api/v3/ping", timeout=10)
        results['coingecko'] = response.status_code == 200
        logger.info(f"CoinGecko API: {'✅ Connected' if results['coingecko'] else '❌ Failed'}")
    except Exception as e:
        results['coingecko'] = False
        logger.error(f"CoinGecko API: ❌ Failed - {e}")
    
    # Test yfinance (by fetching a simple ticker)
    try:
        ticker = yf.Ticker("AAPL")
        info = ticker.info
        results['yfinance'] = bool(info)
        logger.info(f"yfinance API: {'✅ Connected' if results['yfinance'] else '❌ Failed'}")
    except Exception as e:
        results['yfinance'] = False
        logger.error(f"yfinance API: ❌ Failed - {e}")
    
    return results


def validate_crypto_list(cryptolist_path: str = "data/cryptolist.txt") -> List[str]:
    """
    Validate and return the list of cryptocurrencies from the cryptolist file.
    
    Args:
        cryptolist_path (str): Path to the cryptocurrency list file
        
    Returns:
        List[str]: List of valid cryptocurrency symbols
    """
    logger = get_run_logger()
    
    if not os.path.exists(cryptolist_path):
        logger.error(f"Cryptocurrency list file not found: {cryptolist_path}")
        return []
    
    try:
        with open(cryptolist_path, 'r') as file:
            cryptocurrencies = [line.strip().lower() for line in file.readlines() if line.strip()]
        
        logger.info(f"Loaded {len(cryptocurrencies)} cryptocurrencies from {cryptolist_path}")
        return cryptocurrencies
    except Exception as e:
        logger.error(f"Error reading cryptocurrency list: {e}")
        return []


def get_sample_crypto_data(symbols: List[str], limit: int = 5) -> pd.DataFrame:
    """
    Fetch sample cryptocurrency data for testing purposes.
    
    Args:
        symbols (List[str]): List of cryptocurrency symbols
        limit (int): Maximum number of symbols to test
        
    Returns:
        pd.DataFrame: Sample data from APIs
    """
    logger = get_run_logger()
    
    sample_symbols = symbols[:limit]
    sample_data = []
    
    # Test Binance API
    try:
        response = requests.get("https://api.binance.com/api/v3/ticker/24hr", timeout=30)
        if response.status_code == 200:
            binance_data = response.json()
            for ticker in binance_data:
                symbol = ticker['symbol']
                if symbol.endswith('USDT'):
                    base_currency = symbol[:-4].lower()
                    if base_currency in sample_symbols:
                        sample_data.append({
                            'symbol': symbol,
                            'base_currency': base_currency,
                            'price': float(ticker['lastPrice']),
                            'source': 'binance',
                            'timestamp': datetime.now().isoformat()
                        })
                        if len(sample_data) >= limit:
                            break
    except Exception as e:
        logger.error(f"Error fetching sample Binance data: {e}")
    
    # Test CoinGecko API
    try:
        coin_ids = ','.join(sample_symbols)
        params = {
            'ids': coin_ids,
            'vs_currencies': 'usd',
            'include_market_cap': 'true'
        }
        response = requests.get("https://api.coingecko.com/api/v3/simple/price", params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            for coin_id, coin_data in data.items():
                if 'usd' in coin_data:
                    sample_data.append({
                        'symbol': coin_id.upper(),
                        'base_currency': coin_id.lower(),
                        'price': coin_data['usd'],
                        'market_cap': coin_data.get('usd_market_cap'),
                        'source': 'coingecko',
                        'timestamp': datetime.now().isoformat()
                    })
    except Exception as e:
        logger.error(f"Error fetching sample CoinGecko data: {e}")
    
    df = pd.DataFrame(sample_data)
    logger.info(f"Fetched sample data for {len(df)} cryptocurrency records")
    return df


def get_sample_stock_data(tickers: List[str], limit: int = 5) -> pd.DataFrame:
    """
    Fetch sample stock data for testing purposes.
    
    Args:
        tickers (List[str]): List of stock tickers
        limit (int): Maximum number of tickers to test
        
    Returns:
        pd.DataFrame: Sample stock data
    """
    logger = get_run_logger()
    
    sample_tickers = tickers[:limit]
    sample_data = []
    
    for ticker in sample_tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            hist = stock.history(period="1d")
            
            if not hist.empty:
                latest_price = hist['Close'].iloc[-1]
                sample_data.append({
                    'ticker': ticker,
                    'current_price': float(latest_price),
                    'company_name': info.get('longName', ticker),
                    'sector': info.get('sector', 'Unknown'),
                    'market_cap': info.get('marketCap'),
                    'source': 'yfinance',
                    'timestamp': datetime.now().isoformat()
                })
        except Exception as e:
            logger.error(f"Error fetching sample data for {ticker}: {e}")
    
    df = pd.DataFrame(sample_data)
    logger.info(f"Fetched sample data for {len(df)} stock records")
    return df


def validate_environment_variables() -> Dict[str, bool]:
    """
    Validate that all required environment variables are set.
    
    Returns:
        Dict[str, bool]: Dictionary with variable names and their validation status
    """
    logger = get_run_logger()
    
    required_vars = [
        'BINANCE_URL',
        'COINGECKO_URL',
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA',
        'SNOWFLAKE_ROLE'
    ]
    
    results = {}
    
    for var in required_vars:
        value = os.getenv(var)
        results[var] = bool(value)
        status = '✅ Set' if results[var] else '❌ Missing'
        logger.info(f"{var}: {status}")
    
    return results


def generate_data_summary(crypto_df: pd.DataFrame, stock_df: pd.DataFrame) -> Dict:
    """
    Generate a summary of the ingested data.
    
    Args:
        crypto_df (pd.DataFrame): Cryptocurrency data
        stock_df (pd.DataFrame): Stock data
        
    Returns:
        Dict: Summary statistics
    """
    logger = get_run_logger()
    
    summary = {
        'crypto_records': len(crypto_df),
        'stock_records': len(stock_df),
        'total_records': len(crypto_df) + len(stock_df),
        'crypto_sources': crypto_df['source'].value_counts().to_dict() if not crypto_df.empty else {},
        'stock_sources': stock_df['source'].value_counts().to_dict() if not stock_df.empty else {},
        'unique_crypto_symbols': crypto_df['base_currency'].nunique() if not crypto_df.empty else 0,
        'unique_stock_tickers': stock_df['ticker'].nunique() if not stock_df.empty else 0,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"Data Summary: {summary}")
    return summary


def check_snowflake_connection() -> bool:
    """
    Test Snowflake connection using manual connector.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    logger = get_run_logger()
    
    try:
        import snowflake.connector
        from dotenv import load_dotenv
        
        load_dotenv()
        
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="SNOWFLAKE_JWT",
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        
        conn.close()
        
        logger.info(f"✅ Snowflake connection successful. Version: {result[0]}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Snowflake connection failed: {e}")
        return False


def check_snowflake_table_exists(table_name: str) -> bool:
    """
    Check if a Snowflake table exists using manual connector.
    
    Args:
        table_name (str): Full table name (database.schema.table)
        
    Returns:
        bool: True if table exists, False otherwise
    """
    logger = get_run_logger()
    
    try:
        import snowflake.connector
        from dotenv import load_dotenv
        
        load_dotenv()
        
        # Parse table name
        parts = table_name.split('.')
        if len(parts) != 3:
            logger.error(f"Invalid table name format: {table_name}. Expected: database.schema.table")
            return False
        
        database, schema, table = parts
        
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="SNOWFLAKE_JWT",
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        
        cursor = conn.cursor()
        
        # Check if table exists
        check_sql = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_CATALOG = '{database}'
        AND TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        """
        
        cursor.execute(check_sql)
        result = cursor.fetchone()
        exists = result[0] > 0 if result else False
        
        conn.close()
        
        status = '✅ Exists' if exists else '❌ Does not exist'
        logger.info(f"Table {table_name}: {status}")
        return exists
        
    except Exception as e:
        logger.error(f"Error checking Snowflake table existence: {e}")
        return False