"""
Kafka producer for generating and streaming fake stock and crypto transaction data.
This replaces the Prefect flow for continuous data generation.
"""

import os
import json
import time
import random
import uuid
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import List, Dict
import logging

from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
import pandas as pd

# UTC+7 timezone (e.g., Asia/Bangkok, Indochina Time)
UTC_PLUS_7 = timezone(timedelta(hours=7))




# ------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------
load_dotenv()
fake = Faker()

def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def json_serialize_value(v):
    """Serialize a value to JSON, handling datetime objects."""
    if isinstance(v, dict):
        return {k: json_serialize_value(val) for k, val in v.items()}
    elif isinstance(v, list):
        return [json_serialize_value(item) for item in v]
    elif isinstance(v, (datetime, date)):
        return v.isoformat()
    elif isinstance(v, pd.Timestamp):
        return v.isoformat()
    elif hasattr(v, 'isoformat'):  # Handle other datetime-like objects
        return v.isoformat()
    return v


logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Suppress Kafka library verbose logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.producer").setLevel(logging.WARNING)
logging.getLogger("kafka.client").setLevel(logging.WARNING)

# ------------------------------------------------------------------
# Kafka config
# ------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
# Hardcoded topic names as per requirements
KAFKA_TOPIC_CORPORATES = "raw_corporates"
KAFKA_TOPIC_CUSTOMERS = "raw_customers"
KAFKA_TOPIC_TRANSACTION_PERSONAL = "raw_transaction_personal"
KAFKA_TOPIC_TRANSACTION_CORPORATE = "raw_transaction_corporate"


# ==============================================================
# 5. Asset Lists + Price Ranges
# ==============================================================
# Data generation parameters
NUM_CUSTOMERS = int(os.getenv('NUM_CUSTOMERS', '1000'))
NUM_CORPORATES = int(os.getenv('NUM_CORPORATES', '100'))
NUM_STOCK_TRANSACTIONS_PER_BATCH = int(os.getenv('NUM_STOCK_TRANSACTIONS_PER_BATCH', '100'))
NUM_CRYPTO_TRANSACTIONS_PER_BATCH = int(os.getenv('NUM_CRYPTO_TRANSACTIONS_PER_BATCH', '60'))
BATCH_INTERVAL_SECONDS = int(os.getenv('BATCH_INTERVAL_SECONDS', '60'))

# Desired rate: ~N transactions per minute per transaction topic.
TRANSACTIONS_PER_TOPIC_PER_MIN = int(os.getenv('TRANSACTIONS_PER_TOPIC_PER_MIN', '3'))
DAYS_BACK = int(os.getenv('DAYS_BACK', '90'))
# Batch generation interval (in seconds)
BATCH_INTERVAL_SEC = int(os.getenv('BATCH_INTERVAL_SEC', '15'))  # Generate batch every 15 seconds
# How many records to generate per batch
NEW_CORPORATES_BATCH_SIZE = int(os.getenv('NEW_CORPORATES_BATCH_SIZE', '2'))
NEW_CUSTOMERS_BATCH_SIZE = int(os.getenv('NEW_CUSTOMERS_BATCH_SIZE', '3'))
NUM_TRANSACTIONS_PER_BATCH = int(os.getenv('NUM_TRANSACTIONS_PER_BATCH', '5'))  # Total transactions per batch

# Paths
STOCKLIST_PATH = Path("seeds/stocklist.txt")
CRYPTOLIST_PATH = Path("seeds/cryptolist.txt")

# Stock tickers (default fallback)
DEFAULT_STOCK_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 
    'AMD', 'INTC', 'CRM', 'ADBE', 'PYPL', 'UBER', 'SPOT', 'SQ', 'ROKU',
    'ZM', 'DOCU', 'SNOW', 'PLTR', 'CRWD', 'OKTA', 'DDOG', 'NET'
]

# Transaction types
COMPANY_TYPES = ['LLC', 'PUBLIC', 'PRIVATE']
TRANSACTION_TYPES = ['BUY', 'SELL']
CUSTOMER_GENDERS = ['M', 'F', 'Other']
CUSTOMER_AGE_GROUPS = ['18-25', '26-35', '36-45', '46-55', '56-65', '65+']

# Price ranges for different asset types
STOCK_PRICE_RANGES = {
    'NVDA': (400, 900), 'AAPL': (150, 250), 'MSFT': (300, 450), 'AMZN': (100, 200),
    'META': (250, 400), 'GOOGL': (100, 180), 'TSLA': (200, 350), 'NFLX': (350, 600),
    'AMD': (100, 200), 'INTC': (30, 50), 'CRM': (200, 300), 'ADBE': (400, 650),
    'PYPL': (50, 100), 'UBER': (30, 60), 'SPOT': (200, 300), 'SQ': (50, 100),
    'ROKU': (50, 100), 'ZM': (50, 100), 'DOCU': (40, 80), 'SNOW': (100, 200),
    'PLTR': (10, 35), 'CRWD': (150, 250), 'OKTA': (60, 100), 'DDOG': (80, 150),
    'NET': (50, 100)
}

CRYPTO_PRICE_RANGES = {
    'btc': (60000, 120000), 'eth': (2000, 4300), 'usdt': (0.99, 1.01), 'bnb': (200, 400),
    'sol': (100, 200), 'usdc': (0.99, 1.01), 'xrp': (0.3, 0.8), 'ada': (0.25, 0.8),
    'doge': (0.05, 0.25), 'trx': (0.05, 0.15), 'link': (10, 25), 'avax': (20, 50),
    'ltc': (50, 150), 'dot': (4, 10), 'uni': (5, 15), 'atom': (6, 12)
}


def load_asset_lists() -> tuple[List[str], List[str]]:
    """Load stock tickers and cryptocurrency symbols from seeds directory."""
    # Load stock tickers
    if STOCKLIST_PATH.exists():
        with open(STOCKLIST_PATH, "r") as file:
            stock_tickers = [line.strip().upper() for line in file.readlines() if line.strip()]
        #logger.info(f"Loaded {len(stock_tickers)} stock tickers from {STOCKLIST_PATH}")
    else:
        stock_tickers = DEFAULT_STOCK_TICKERS
        #logger.warning(f"{STOCKLIST_PATH} not found. Using default {len(stock_tickers)} stock tickers.")

    # Load crypto symbols
    if CRYPTOLIST_PATH.exists():
        with open(CRYPTOLIST_PATH, "r") as file:
            crypto_symbols = [line.strip().lower() for line in file.readlines() if line.strip()]
        #logger.info(f"Loaded {len(crypto_symbols)} crypto symbols from {CRYPTOLIST_PATH}")
    else:
        crypto_symbols = list(CRYPTO_PRICE_RANGES.keys())
        #logger.warning(f"{CRYPTOLIST_PATH} not found. Using default {len(crypto_symbols)} crypto symbols.")
    
    return stock_tickers, crypto_symbols


# ==============================================================
# 6. Generator Functions
# ==============================================================

def generate_corporate_demographics(num_corporates: int = 100) -> pd.DataFrame:
    """Generate corporate/company demographic data using Faker."""
    corporates = []
    
    for i in range(num_corporates):
        # Generate company name
        company_name = fake.company()
        
        # Generate company type (LLC, PUBLIC, PRIVATE)
        company_type = random.choices(COMPANY_TYPES, weights=[50, 20, 30])[0]
        
        # Generate country with realistic distribution
        country = random.choices(
            ['US', 'CA', 'UK', 'AU', 'DE', 'FR', 'JP', 'SG', 'NL', 'CH'],
            weights=[60, 10, 8, 5, 4, 4, 3, 2, 2, 2]
        )[0]
        
        # Generate year founded (between 1950 and 2020)
        year_founded = fake.random_int(min=1950, max=2020)
        
        # Generate tax number (EIN/TIN format varies by country)
        if country == 'US':
            # US EIN format: XX-XXXXXXX
            tax_number = f"{fake.random_int(10, 99)}-{fake.random_int(1000000, 9999999)}"
        elif country in ['CA', 'UK', 'AU']:
            # Format: XXXXXXXXX
            tax_number = str(fake.random_int(100000000, 999999999))
        else:
            # European format: XX.XXX.XXX/XXXX-XX
            tax_number = f"{fake.random_int(10, 99)}.{fake.random_int(100, 999)}.{fake.random_int(100, 999)}/{fake.random_int(1000, 9999)}-{fake.random_int(10, 99)}"
        
        # Generate primary office location
        office_location = fake.address().replace('\n', ', ')
        
        # Generate company email
        company_email = f"info@{fake.domain_name()}"
        
        # Generate company ID
        company_id = str(uuid.uuid4().hex[:12].upper())
        
        corporate = {
            'company_id': company_id,
            'company_name': company_name,
            'company_type': company_type,
            'company_email': company_email,
            'country': country,
            'year_founded': year_founded,
            'tax_number': tax_number,
            'office_primary_location': office_location,
            'registration_date': fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d')
        }
        corporates.append(corporate)
    
    df = pd.DataFrame(corporates)
    # Store as UTC+7 timezone-aware datetime using pandas Timestamp
    df['load_timestamp'] = pd.Timestamp.now(tz='Asia/Bangkok')  # UTC+7
    return df


def generate_customer(corporates_df: pd.DataFrame = None) -> Dict:
    """Generate a single customer record."""
    age = fake.random_int(min=18, max=80)
    if age <= 25:
        age_group = '18-25'
    elif age <= 35:
        age_group = '26-35'
    elif age <= 45:
        age_group = '36-45'
    elif age <= 55:
        age_group = '46-55'
    elif age <= 65:
        age_group = '56-65'
    else:
        age_group = '65+'
    
    gender = random.choices(['M', 'F', 'Other'], weights=[49, 49, 2])[0]
    country = random.choices(
        ['US', 'CA', 'UK', 'AU', 'DE', 'FR', 'JP', 'SG', 'NL', 'CH'],
        weights=[60, 10, 8, 5, 4, 4, 3, 2, 2, 2]
    )[0]

    customer_type = random.choices(['PERSONAL', 'CORPORATE'], weights=[80, 20])[0]
        
    # Link corporate customers to corporate records
    company_id = None
    first_name = None
    last_name = None
    email = None
    if customer_type == 'CORPORATE' and corporates_df is not None and len(corporates_df) > 0:
        corp = corporates_df.sample(1).iloc[0]
        customer_id=corp['company_id']
        company_id = corp['company_id']
        email = corp['company_email']
        age = None
        age_group = None
        gender=None
        first_name = None
        last_name = None
        country = corp['country']
        registration_date = corp['registration_date']
        
    else:
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        # Generate more realistic age group based on actual age
        age = fake.random_int(min=18, max=80)
        if age <= 25:
            age_group = '18-25'
        elif age <= 35:
            age_group = '26-35'
        elif age <= 45:
            age_group = '36-45'
        elif age <= 55:
            age_group = '46-55'
        elif age <= 65:
            age_group = '56-65'
        else:
            age_group = '65+'
        
        # Generate gender with realistic distribution
        gender = random.choices(['M', 'F', 'Other'], weights=[49, 49, 2])[0]
        
        # Generate country with realistic distribution (more US customers)
        country = random.choices(
            ['US', 'CA', 'UK', 'AU', 'DE', 'FR', 'JP', 'SG', 'NL', 'CH'],
            weights=[60, 10, 8, 5, 4, 4, 3, 2, 2, 2]
        )[0]
        registration_date= fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d')
        customer_id = str(uuid.uuid4().hex[:10].upper())
        company_id = None

    return {
        'customer_id': customer_id,
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'gender': gender,
        'age_group': age_group,
        'country': country,
        'registration_date': registration_date,
        'customer_tier': random.choices(['Bronze', 'Silver', 'Gold', 'Platinum'], weights=[40, 30, 20, 10])[0],
        'risk_tolerance': random.choices(['Conservative', 'Moderate', 'Aggressive'], weights=[30, 50, 20])[0],
        'customer_type': customer_type,
        'company_id': company_id,
        'load_timestamp': datetime.now(UTC_PLUS_7)
    }


def generate_stock_transaction(customer: Dict, stock_tickers: List[str], start_date: datetime, end_date: datetime) -> Dict:
    """Generate a single stock transaction."""
    ticker = random.choice(stock_tickers)
    transaction_type = random.choice(TRANSACTION_TYPES)
    
    # Generate realistic price
    if ticker in STOCK_PRICE_RANGES:
        min_price, max_price = STOCK_PRICE_RANGES[ticker]
        price_per_unit = round(random.uniform(min_price, max_price), 2)
    else:
        price_per_unit = round(random.uniform(10, 500), 2)
    
    # Generate quantity based on customer tier
    if customer['customer_type'] == 'CORPORATE':
        lots = random.choices(
            [1, 2, 5, 10, 20, 50],  # 100 → 5000
            weights=[10, 15, 25, 25, 15, 10]
        )[0]
    elif customer['customer_tier'] in ['Gold', 'Platinum']:
        lots = random.choices(
            [1, 2, 3, 5, 10],  # 100 → 1000
            weights=[25, 25, 20, 20, 10]
        )[0]
    elif customer['customer_tier'] == 'Silver':
        lots = random.choices(
            [1, 2, 3, 5],  # 100 → 500
            weights=[40, 30, 20, 10]
        )[0]
    else:  # Bronze
        lots = random.choices(
            [1, 2, 3],  # 100 → 300
            weights=[60, 30, 10]
        )[0]

    quantity = lots * 100

    if transaction_type == 'SELL':
        sell_lots = max(1, int(lots * random.uniform(0.5, 1.0)))
        quantity = sell_lots * 100
        price_per_unit *= random.uniform(0.995, 1.0)

    timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
    # Make timestamp timezone-aware (UTC+7)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC_PLUS_7)
    else:
        timestamp = timestamp.astimezone(UTC_PLUS_7)
    
    transaction_amount = round(price_per_unit * quantity, 2)
    fee_percentage = random.uniform(0.001, 0.01)
    fee_amount = round(transaction_amount * fee_percentage, 2)
    
    return {
        'transaction_id': str(uuid.uuid4()),
        'customer_id': customer['customer_id'],
        'asset_type': 'STOCK',
        'asset_symbol': ticker,
        'transaction_type': transaction_type,
        'quantity': quantity,
        'price_per_unit': round(price_per_unit, 2),
        'transaction_amount': transaction_amount,
        'fee_amount': fee_amount,
        'transaction_timestamp': timestamp,
        'data_date': timestamp.strftime('%Y-%m-%d'),
        'customer_tier': customer['customer_tier'],
        'customer_risk_tolerance': customer['risk_tolerance'],
        'customer_type': customer['customer_type'],        
        'data_source': 'KAFKA_PRODUCER',
        'load_timestamp': datetime.now(UTC_PLUS_7)
    }


def generate_crypto_transaction(customer: Dict, crypto_symbols: List[str], start_date: datetime, end_date: datetime) -> Dict:
    """Generate a single crypto transaction."""
    symbol = random.choice(crypto_symbols)
    transaction_type = random.choice(TRANSACTION_TYPES)
    
    # Generate realistic price
    if symbol in CRYPTO_PRICE_RANGES:
        min_price, max_price = CRYPTO_PRICE_RANGES[symbol]
        price_per_unit = round(random.uniform(min_price, max_price), 8)
    else:
        price_per_unit = round(random.uniform(0.01, 100), 8)
    
    # Generate quantity based on customer tier
    if customer['customer_type'] == 'CORPORATE':
        quantity = (
            round(random.uniform(1, 50), 6)
            if symbol in ['btc', 'eth']
            else round(random.uniform(1000, 100000), 2)
        )
    elif customer['customer_tier'] in ['Gold', 'Platinum']:
        quantity = (
            round(random.uniform(0.1, 5), 6)
            if symbol in ['btc', 'eth']
            else round(random.uniform(100, 10000), 2)
        )
    elif customer['customer_tier'] == 'Silver':
        quantity = (
            round(random.uniform(0.01, 1), 6)
            if symbol in ['btc', 'eth']
            else round(random.uniform(10, 1000), 2)
        )
    else:  # Bronze
        quantity = (
            round(random.uniform(0.001, 0.1), 6)
            if symbol in ['btc', 'eth']
            else round(random.uniform(1, 100), 2)
        )

    # SELL transactions typically smaller & discounted
    if transaction_type == 'SELL':
        quantity *= random.uniform(0.5, 1.0)
        price_per_unit *= random.uniform(0.995, 1.0)

    quantity = round(max(quantity, 0.000001), 6)

    timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
    # Make timestamp timezone-aware (UTC+7)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC_PLUS_7)
    else:
        timestamp = timestamp.astimezone(UTC_PLUS_7)
    
    transaction_amount = round(price_per_unit * quantity, 2)
    fee_percentage = random.uniform(0.001, 0.005)
    fee_amount = round(transaction_amount * fee_percentage, 2)
    
    return {
        'transaction_id': str(uuid.uuid4()),
        'customer_id': customer['customer_id'],
        'asset_type': 'CRYPTO',
        'asset_symbol': symbol.upper(),
        'transaction_type': transaction_type,
        'quantity': quantity,
        'price_per_unit': price_per_unit,
        'transaction_amount': transaction_amount,
        'fee_amount': fee_amount,
        'transaction_timestamp': timestamp,
        'data_date': timestamp.strftime('%Y-%m-%d'),
        'customer_tier': customer['customer_tier'],
        'customer_risk_tolerance': customer['risk_tolerance'],
        'customer_type': customer['customer_type'],        
        'data_source': 'KAFKA_PRODUCER',
        'load_timestamp': datetime.now(UTC_PLUS_7)
    }


# ==============================================================
# 7. Main
# ==============================================================

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(json_serialize_value(v), default=json_serializer).encode("utf-8"),
    )

    stocks, cryptos = load_asset_lists()

    # Maintain state across batches
    corporates_df = pd.DataFrame()  # Store all generated corporates
    customers = []  # Store all generated customers
    
    start = datetime.now(UTC_PLUS_7) - timedelta(days=DAYS_BACK)
    end = datetime.now(UTC_PLUS_7)

    logger.info(f"Starting producer with batch interval: {BATCH_INTERVAL_SEC} seconds")
    logger.info(f"Batch sizes - Corporates: {NEW_CORPORATES_BATCH_SIZE}, Customers: {NEW_CUSTOMERS_BATCH_SIZE}, Transactions: {NUM_TRANSACTIONS_PER_BATCH}")

    while True:
        batch_start_time = time.time()
        
        # Step 1: Generate corporates first (they don't depend on anything)
        logger.info("=" * 60)
        logger.info(f"Starting new batch at {datetime.now(UTC_PLUS_7).strftime('%Y-%m-%d %H:%M:%S')}")
        
        new_corporates_df = generate_corporate_demographics(NEW_CORPORATES_BATCH_SIZE)
        for _, corp in new_corporates_df.iterrows():
            corp_dict = corp.to_dict()
            # Ensure load_timestamp is in UTC+7 (pandas might normalize timezone)
            if 'load_timestamp' in corp_dict:
                ts = corp_dict['load_timestamp']
                if isinstance(ts, pd.Timestamp):
                    # If it's a pandas Timestamp, ensure it's in UTC+7
                    if ts.tz is None:
                        # Naive timestamp, assume it's already UTC+7 time
                        corp_dict['load_timestamp'] = ts.to_pydatetime().replace(tzinfo=UTC_PLUS_7)
                    elif str(ts.tz) == 'UTC':
                        # Convert from UTC to UTC+7
                        ts_utc7 = ts.tz_convert('Asia/Bangkok')
                        corp_dict['load_timestamp'] = ts_utc7.to_pydatetime()
                    else:
                        # Already in correct timezone, just convert to datetime
                        corp_dict['load_timestamp'] = ts.to_pydatetime()
            producer.send(KAFKA_TOPIC_CORPORATES, key=str(corp["company_id"]), value=corp_dict)
            logger.info(f"corporate {corp['company_id']} produced")
        
        # Update corporates_df to include new ones
        if corporates_df.empty:
            corporates_df = new_corporates_df
        else:
            corporates_df = pd.concat([corporates_df, new_corporates_df], ignore_index=True)
        producer.flush()
        
        # Step 2: Generate customers (they depend on corporates)
        new_customers = [generate_customer(corporates_df) for _ in range(NEW_CUSTOMERS_BATCH_SIZE)]
        for c in new_customers:
            # Use customer_id as key (company_id is None for personal customers)
            producer.send(KAFKA_TOPIC_CUSTOMERS, key=c["customer_id"], value=c)
            logger.info(f"customer {c['customer_id']} produced")
            customers.append(c)  # Add to the list for transaction generation
        producer.flush()
        
        # Step 3: Generate transactions (they depend on customers)
        # Generate transactions for both personal and corporate customers
        personal_transactions = []
        corporate_transactions = []
        
        for _ in range(NUM_TRANSACTIONS_PER_BATCH):
            if not customers:
                break
            
            customer = random.choice(customers)
            txn_fn = random.choice([generate_stock_transaction, generate_crypto_transaction])

            txn = txn_fn(
                customer,
                stocks if txn_fn == generate_stock_transaction else cryptos,
                start,
                end,
            )

            if customer["customer_type"] == "CORPORATE":
                corporate_transactions.append(txn)
            else:
                personal_transactions.append(txn)
        
        # Send personal transactions
        for txn in personal_transactions:
            producer.send(KAFKA_TOPIC_TRANSACTION_PERSONAL, key=txn["customer_id"], value=txn)
            logger.info(f"transaction_personal {txn['transaction_id']} produced")
        
        # Send corporate transactions
        for txn in corporate_transactions:
            producer.send(KAFKA_TOPIC_TRANSACTION_CORPORATE, key=txn["customer_id"], value=txn)
            logger.info(f"transaction_corporate {txn['transaction_id']} produced")
        
        producer.flush()
        
        # Update time window for transactions periodically
        start = datetime.now(UTC_PLUS_7) - timedelta(days=DAYS_BACK)
        end = datetime.now(UTC_PLUS_7)
        
        # Sleep until next batch interval
        elapsed = time.time() - batch_start_time
        sleep_time = max(0, BATCH_INTERVAL_SEC - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

# ==============================================================
# 8. Entry Point
# ==============================================================

if __name__ == "__main__":
    main()