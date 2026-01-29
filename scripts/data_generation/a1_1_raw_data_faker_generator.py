"""
Fake transaction data generator for stock and crypto market data.
Generates realistic transaction data with customer demographics, transaction details, and timestamps.
"""

import os
import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple
import uuid

from prefect import flow, task, get_run_logger
from prefect.variables import Variable
from dotenv import load_dotenv
from faker import Faker
from scripts.utils.date_utils import get_canonical_data_date

# Load environment variables
load_dotenv()

# Initialize Faker
fake = Faker()

# Configuration
LOCAL_DATA_DIR = Path("data")
STOCKLIST_PATH = Path("seeds/stocklist.txt")
CRYPTOLIST_PATH = Path("seeds/cryptolist.txt")

# Stock tickers (from existing configuration)
DEFAULT_STOCK_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 
    'AMD', 'INTC', 'CRM', 'ADBE', 'PYPL', 'UBER', 'SPOT', 'SQ', 'ROKU',
    'ZM', 'DOCU', 'SNOW', 'PLTR', 'CRWD', 'OKTA', 'DDOG', 'NET'
]

# Transaction types
TRANSACTION_TYPES = ['BUY', 'SELL']
CUSTOMER_GENDERS = ['M', 'F', 'Other']
CUSTOMER_AGE_GROUPS = ['18-25', '26-35', '36-45', '46-55', '56-65', '65+']

# Company types
COMPANY_TYPES = ['LLC', 'PUBLIC', 'PRIVATE']

# Price ranges for different asset types (approximate current values)
STOCK_PRICE_RANGES = {
    'NVDA': (400, 900), 'AAPL': (150, 250), 'MSFT': (300, 450), 'AMZN': (100, 200),
    'META': (250, 400), 'AVGO': (800, 1200), 'GOOGL': (100, 180), 'GOOG': (100, 180),
    'TSLA': (200, 350), 'NFLX': (350, 600), 'PLTR': (10, 35), 'COST': (500, 800),
    'AMD': (100, 200), 'ASML': (600, 1000), 'CSCO': (45, 70), 'AZN': (50, 80),
    'MU': (60, 120), 'TMUS': (130, 180), 'SHOP': (50, 100), 'APP': (30, 60),
    'LIN': (350, 500), 'PEP': (150, 200), 'ISRG': (250, 400), 'LRCX': (600, 900),
    'INTU': (400, 650), 'PDD': (100, 200), 'QCOM': (120, 200), 'AMAT': (100, 200),
    'INTC': (30, 50), 'ARM': (80, 150), 'BKNG': (3000, 4000), 'AMGN': (250, 350),
    'KLAC': (400, 600), 'TXN': (150, 220), 'GILD': (60, 100), 'ADBE': (400, 650),
    'PANW': (250, 400), 'HON': (180, 250), 'CRWD': (150, 250), 'CEG': (70, 120),
    'ADI': (150, 220), 'ADP': (200, 280), 'DASH': (70, 130), 'MELI': (1000, 1600),
    'SBUX': (80, 120), 'SNPS': (400, 600), 'VRSK': (200, 300), 'BIIB': (200, 300),
    'REGN': (700, 1000), 'CSX': (30, 45), 'MRVL': (60, 100), 'JD': (25, 45),
    'ZM': (50, 100), 'DOCU': (40, 80), 'IDXX': (450, 650), 'ORLY': (900, 1200),
    'MCHP': (70, 120), 'ROST': (120, 180), 'MAR': (160, 220), 'VRTX': (350, 500),
    'DXCM': (80, 130), 'ILMN': (100, 200), 'CTSH': (60, 90), 'NXPI': (150, 250),
    'EXC': (35, 60), 'CDNS': (300, 450), 'CTAS': (500, 650), 'FAST': (50, 80),
    'SWKS': (90, 130), 'EBAY': (35, 60), 'EA': (100, 150), 'TCOM': (30, 50),
    'BIDU': (90, 150), 'LULU': (300, 450), 'SIRI': (3, 8), 'MNST': (45, 70),
    'XEL': (50, 70), 'WDC': (40, 70), 'PAYX': (100, 140), 'TTWO': (120, 180),
    'AEP': (70, 100), 'VRSN': (180, 250), 'PCAR': (80, 130), 'ALGN': (200, 350),
    'CDW': (180, 250), 'MRNA': (80, 150), 'OKTA': (60, 100), 'KHC': (30, 50)
}

CRYPTO_PRICE_RANGES = {
    'btc': (60000, 120000), 'eth': (2000, 4300), 'usdt': (0.99, 1.01), 'bnb': (200, 400),
    'xrp': (0.02, 3.84), 'sol': (0.3, 294), 'usdc': (0.99, 1.01), 'trx': (0.05, 0.15), 'doge': (0.05, 0.25),
    'ada': (0.25, 0.8), 'hype': (1, 10), 'usde': (0.99, 1.01), 'link': (10, 25),
    'xlm': (0.1, 0.3), 'sui': (1, 3), 'bch': (200, 500), 'avax': (20, 50),
    'leo': (3, 8), 'hbar': (0.05, 0.15), 'ltc': (50, 150), 'shib': (0.00001, 0.00005),
    'xmr': (100, 200), 'ton': (2, 10), 'dai': (0.99, 1.01),
    'cro': (0.05, 0.15), 'dot': (4, 10), 'tao': (200, 400), 'uni': (5, 15),
    'zec': (20, 60), 'okb': (40, 80), 'ena': (0.5, 2), 'aave': (50, 150),
    'bgb': (0.3, 1), 'wlfi': (0.1, 1), 'pepe': (0.000001, 0.000005),
    'near': (3, 8), 'pyusd': (0.99, 1.01), 'usd1': (0.99, 1.01), 'etc': (15, 30),
    'aster': (0.05, 0.5), 'ondo': (0.8, 1.5), 'apt': (5, 12), 'pol': (0.1, 1),
    'wld': (1, 5), 'kcs': (5, 10), 'icp': (3, 8), 'algo': (0.1, 0.3),
    'atom': (6, 12), 'vet': (0.02, 0.05), 'kas': (0.05, 0.15), 'pengu': (0.01, 0.05),
    'sky': (0.5, 2), 'pump': (0.001, 0.01), 'paxg': (1800, 2200), 'flr': (0.01, 0.05),
    'render': (5, 10), 'gt': (5, 10), 'sei': (0.2, 1), 'bonk': (0.000001, 0.00001),
    'fil': (3, 10), 'xaut': (1800, 2200), 'qnt': (80, 160), 'imx': (1, 3),
    'cake': (1, 5), 'fdusd': (0.99, 1.01), 'rlusd': (0.99, 1.01),
    'grt': (0.1, 0.3), 'morpho': (1, 3), 'floki': (0.00001, 0.0001),
    'kaia': (0.5, 2), 'pyth': (0.3, 1), 'twt': (0.5, 2)
}

def get_unit_price(symbol: str, asset_type: str) -> float:
    if asset_type == "STOCK":
        low, high = STOCK_PRICE_RANGES[symbol]
        return round(random.uniform(low, high), 2)
    elif asset_type == "CRYPTO":
        low, high = CRYPTO_PRICE_RANGES[symbol]
        # crypto prices can be more granular
        return round(random.uniform(low, high), 6)
    else:
        raise ValueError("Unknown asset type")

@task(name="Load Asset Lists")
def load_asset_lists() -> Tuple[List[str], List[str]]:
    """Load stock tickers and cryptocurrency symbols from seeds directory."""
    logger = get_run_logger()
    
    # Load stock tickers
    if STOCKLIST_PATH.exists():
        with open(STOCKLIST_PATH, "r") as file:
            stock_tickers = [line.strip().upper() for line in file.readlines() if line.strip()]
        logger.info(f"Loaded {len(stock_tickers)} stock tickers from {STOCKLIST_PATH}")
    else:
        stock_tickers = DEFAULT_STOCK_TICKERS
        logger.warning(f"{STOCKLIST_PATH} not found. Using default {len(stock_tickers)} stock tickers.")

    # Load crypto symbols
    if CRYPTOLIST_PATH.exists():
        with open(CRYPTOLIST_PATH, "r") as file:
            crypto_symbols = [line.strip().lower() for line in file.readlines() if line.strip()]
        logger.info(f"Loaded {len(crypto_symbols)} crypto symbols from {CRYPTOLIST_PATH}")
    else:
        crypto_symbols = list(CRYPTO_PRICE_RANGES.keys())
        logger.warning(f"{CRYPTOLIST_PATH} not found. Using default {len(crypto_symbols)} crypto symbols.")
    
    return stock_tickers, crypto_symbols


@task(name="Generate Corporate Demographics")
def generate_corporate_demographics(num_corporates: int = 200) -> pd.DataFrame:
    """Generate corporate/company demographic data using Faker."""
    logger = get_run_logger()
    
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
    df['load_timestamp'] = datetime.now().isoformat()
    logger.info(f"Generated {len(df)} corporate records using Faker")
    logger.info(f"Corporate distribution by country: {df['country'].value_counts().head().to_dict()}")
    logger.info(f"Corporate distribution by type: {df['company_type'].value_counts().to_dict()}")
    return df


@task(name="Generate Customer Demographics")
def generate_customer_demographics(num_customers: int = 1000, corporates_df: pd.DataFrame = None) -> pd.DataFrame:
    """Generate customer demographic data using Faker."""
    logger = get_run_logger()
    
    customers = []
    
    # Set seed for reproducible results (optional)
    # fake.seed(42)
    
    for i in range(num_customers):
        customer_type = random.choices(['PERSONAL', 'CORPORATE'], weights=[80, 20])[0]

        company_id = None
        first_name = None
        last_name = None
        email = None
        # Generate realistic customer data using Faker
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
        # Generate customer type (PERSONAL or CORPORATE)
        # Approximately 80% personal, 20% corporate
        #customer_type = random.choices(['PERSONAL', 'CORPORATE'], weights=[80, 20])[0]
        
        # Link corporate customers to corporate records
        #company_id = None
        #if customer_type == 'CORPORATE' and corporates_df is not None and len(corporates_df) > 0:
        #    company_id = corporates_df.sample(1).iloc[0]['company_id']
        
        customer = {
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
            'company_id': company_id
        }
        customers.append(customer)
    
    df = pd.DataFrame(customers)
    df['load_timestamp'] = datetime.now().isoformat()
    logger.info(f"Generated {len(df)} customer records using Faker")
    logger.info(f"Customer distribution by country: {df['country'].value_counts().head().to_dict()}")
    logger.info(f"Customer distribution by tier: {df['customer_tier'].value_counts().to_dict()}")
    logger.info(f"Customer distribution by type: {df['customer_type'].value_counts().to_dict()}")
    return df



@task(name="Generate Stock Transactions")
def generate_stock_transactions(
    customers_df: pd.DataFrame,
    stock_tickers: List[str],
    num_transactions: int = 5000,
    start_date: datetime = None,
    end_date: datetime = None
) -> pd.DataFrame:
    logger = get_run_logger()

    if start_date is None:
        start_date = datetime.now() - timedelta(days=90)
    if end_date is None:
        end_date = datetime.now() - timedelta(days=1)

    transactions = []

    for _ in range(num_transactions):
        customer = customers_df.sample(1).iloc[0]
        ticker = random.choice(stock_tickers)
        transaction_type = random.choice(TRANSACTION_TYPES)

        # -------------------------------
        # 1️⃣ Price per unit (market-based)
        # -------------------------------
        low, high = STOCK_PRICE_RANGES[ticker]
        price_per_unit = round(random.uniform(low, high), 2)

        # -------------------------------
        # 2️⃣ Quantity (multiples of 100)
        # -------------------------------
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

        # SELL trades are often smaller
        if transaction_type == 'SELL':
            sell_lots = max(1, int(lots * random.uniform(0.5, 1.0)))
            quantity = sell_lots * 100
            price_per_unit *= random.uniform(0.995, 1.0)

        # -------------------------------
        # 3️⃣ Transaction amount (derived)
        # -------------------------------
        transaction_amount = round(quantity * price_per_unit, 2)

        # -------------------------------
        # 4️⃣ Fees (stocks: 0.1% – 1%)
        # -------------------------------
        fee_percentage = random.uniform(0.001, 0.01)
        fee_amount = round(transaction_amount * fee_percentage, 2)

        # -------------------------------
        # 5️⃣ Timestamp
        # -------------------------------
        timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)

        transactions.append({
            'transaction_id': str(uuid.uuid4()),
            'customer_id': customer['customer_id'],
            'asset_type': 'STOCK',
            'asset_symbol': ticker,
            'transaction_type': transaction_type,
            'quantity': quantity,
            'price_per_unit': round(price_per_unit, 2),
            'transaction_amount': transaction_amount,
            'fee_amount': fee_amount,
            'transaction_timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'data_date': timestamp.strftime('%Y-%m-%d'),
            'customer_tier': customer['customer_tier'],
            'customer_risk_tolerance': customer['risk_tolerance'],
            'customer_type': customer['customer_type']
        })

    df = pd.DataFrame(transactions)
    logger.info(f"Generated {len(df)} stock transaction records")

    return df


@task(name="Generate Crypto Transactions")
def generate_crypto_transactions(
    customers_df: pd.DataFrame,
    crypto_symbols: List[str],
    num_transactions: int = 3000,
    start_date: datetime = None,
    end_date: datetime = None
) -> pd.DataFrame:
    logger = get_run_logger()

    if start_date is None:
        start_date = datetime.now() - timedelta(days=90)
    if end_date is None:
        end_date = datetime.now()

    transactions = []

    for _ in range(num_transactions):
        customer = customers_df.sample(1).iloc[0]
        symbol = random.choice(crypto_symbols).lower()
        transaction_type = random.choice(TRANSACTION_TYPES)

        # -------------------------------
        # 1️⃣ Price per unit (market-based)
        # -------------------------------
        low, high = CRYPTO_PRICE_RANGES[symbol]
        price_per_unit = round(random.uniform(low, high), 6)

        # -------------------------------
        # 2️⃣ Quantity (customer-driven)
        # -------------------------------
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

        # -------------------------------
        # 3️⃣ Transaction amount (derived)
        # -------------------------------
        transaction_amount = round(quantity * price_per_unit, 2)

        # -------------------------------
        # 4️⃣ Fees (crypto usually 0.1–0.5%)
        # -------------------------------
        fee_percentage = random.uniform(0.001, 0.005)
        fee_amount = round(transaction_amount * fee_percentage, 2)

        # -------------------------------
        # 5️⃣ Timestamp
        # -------------------------------
        timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)

        transactions.append({
            'transaction_id': str(uuid.uuid4()),
            'customer_id': customer['customer_id'],
            'asset_type': 'CRYPTO',
            'asset_symbol': symbol.upper(),
            'transaction_type': transaction_type,
            'quantity': quantity,
            'price_per_unit': price_per_unit,
            'transaction_amount': transaction_amount,
            'fee_amount': fee_amount,
            'transaction_timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'data_date': timestamp.strftime('%Y-%m-%d'),
            'customer_tier': customer['customer_tier'],
            'customer_risk_tolerance': customer['risk_tolerance'],
            'customer_type': customer['customer_type']
        })

    df = pd.DataFrame(transactions)
    logger.info(f"Generated {len(df)} crypto transaction records")

    return df



@task(name="Combine and Clean Transaction Data")
def combine_transaction_data(stock_df: pd.DataFrame, crypto_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Combine stock and crypto transactions, split by customer type, and perform cleaning."""
    logger = get_run_logger()
    
    # Combine dataframes
    combined_df = pd.concat([stock_df, crypto_df], ignore_index=True)
    
    # Sort by timestamp
    combined_df = combined_df.sort_values('transaction_timestamp')
    
    # Add additional fields
    combined_df['load_timestamp'] = datetime.now().isoformat()
    combined_df['data_source'] = 'FAKE_DATA_GENERATOR'
    
    # Clean data
    combined_df = combined_df.dropna(subset=['transaction_id', 'customer_id', 'asset_symbol'])
    
    # Split into personal and corporate customer transactions
    personal_transactions = combined_df[combined_df['customer_type'] == 'PERSONAL'].copy()
    corporate_transactions = combined_df[combined_df['customer_type'] == 'CORPORATE'].copy()
    
    logger.info(f"Combined and cleaned data: {len(combined_df)} total transactions")
    logger.info(f"Stock transactions: {len(stock_df)}")
    logger.info(f"Crypto transactions: {len(crypto_df)}")
    #logger.info(f"Personal customer transactions: {len(personal_transactions)}")
    #logger.info(f"Corporate customer transactions: {len(corporate_transactions)}")
    
    return personal_transactions, corporate_transactions


@task(name="Save Transaction Data to CSV")
def save_transaction_data_to_csv(
    personal_df: pd.DataFrame, 
    corporate_df: pd.DataFrame, 
    customers_df: pd.DataFrame,
    corporates_df: pd.DataFrame = None,
    data_date: str | None = None,
) -> Tuple[Path, Path, Path, Path]:
    """Save personal and corporate transaction data, customer data, and corporate demographics to CSV files.
    Filenames are suffixed with a canonical, filename-safe data_date (YYYYMMDD_HHMMSS).
    """
    logger = get_run_logger()
    
    # Ensure data directory exists
    LOCAL_DATA_DIR.mkdir(exist_ok=True, parents=True)
    
    # Create filenames with canonical run data_date
    run_suffix = get_canonical_data_date(data_date)
    
    personal_transactions_filename = f"fake_personal_customers_transactions_{run_suffix}.csv"
    corporate_transactions_filename = f"fake_corporate_customers_transactions_{run_suffix}.csv"
    customers_filename = f"fake_customers_{run_suffix}.csv"
    corporates_filename = f"fake_corporates_{run_suffix}.csv"
    
    personal_transactions_filepath = LOCAL_DATA_DIR / personal_transactions_filename
    corporate_transactions_filepath = LOCAL_DATA_DIR / corporate_transactions_filename
    customers_filepath = LOCAL_DATA_DIR / customers_filename
    corporates_filepath = LOCAL_DATA_DIR / corporates_filename
    
    # Save to CSV
    personal_df.to_csv(personal_transactions_filepath, index=False, encoding='utf-8')
    corporate_df.to_csv(corporate_transactions_filepath, index=False, encoding='utf-8')
    customers_df.to_csv(customers_filepath, index=False, encoding='utf-8')
    
    if corporates_df is not None and len(corporates_df) > 0:
        corporates_df.to_csv(corporates_filepath, index=False, encoding='utf-8')
        logger.info(f"Saved {len(corporates_df)} corporate records to {corporates_filepath}")
    
    logger.info(f"Saved {len(personal_df)} personal customer transactions to {personal_transactions_filepath}")
    logger.info(f"Saved {len(corporate_df)} corporate customer transactions to {corporate_transactions_filepath}")
    logger.info(f"Saved {len(customers_df)} customers to {customers_filepath}")
    
    return personal_transactions_filepath, corporate_transactions_filepath, customers_filepath, corporates_filepath


@flow(name="1_Generate_Fake_Market_Data")
def generate_fake_market_data_flow(
    num_customers: int = 1000,
    num_stock_transactions: int = 5000,
    num_crypto_transactions: int = 3000,
    days_back: int = 90,
    data_date: str | None = None,
):
    """Main flow for generating fake market transaction data.
    A canonical data_date (YYYYMMDD_HHMMSS) is used for all output filenames so downstream steps can find them.
    """
    logger = get_run_logger()

    #Prefect variables override
    num_customers = int(
        Variable.get("faker_num_customers", default=num_customers)
    )
    num_stock_transactions = int(
        Variable.get("faker_num_stock_transactions", default=num_stock_transactions)
    )
    num_crypto_transactions = int(
        Variable.get("faker_num_crypto_transactions", default=num_crypto_transactions)
    )

    logger.info(
        f"Using parameters: "
        f"customers={num_customers}, "
        f"stock_txns={num_stock_transactions}, "
        f"crypto_txns={num_crypto_transactions}"
    )

    try:
        logger.info("🚀 Starting fake market data generation flow")
        
        # Set date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        run_suffix = get_canonical_data_date(data_date)
        logger.info(f"📅 Using data_date suffix: {run_suffix}")
        
        # 1. Load asset lists
        stock_tickers, crypto_symbols = load_asset_lists()
        
        # 2. Generate corporate demographics first (needed for linking)
        num_corporates = max(1, int(num_customers * 0.2))  # 20% of customers are corporate
        corporates_df = generate_corporate_demographics(num_corporates)
        
        # 3. Generate customer demographics (link corporate customers to corporates)
        customers_df = generate_customer_demographics(num_customers, corporates_df)
        
        # 4. Generate stock transactions
        stock_transactions_df = generate_stock_transactions(
            customers_df, stock_tickers, num_stock_transactions, start_date, end_date
        )
        
        # 5. Generate crypto transactions
        crypto_transactions_df = generate_crypto_transactions(
            customers_df, crypto_symbols, num_crypto_transactions, start_date, end_date
        )
        
        # 6. Combine and clean data, split by customer type
        personal_transactions_df, corporate_transactions_df = combine_transaction_data(
            stock_transactions_df, crypto_transactions_df
        )
        
        # 7. Save to CSV files
        personal_filepath, corporate_filepath, customers_filepath, corporates_filepath = save_transaction_data_to_csv(
            personal_transactions_df, corporate_transactions_df, customers_df, corporates_df, data_date=run_suffix
        )
        
        logger.info("✅ Fake market data generation flow completed successfully")
        
        return {
            'data_date': run_suffix,
            'personal_transactions_file': str(personal_filepath),
            'corporate_transactions_file': str(corporate_filepath),
            'customers_file': str(customers_filepath),
            'corporates_file': str(corporates_filepath),
            'total_transactions': len(personal_transactions_df) + len(corporate_transactions_df),
            'personal_transactions': len(personal_transactions_df),
            'corporate_transactions': len(corporate_transactions_df),
            'total_customers': len(customers_df),
            'total_corporates': len(corporates_df),
            'stock_transactions': len(stock_transactions_df),
            'crypto_transactions': len(crypto_transactions_df)
        }
        
    except Exception as e:
        logger.error(f"❌ Flow failed: {e}")
        raise


if __name__ == "__main__":
    # Run with default parameters
    generate_fake_market_data_flow()
