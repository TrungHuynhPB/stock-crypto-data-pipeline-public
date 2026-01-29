# Prefect Flows Documentation

This document describes the four main Prefect flows deployed for data ingestion and transformation in the data engineering pipeline.

---

## Overview

The pipeline consists of four orchestrated flows that handle:
1. **Batch Data Pipeline**: Fake transaction data, customer demographics, and crypto news
2. **Crypto Prices Pipeline**: Real-time cryptocurrency price data from multiple sources
3. **Stock Prices Pipeline**: Stock market price data from yfinance
4. **Trino Incremental dbt**: Incremental dbt transformations for Kafka-consumed data

---

## 1. Batch Data Pipeline

**Flow Name**: `batch-data-pipeline`  
**Flow File**: `scripts/flow/flow__batch_data_s3_snowflake.py`  
**Entry Point**: `batch_data_s3_snowflake`

### Schedule
- **Cron Expression**: `0 20 * * *` (Daily at 8:00 PM)
- **Timezone**: `Asia/Ho_Chi_Minh`

### Data Gathered

This flow orchestrates multiple data generation and ingestion tasks:

1. **Fake Market Data** (`a1_1_raw_data_faker_generator.py`):
   - Personal customer transactions (stock and crypto)
   - Corporate customer transactions (stock and crypto)
   - Customer demographics (1000 customers by default)
   - Corporate/company demographics (200 corporates by default)
   - Transaction data includes: transaction_id, customer_id, asset_type, asset_symbol, transaction_type, quantity, price_per_unit, transaction_amount, fee_amount, timestamps

2. **Crypto News** (`a1_2_news_data_scrapper.py`):
   - Cryptocurrency news articles from Karpet API
   - News data includes: cryptocurrency, url, title, description, date, image
   - Reads from `seeds/cryptolist.txt` (limited by `news_read_limit` Prefect variable, default: 10)

### Destination Tables

**Snowflake Tables** (in `SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA`):
- `RAW_TRANSACTIONS`: Combined personal and corporate transactions
- `RAW_CUSTOMERS`: Customer demographic data
- `RAW_CORPORATES`: Corporate/company data
- `RAW_NEWS`: Cryptocurrency news articles

**PostgreSQL Tables** (in `stock_data` database):
- `raw_transactions_personal`: Personal customer transactions
- `raw_transactions_corporate`: Corporate customer transactions
- `raw_customers`: Customer demographics
- `raw_corporates`: Corporate data
- `raw_news`: Crypto news articles

### Flow Steps

1. **Generate Fake Market Data** (`generate_fake_market_data_flow`):
   - Generates customer and corporate demographics using Faker
   - Creates stock and crypto transactions with realistic pricing
   - Saves to CSV files with canonical date suffix (`YYYYMMDD_HHMMSS`)

2. **Scrape Crypto News** (`crypto_news_scraper`):
   - Fetches news articles for cryptocurrencies from Karpet API
   - Saves to `news_raw_{date}.csv`

3. **Upload to S3/MinIO** (`data_to_s3_flow`):
   - Uploads all CSV files to MinIO object storage:
     - `raw-data/transactions/personal/`
     - `raw-data/transactions/corporate/`
     - `raw-data/customers/`
     - `raw-data/corporates/`
     - `raw-data/crypto_news/`

4. **Load to Snowflake** (`crypto_news_s3_to_snowflake_flow`):
   - Downloads files from S3
   - Uploads to Snowflake stage (`MINIO_RAW_STAGE`)
   - Creates raw tables if not exists
   - Performs MERGE operations to load data

5. **Load to PostgreSQL** (`batch_s3_to_postgres_flow`):
   - Loads CSV data into PostgreSQL raw tables

6. **Run dbt Build** (`run_dbt_build_after_staging`):
   - Executes dbt transformations on Snowflake
   - Builds downstream models from raw tables

---

## 2. Crypto Prices Pipeline

**Flow Name**: `prices-crypto-pipeline`  
**Flow File**: `scripts/flow/flow__prices_data_s3_snowflake.py`  
**Entry Point**: `prices_data_s3_snowflake_flow`

### Schedule
- **Cron Expression**: `0 20 * * *` (Daily at 8:00 PM)
- **Timezone**: `Asia/Ho_Chi_Minh`

### Data Gathered

Fetches cryptocurrency price data from four sources:

1. **Binance** (`a2_1_crypto_binance.py`):
   - 24-hour ticker statistics
   - Price, volume, price change, high/low prices

2. **CoinGecko** (`a2_2_crypto_coingecko.py`):
   - Current market prices and volumes
   - Market cap and price change percentages

3. **yfinance** (`a2_3_crypto_yfinance.py`):
   - Historical and current crypto prices
   - Market data via Yahoo Finance API

4. **FreeCryptoAPI** (`a2_4_crypto_freecryptoapi.py`):
   - Generic crypto price API
   - Configurable via `FREECRYPTO_API` environment variable

All sources read cryptocurrency symbols from `seeds/cryptolist.txt` and fetch USD-paired prices.

### Destination Tables

**Snowflake Tables** (in `SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA_STAGING`):
- `RAW_CRYPTOPRICES_BINANCE`
- `RAW_CRYPTOPRICES_COINGECKO`
- `RAW_CRYPTOPRICES_YFINANCE`
- `RAW_CRYPTOPRICES_FREECRYPTOAPI`

**PostgreSQL Tables** (in `stock_data` database):
- `raw_cryptoprices_binance`
- `raw_cryptoprices_coingecko`
- `raw_cryptoprices_yfinance`
- `raw_cryptoprices_freecryptoapi`

**Common Schema**:
- `symbol` (VARCHAR): Cryptocurrency symbol
- `base_currency` (VARCHAR): Base currency (e.g., BTC, ETH)
- `quote_currency` (VARCHAR): Quote currency (USD)
- `price` (NUMERIC): Current price
- `volume` (NUMERIC): Trading volume
- `source` (VARCHAR): Data source name
- `observed_at` (TIMESTAMP): Observation timestamp
- `load_timestamp` (TIMESTAMP): Load timestamp

### Flow Steps

For each source (Binance, CoinGecko, yfinance, FreeCryptoAPI):

1. **Load Cryptocurrency List**: Reads from `seeds/cryptolist.txt`

2. **Fetch Price Data**: Calls respective API to get latest prices

3. **Save to CSV**: Writes to `data/crypto_{source}_{YYYYMMDD_HHMMSS}.csv`

4. **Upload to MinIO**: Uploads to `raw-data/crypto/{source}/`

5. **Upload to Snowflake Stage**: PUTs file to `MINIO_RAW_STAGE`

6. **Load to PostgreSQL**: Creates table if needed, loads CSV via COPY

7. **Load to Snowflake**: Creates table if needed, COPY INTO from stage

The flow runs all four sources sequentially, with error handling to continue if one source fails.

---

## 3. Stock Prices Pipeline

**Flow Name**: `prices-stock-pipeline`  
**Flow File**: `scripts/flow/flow__stock_prices_data_s3_snowflake.py`  
**Entry Point**: `stock_prices_data_s3_snowflake_flow`

### Schedule
- **Cron Expression**: `0 20 * * *` (Daily at 8:00 PM)
- **Timezone**: `Asia/Ho_Chi_Minh`

### Data Gathered

Fetches stock market price data from:

1. **yfinance** (`a3_1_stock_yfinance.py`):
   - Daily OHLCV (Open, High, Low, Close, Volume) data
   - Adjusted close prices
   - Dividends and stock splits
   - Company metadata (name, sector, industry)
   - Market cap, P/E ratio
   - 52-week high/low
   - Average volume

Reads stock tickers from `seeds/stocklist.txt`.

### Destination Tables

**Snowflake Tables** (in `SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA_STAGING`):
- `RAW_STOCK_PRICES_YFINANCE`

**PostgreSQL Tables** (in `stock_data` database):
- `raw_stock_prices_yfinance`

**Schema**:
- `ticker` (VARCHAR): Stock ticker symbol
- `date` (DATE): Trading date
- `open_price`, `high_price`, `low_price`, `close_price`, `adj_close_price` (NUMERIC)
- `volume` (NUMERIC): Trading volume
- `dividends`, `stock_splits` (NUMERIC)
- `company_name`, `sector`, `industry` (TEXT/VARCHAR)
- `market_cap`, `pe_ratio`, `week_52_high`, `week_52_low`, `avg_volume` (NUMERIC)
- `source` (VARCHAR): Data source name
- `observed_at` (TIMESTAMP): Observation timestamp
- `load_timestamp` (TIMESTAMP): Load timestamp

### Flow Steps

1. **Load Stock Ticker List**: Reads from `seeds/stocklist.txt`

2. **Fetch Stock Data**: Uses yfinance to get latest daily prices and metadata

3. **Save to CSV**: Writes to `data/stock_yfinance_{YYYYMMDD_HHMMSS}.csv`

4. **Upload to MinIO**: Uploads to `raw-data/stock/yfinance/`

5. **Upload to Snowflake Stage**: PUTs file to `MINIO_RAW_STAGE`

6. **Load to PostgreSQL**: Creates table if needed, loads CSV via COPY

7. **Load to Snowflake**: Creates table if needed, COPY INTO from stage

---

## 4. Trino Incremental dbt

**Flow Name**: `trino-incremental-dbt-5min`  
**Flow File**: `scripts/data_generation/b1_1_trino_incremental_dbt.py`  
**Entry Point**: `trino_incremental_dbt_flow`

### Schedule
- **Cron Expression**: `*/5 * * * *` (Every 5 minutes)
- **Timezone**: `Asia/Ho_Chi_Minh`

### Data Gathered

This flow processes data that has been consumed from Kafka and loaded into raw tables. It does not gather new data but transforms existing raw data.

**Input Tables** (from Kafka consumer):
- `raw_customers`: Customer data from Kafka
- `raw_corporates`: Corporate data from Kafka
- `raw_transaction_corporate`: Corporate transactions from Kafka
- `raw_transaction_personal`: Personal transactions from Kafka

### Destination Tables

Runs dbt transformations on **both Trino and Snowflake** targets:

**Trino Target** (`dev-trino`):
- Runs dbt models tagged with `trino` selector
- Processes raw tables in Trino catalog
- Builds downstream models that depend on the 4 raw tables

**Snowflake Target** (`dev-snowflake`):
- Runs dbt models tagged with `snowflake` selector
- Processes raw tables in Snowflake
- Builds downstream models that depend on the 4 raw tables

**dbt Command Pattern**:
```bash
dbt run --select raw_customers+ raw_corporates+ raw_transaction_corporate+ raw_transaction_personal+ --selector {trino|snowflake} --target {dev-trino|dev-snowflake}
```

The `+` selector includes all downstream models that depend on these raw tables.

### Flow Steps

1. **Run dbt with Trino Selector**:
   - Executes: `dbt run --select raw_customers+ raw_corporates+ raw_transaction_corporate+ raw_transaction_personal+ --selector trino --target dev-trino`
   - Only runs models tagged with `trino` selector
   - Ensures Trino-specific views/models are built

2. **Run dbt with Snowflake Selector**:
   - Executes: `dbt run --select raw_customers+ raw_corporates+ raw_transaction_corporate+ raw_transaction_personal+ --selector snowflake --target dev-snowflake`
   - Only runs models tagged with `snowflake` selector
   - Ensures Snowflake-specific views/models are built

3. **Return Results**:
   - Returns return codes and success status for both runs
   - Logs stdout/stderr from dbt execution

### Notes

- This flow is designed to run **incrementally** every 5 minutes to process new data from Kafka
- It uses dbt selectors to ensure only appropriate models run on each target
- The flow assumes raw tables are already populated by Kafka consumers
- Both Trino and Snowflake transformations run independently

---

## Summary Table

| Flow Name | Schedule | Data Sources | Main Destination Tables | Purpose |
|-----------|----------|--------------|-------------------------|---------|
| **batch-data-pipeline** | Daily 8 PM | Faker, Karpet API | `RAW_TRANSACTIONS`, `RAW_CUSTOMERS`, `RAW_CORPORATES`, `RAW_NEWS` | Generate fake transaction data and scrape crypto news |
| **prices-crypto-pipeline** | Daily 8 PM | Binance, CoinGecko, yfinance, FreeCryptoAPI | `RAW_CRYPTOPRICES_*` (4 tables) | Fetch cryptocurrency prices from multiple sources |
| **prices-stock-pipeline** | Daily 8 PM | yfinance | `RAW_STOCK_PRICES_YFINANCE` | Fetch stock market prices |
| **trino-incremental-dbt-5min** | Every 5 minutes | Kafka raw tables | dbt transformed models (Trino & Snowflake) | Transform Kafka-consumed data using dbt |

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
├─────────────────────────────────────────────────────────────┤
│ • Faker (fake transactions)                                 │
│ • Karpet API (crypto news)                                  │
│ • Binance/CoinGecko/yfinance (crypto prices)               │
│ • yfinance (stock prices)                                   │
│ • Kafka (real-time transactions)                            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Prefect Flows (Orchestration)                   │
├─────────────────────────────────────────────────────────────┤
│ • batch-data-pipeline (daily)                              │
│ • prices-crypto-pipeline (daily)                           │
│ • prices-stock-pipeline (daily)                            │
│ • trino-incremental-dbt-5min (every 5 min)                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Lake (MinIO/S3)                           │
│  • raw-data/transactions/                                   │
│  • raw-data/customers/                                       │
│  • raw-data/crypto/                                         │
│  • raw-data/stock/                                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│         Snowflake Stage (MINIO_RAW_STAGE)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Warehouse                                  │
├─────────────────────────────────────────────────────────────┤
│ Snowflake: RAW_* tables → dbt models → marts              │
│ Trino: RAW_* tables → dbt models → federated queries       │
│ PostgreSQL: raw_* tables (for local queries)               │
└─────────────────────────────────────────────────────────────┘
```

---

## Configuration

All flows use environment variables for configuration. Key variables include:

- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_SCHEMA_STAGING`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `FREECRYPTO_API`, `FREECRYPTO_API_KEY` (for FreeCryptoAPI source)

Prefect Variables:
- `faker_num_customers`: Number of customers to generate (default: 1000)
- `faker_num_stock_transactions`: Number of stock transactions (default: 5000)
- `faker_num_crypto_transactions`: Number of crypto transactions (default: 3000)
- `news_read_limit`: Number of cryptocurrencies to fetch news for (default: 10)

---

## Error Handling

All flows implement error handling:
- Individual subflow failures don't stop the entire flow
- Errors are logged with detailed messages
- Partial success is tracked and reported
- Failed steps are logged but subsequent steps continue when possible

---

## Monitoring

Monitor flows via:
- **Prefect UI**: View flow runs, logs, and task status
- **Grafana Dashboards**: System metrics and flow execution times
- **Prometheus**: Metrics collection for flow performance

