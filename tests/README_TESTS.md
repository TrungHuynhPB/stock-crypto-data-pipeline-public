# Custom dbt Tests Documentation

This directory contains custom data quality tests for the market data pipeline project. All tests are integrated with dbt and will run automatically when you execute `dbt test`.

## Test Categories

### 1. Transaction Data Quality Tests

#### `test_transaction_amount_calculation.sql`
- **Purpose**: Validates that `transaction_amount` equals `quantity * price_per_unit` (with 1 cent tolerance for rounding)
- **Model**: `fct_transactions`
- **Business Rule**: Transaction amounts must be mathematically correct

#### `test_transaction_positive_values.sql`
- **Purpose**: Ensures all transaction quantities, prices, and amounts are positive
- **Model**: `fct_transactions`
- **Business Rule**: No negative or zero values in financial transactions

#### `test_transaction_type_valid.sql`
- **Purpose**: Validates transaction types are either "Buy" or "Sell"
- **Model**: `fct_transactions`
- **Business Rule**: Transaction types must be valid enum values

#### `test_transaction_fee_reasonable.sql`
- **Purpose**: Ensures fees are non-negative, not greater than transaction amount, and not exceeding 10%
- **Model**: `fct_transactions`
- **Business Rule**: Fees must be within reasonable business constraints

#### `test_transaction_timestamp_not_future.sql`
- **Purpose**: Ensures no transactions are recorded with future timestamps
- **Model**: `fct_transactions`
- **Business Rule**: Transactions cannot occur in the future

#### `test_transaction_date_consistency.sql`
- **Purpose**: Validates that `transaction_timestamp` date matches `data_date`
- **Model**: `fct_transactions`
- **Business Rule**: Temporal consistency between timestamp and date fields

### 2. Price Data Quality Tests

#### `test_price_positive.sql`
- **Purpose**: Ensures all asset prices are positive
- **Model**: `fct_asset_prices`
- **Business Rule**: Prices must be greater than zero

#### `test_stock_price_range_valid.sql`
- **Purpose**: Validates stock price ranges (high >= low, close between low and high)
- **Model**: `sat_asset_price_stock`
- **Business Rule**: Stock price data must follow logical OHLC constraints

#### `test_week52_price_range_valid.sql`
- **Purpose**: Ensures 52-week high >= 52-week low
- **Model**: `dim_asset`
- **Business Rule**: 52-week price ranges must be logically consistent

#### `test_price_comparison_variance.sql`
- **Purpose**: Validates price variance across sources (Binance, CoinGecko, Yahoo Finance) is within 20%
- **Model**: `fct_asset_price_comparison`
- **Business Rule**: Prices from different sources should be reasonably consistent

#### `test_price_data_freshness.sql`
- **Purpose**: Ensures price data exists within the last 7 days
- **Model**: `fct_asset_prices`
- **Business Rule**: Active monitoring of data pipeline health

#### `test_volume_positive.sql`
- **Purpose**: Ensures trading volumes are non-negative
- **Model**: `fct_asset_prices`
- **Business Rule**: Volume cannot be negative

### 3. Dimension Data Quality Tests

#### `test_customer_tier_valid.sql`
- **Purpose**: Validates customer tier values (Silver, Gold, Platinum, Bronze, Basic)
- **Model**: `dim_customer`
- **Business Rule**: Customer tiers must be valid enum values

#### `test_risk_tolerance_valid.sql`
- **Purpose**: Validates risk tolerance values (Low, Medium, High, Conservative, Moderate, Aggressive)
- **Model**: `dim_customer`
- **Business Rule**: Risk tolerance must be valid enum values

#### `test_asset_type_valid.sql`
- **Purpose**: Validates asset types are either "Stock" or "Crypto"
- **Model**: `dim_asset`
- **Business Rule**: Asset types must be valid enum values

#### `test_email_format_valid.sql`
- **Purpose**: Validates email address format
- **Model**: `dim_customer`
- **Business Rule**: Email addresses must follow basic format rules

#### `test_year_founded_reasonable.sql`
- **Purpose**: Ensures company founding years are between 1000 and current year
- **Model**: `dim_company`
- **Business Rule**: Founding years must be within logical bounds

#### `test_pe_ratio_reasonable.sql`
- **Purpose**: Validates P/E ratios are between 0 and 1000
- **Model**: `dim_asset`
- **Business Rule**: P/E ratios must be within expected business ranges

### 4. SCD Type 2 (Slowly Changing Dimension) Tests

#### `test_scd2_effective_dates.sql`
- **Purpose**: Validates effective_from <= effective_to for historical records
- **Model**: `dim_customer_history`
- **Business Rule**: SCD Type 2 date ranges must be valid

#### `test_customer_history_no_overlaps.sql`
- **Purpose**: Ensures no overlapping effective date ranges for the same customer
- **Model**: `dim_customer_history`
- **Business Rule**: Historical records should not have temporal overlaps

### 5. News Data Quality Tests

#### `test_news_published_date_not_future.sql`
- **Purpose**: Ensures news articles are not dated in the future
- **Model**: `fct_news_events`
- **Business Rule**: News cannot be published in the future

#### `test_url_format_valid.sql`
- **Purpose**: Validates news URL format
- **Model**: `fct_news_events`
- **Business Rule**: URLs must follow basic format rules

#### `test_news_impact_returns_calculation.sql`
- **Purpose**: Validates calculated return values match formula: (price_tN - price_t0) / price_t0
- **Model**: `fct_asset_news_impact`
- **Business Rule**: Calculated returns must be mathematically accurate

## Running Tests

### Run all tests:
```bash
dbt test
```

### Run tests for a specific model:
```bash
dbt test --select fct_transactions
```

### Run a specific test:
```bash
dbt test --select test_transaction_amount_calculation
```

### Run tests with tags:
```bash
dbt test --select tag:data_quality
```

## Test Configuration

All tests are configured in `dbt_project.yml`:
```yaml
test-paths: ["tests"]
```

Tests are automatically discovered by dbt when placed in the `tests/` directory.

## Notes

- Most tests use tolerance values (e.g., 1 cent for amounts, 0.0001 for returns) to account for rounding differences
- Some tests may need adjustment based on your specific business rules (e.g., fee percentage limits, P/E ratio ranges)
- The `test_price_data_freshness.sql` test may fail during legitimate data gaps - adjust the 7-day threshold as needed
- Tests that check for enum values may need updates if your valid value sets change

## Adding New Tests

To add a new test:
1. Create a `.sql` file in the `tests/` directory
2. Write a SELECT query that returns rows when the test fails
3. Use `{{ ref('model_name') }}` to reference models
4. The test will automatically be discovered by dbt

Example test structure:
```sql
select *
from {{ ref('model_name') }}
where condition_that_should_never_be_true
```

