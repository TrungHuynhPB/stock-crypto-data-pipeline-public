{{ config(
    materialized='incremental',
    unique_key=['ticker','date'],
    views_enabled=false,
    incremental_strategy='append', tags=['trino']) }}
/*
raw_stockprices_yfinance: This raw table contains stock price data ingested from Yahoo Finance.
*/
select
    ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close_price,
    volume,
    dividends,
    stock_splits,
    company_name,
    sector,
    industry,
    market_cap,
    pe_ratio,
    week_52_high,
    week_52_low,
    avg_volume,
    source,
    observed_at,
    load_timestamp
from {{ source('raw_pg', 'raw_stock_prices_yfinance') }}

{% if is_incremental() %}
    where
        observed_at
        > (select coalesce(max(observed_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
