{{ config(
    materialized='incremental',
    unique_key=['symbol','observed_at'],
    views_enabled=false,
    incremental_strategy='append', tags=['trino']) }}
/*
raw_cryptoprices_yfinance: This raw table contains cryptocurrency price data ingested from Yahoo Finance.
*/
select
    symbol,
    base_currency,
    quote_currency,
    price,
    volume,
    source,
    observed_at,
    load_timestamp
from {{ source('raw_pg', 'raw_cryptoprices_yfinance') }}

{% if is_incremental() %}
    where
        observed_at
        > (select coalesce(max(observed_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
