{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    views_enabled=false,
    incremental_strategy='append', tags=['trino','kafka']) }}
/*
raw_transaction_corporate: This raw table contains transaction data of corporate customers ingested from the source system.
*/
select
    transaction_id,
    customer_id,
    asset_type,
    asset_symbol,
    transaction_type,
    quantity,
    price_per_unit,
    transaction_amount,
    fee_amount,
    transaction_timestamp,
    data_date,
    customer_tier,
    customer_risk_tolerance,
    customer_type,
    data_source,
    load_timestamp,
    source
from {{ source('raw_pg', 'raw_transaction_corporate') }}

{% if is_incremental() %}
    where
        load_timestamp
        > (select coalesce(max(load_timestamp), timestamp '1900-01-01') from {{ this }})
{% endif %}
