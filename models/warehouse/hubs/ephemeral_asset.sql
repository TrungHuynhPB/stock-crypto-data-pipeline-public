{{ config(materialized='ephemeral',
    views_enabled=false, unique_key='asset_hk') }}

/*ephemeral_asset: This ephemeral removes duplicates from raw asset data source (each asset symbol and type should only have 1 unique record).*/
with all_assets as (

    select
        upper(base_currency) as asset_symbol,
        'crypto' as asset_type,
        cast(load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,
        source as record_source
    from {{ source('raw_pg', 'raw_cryptoprices_binance') }}

    union all

    select
        upper(base_currency) as asset_symbol,
        'crypto' as asset_type,
        cast(load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,
        source as record_source
    from {{ source('raw_pg', 'raw_cryptoprices_coingecko') }}

    union all

    select
        upper(base_currency) as asset_symbol,
        'crypto' as asset_type,
        cast(load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,
        source as record_source
    from {{ source('raw_pg', 'raw_cryptoprices_yfinance') }}

    union all

    select
        upper(ticker) as asset_symbol,
        'stock' as asset_type,
        cast(load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,
        coalesce(source, 'yfinance') as record_source
    from {{ source('raw_pg', 'raw_stock_prices_yfinance') }}

    union all

    -- capture assets introduced only via transactions
    select
        upper(asset_symbol) as asset_symbol,
        lower(asset_type) as asset_type,
        cast(load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,
        data_source as record_source
    from {{ source('raw_pg', 'raw_transaction_personal') }}
    union all
    select
        upper(asset_symbol) as asset_symbol,
        lower(asset_type) as asset_type,
        cast(load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,
        data_source as record_source
    from {{ source('raw_pg', 'raw_transaction_corporate') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['asset_symbol', 'asset_type']) }} as asset_hk,
    asset_symbol,
    asset_type,
    load_timestamp,
    record_source
from all_assets
