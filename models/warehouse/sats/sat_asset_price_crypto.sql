{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='asset_price_crypto_hk',
    views_enabled=false,
    tags=['trino']
) }}

/*
sat_asset_price_crypto
- Insert-only DV satellite
- Grain: asset_hk + observed_at + record_source + hashdiff
*/

with source_prices as (

    select
        upper(base_currency) as asset_symbol,
        'crypto' as asset_type,
        source as asset_source,
        symbol,
        base_currency,
        quote_currency,
        price,
        volume,
        observed_at,
        load_timestamp
    from {{ source('raw_pg', 'raw_cryptoprices_binance') }}

    union all

    select
        upper(base_currency) as asset_symbol,
        'crypto' as asset_type,
        source,
        symbol,
        base_currency,
        quote_currency,
        price,
        volume,
        observed_at,
        load_timestamp
    from {{ source('raw_pg', 'raw_cryptoprices_coingecko') }}

    union all

    select
        upper(base_currency) as asset_symbol,
        'crypto' as asset_type,
        source,
        symbol,
        base_currency,
        quote_currency,
        price,
        volume,
        observed_at,
        load_timestamp
    from {{ source('raw_pg', 'raw_cryptoprices_yfinance') }}
),

resolved as (

    select
        ha.asset_hk,
        s.asset_symbol,
        s.asset_type,
        s.symbol,
        s.base_currency,
        s.quote_currency,
        s.price,
        s.volume,
        s.asset_source as record_source,
        s.observed_at,
        cast(s.load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,

        {{ dbt_utils.generate_surrogate_key([
            'symbol',
            'base_currency',
            'quote_currency',
            'price',
            'volume'
        ]) }} as hashdiff

    from source_prices as s
    inner join {{ ref('hub_asset') }} as ha
        on
            s.asset_symbol = ha.asset_symbol
            and s.asset_type = ha.asset_type
            and s.asset_source = ha.record_source
)

select *
from resolved

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where
            t.asset_hk = resolved.asset_hk
            and t.hashdiff = resolved.hashdiff
    )
{% endif %}
