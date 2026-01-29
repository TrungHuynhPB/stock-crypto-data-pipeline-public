{{ config(
    materialized = 'view',
    tags = ['mart','snowflake']
) }}

/*
fct_asset_prices
- Grain: asset_hk + observed_at + price_source
- Unified crypto + stock prices
*/

with unified_prices as (

    -- Crypto prices
    select
        asset_hk,
        observed_at,
        price,
        volume,
        upper(record_source) as price_source,
        'crypto' as asset_class
    from {{ ref('sat_asset_price_crypto') }}

    union all

    -- Stock prices (use close_price)
    select
        asset_hk,
        observed_at,
        close_price as price,
        volume,
        upper(record_source) as price_source,
        'stock' as asset_class
    from {{ ref('sat_asset_price_stock') }}
)

select
    p.asset_hk,
    h.asset_symbol,
    h.asset_type,

    p.observed_at,
    cast(p.observed_at as date) as price_date,

    p.price,
    p.volume,
    p.price_source,
    p.asset_class

from unified_prices as p
inner join {{ ref('hub_asset') }} as h
    on p.asset_hk = h.asset_hk
