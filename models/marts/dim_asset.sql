{{ config(
    materialized = 'view',
    tags = ['mart','snowflake']
) }}

/*
dim_asset
- Asset dimension - test CIIII
- One row per asset_hk + record_source
- Latest known descriptive attributes
*/

with crypto_latest as (

    select
        asset_hk,
        asset_symbol,
        asset_type,
        record_source,

        -- crypto-specific
        base_currency,
        quote_currency,
        price,
        volume,

        -- stock placeholders
        cast(null as number) as market_cap,
        cast(null as number) as pe_ratio,
        cast(null as number) as week_52_high,
        cast(null as number) as week_52_low,

        observed_at,
        load_timestamp,

        row_number() over (
            partition by asset_hk, record_source
            order by observed_at desc, load_timestamp desc
        ) as rn
    from {{ ref('sat_asset_price_crypto') }}
),

stock_latest as (

    select
        asset_hk,
        asset_symbol,
        asset_type,
        record_source,

        -- crypto placeholders
        cast(null as varchar) as base_currency,
        cast(null as varchar) as quote_currency,
        close_price as price,
        cast(null as number) as volume,

        -- stock-specific
        market_cap,
        pe_ratio,
        week_52_high,
        week_52_low,

        observed_at,
        load_timestamp,

        row_number() over (
            partition by asset_hk, record_source
            order by observed_at desc, load_timestamp desc
        ) as rn
    from {{ ref('sat_asset_price_stock') }}
),

latest_assets as (

    select * from crypto_latest
    where rn = 1
    union all
    select * from stock_latest
    where rn = 1
)

select
    h.asset_hk,
    h.asset_symbol,
    h.asset_type,
    la.record_source,

    -- crypto attributes
    la.base_currency,
    la.quote_currency,
    la.price,
    la.volume,

    -- stock attributes
    la.market_cap,
    la.pe_ratio,
    la.week_52_high,
    la.week_52_low,

    la.observed_at as last_observed_at,
    h.load_timestamp as asset_created_at

from {{ ref('hub_asset') }} as h
left join latest_assets as la
    on h.asset_hk = la.asset_hk
