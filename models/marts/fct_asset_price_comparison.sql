{{ config(materialized='view', tags=['mart','snowflake']) }}

/*
fct_asset_price_comparison
- Grain: asset_hk + price_date
- Purpose: compare prices across sources
*/

with daily_prices as (

    select
        asset_hk,
        cast(observed_at as date) as price_date,
        upper(price_source) as price_source,
        avg(price) as daily_price
    from {{ ref('fct_asset_prices') }}
    group by
        asset_hk,
        cast(observed_at as date),
        upper(price_source)
)

select
    asset_hk,
    price_date,

    max(case when price_source = 'BINANCE' then daily_price end) as price_binance,
    max(case when price_source = 'COINGECKO' then daily_price end) as price_coingecko,
    max(case when price_source = 'YFINANCE' then daily_price end) as price_yfinance

from daily_prices
group by asset_hk, price_date
