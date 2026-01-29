{{ config(materialized='view', tags=['mart','snowflake']) }}

/*
fct_asset_news_impact
- Grain: news_hk + asset_hk + news_date
- Measures: post-news price impact
*/

with news_assets as (

    select
        l.news_hk,
        l.asset_hk,
        cast(s.published_date as date) as news_date
    from {{ ref('link_news_asset') }} as l
    inner join {{ ref('sat_news_content') }} as s
        on l.news_hk = s.news_hk
),

daily_asset_prices as (

    -- crypto prices
    select
        asset_hk,
        cast(observed_at as date) as price_date,
        avg(price) as daily_price
    from {{ ref('sat_asset_price_crypto') }}
    group by asset_hk, cast(observed_at as date)

    union all

    -- stock prices (use close_price)
    select
        asset_hk,
        cast(observed_at as date) as price_date,
        avg(close_price) as daily_price
    from {{ ref('sat_asset_price_stock') }}
    group by asset_hk, cast(observed_at as date)
),

price_windows as (

    select
        n.news_hk,
        n.asset_hk,
        n.news_date,

        p0.daily_price as price_t0,
        p1.daily_price as price_t1,
        p3.daily_price as price_t3,
        p7.daily_price as price_t7

    from news_assets as n

    left join daily_asset_prices as p0
        on
            n.asset_hk = p0.asset_hk
            and n.news_date = p0.price_date

    left join daily_asset_prices as p1
        on
            n.asset_hk = p1.asset_hk
            and p1.price_date = dateadd(day, 1, n.news_date)

    left join daily_asset_prices as p3
        on
            n.asset_hk = p3.asset_hk
            and p3.price_date = dateadd(day, 3, n.news_date)

    left join daily_asset_prices as p7
        on
            n.asset_hk = p7.asset_hk
            and p7.price_date = dateadd(day, 7, n.news_date)
)

select
    news_hk,
    asset_hk,
    news_date,

    price_t0,
    price_t1,
    price_t3,
    price_t7,

    (price_t1 - price_t0) / nullif(price_t0, 0) as return_1d,
    (price_t3 - price_t0) / nullif(price_t0, 0) as return_3d,
    (price_t7 - price_t0) / nullif(price_t0, 0) as return_7d

from price_windows
where price_t0 is not null
