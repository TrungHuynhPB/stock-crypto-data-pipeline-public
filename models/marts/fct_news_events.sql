{{ config(materialized='view', tags=['mart','snowflake']) }}

/*
fct_news_events
- Grain: news_hk + asset_hk
- One row per news-asset relationship
*/

select
    n.news_hk,
    a.asset_hk,
    a.asset_symbol,
    a.asset_type,

    cast(c.published_date as date) as published_date,

    c.title,
    c.description,
    c.url,
    c.record_source as news_source

from {{ ref('link_news_asset') }} as l
inner join {{ ref('hub_news') }} as n
    on l.news_hk = n.news_hk
inner join {{ ref('hub_asset') }} as a
    on l.asset_hk = a.asset_hk
inner join {{ ref('sat_news_content') }} as c
    on n.news_hk = c.news_hk
