{{ config(materialized='ephemeral',
    views_enabled=false, unique_key='news_hk') }}
/*
ephemeral_news: This ephemeral removes duplicates from raw news data source (each ticker should only have 1 unique news article URL).
*/
with deduped as (
    select
        ticker,
        asset_type,
        url,
        date,
        load_timestamp,
        source,
        title,
        description,
        image,
        row_number() over (
            partition by url, ticker, asset_type
            order by load_timestamp asc, source asc
        ) as rn
    from {{ source('raw_pg', 'raw_news') }}
)

select * from deduped
where rn = 1

{% if is_incremental() %}
    where
        load_timestamp
        > (select max(load_timestamp) from {{ this }})
{% endif %}
