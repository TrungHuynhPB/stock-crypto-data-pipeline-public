{{ config(materialized='incremental',
    views_enabled=false,
    incremental_strategy='append',
 unique_key='news_hk', tags=['trino']) }}
/*
hub_news: This hub table contains unique news articles derived from the raw news data source.
*/
with dedup as (

    select
        url,
        load_timestamp,
        source as record_source,

        row_number() over (
            partition by url
            order by load_timestamp asc
        ) as rn

    from {{ ref('ephemeral_news') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['url']) }} as news_hk,
    url,
    load_timestamp,
    record_source

from dedup
where
    rn = 1

    {% if is_incremental() %}
        and {{ dbt_utils.generate_surrogate_key(['url']) }} not in (
            select news_hk from {{ this }}
        )
    {% endif %}
