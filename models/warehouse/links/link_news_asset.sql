{{ config(
    materialized='incremental',
    unique_key='news_asset_hk',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
link_news_asset: Relationship between news articles and affected assets.
*/

with source_data as (

    select
        n.url,
        n.ticker as asset_symbol,
        n.asset_type,
        n.load_timestamp,
        n.source as record_source
    from {{ ref('ephemeral_news') }} as n
    where n.rn = 1
),

resolved_keys as (

    select
        hn.news_hk,
        ha.asset_hk,
        s.load_timestamp,
        s.record_source
    from source_data as s
    inner join {{ ref('hub_news') }} as hn
        on s.url = hn.url
    inner join {{ ref('hub_asset') }} as ha
        on
            s.asset_symbol = ha.asset_symbol
            and s.asset_type = ha.asset_type
),

dedup as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'news_hk',
            'asset_hk'
        ]) }} as news_asset_hk,

        news_hk,
        asset_hk,
        load_timestamp,
        record_source,

        row_number() over (
            partition by news_hk, asset_hk
            order by load_timestamp asc
        ) as rn

    from resolved_keys
)

select
    news_asset_hk,
    news_hk,
    asset_hk,
    load_timestamp,
    record_source

from dedup
where
    rn = 1

    {% if is_incremental() %}
        and news_asset_hk not in (
            select news_asset_hk from {{ this }}
        )
    {% endif %}
