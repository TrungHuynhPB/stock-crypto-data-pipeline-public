{{ config(
    materialized='incremental',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
sat_news_content
- Insert-only Data Vault satellite
- Grain: news_hk + load_timestamp
- Change detection via hashdiff
*/

with staged as (

    select
        url,
        date as published_date,
        title,
        description,
        image,
        source as record_source,
        load_timestamp
    from {{ ref('ephemeral_news') }}
),

resolved as (

    select
        hn.news_hk,
        s.url,
        s.published_date,
        s.title,
        s.description,
        s.image,
        s.record_source,
        cast(s.load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,

        {{ dbt_utils.generate_surrogate_key([
            'published_date',
            'title',
            'description',
            'image'
        ]) }} as hashdiff

    from staged as s
    inner join {{ ref('hub_news') }} as hn
        on s.url = hn.url
)

select *
from resolved

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where
            t.news_hk = resolved.news_hk
            and t.hashdiff = resolved.hashdiff
    )
{% endif %}
