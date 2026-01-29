{{ config(
    materialized='incremental',
    unique_key=['ticker','title'],
    views_enabled=false,
    incremental_strategy='append', tags=['trino']) }}
/*
raw_news: This raw table contains news data ingested from the source system.
*/
select *
from {{ source('raw_pg', 'raw_news') }}

{% if is_incremental() %}
    where
        load_timestamp
        > (select coalesce(max(load_timestamp), timestamp '1900-01-01') from {{ this }})
{% endif %}
