--/* hub_company: This hub table contains unique companies derived from the raw corporate data source. */
{{ config(
    materialized='incremental',
    unique_key='company_hk',
    incremental_strategy='append',
    tags=['trino'],
    views_enabled=false
) }}

with dedup as (

    select
        company_id,
        load_timestamp,
        source as record_source,

        row_number() over (
            partition by company_id
            order by load_timestamp asc
        ) as rn

    from {{ source('raw_pg', 'raw_corporates') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['company_id']) }} as company_hk,
    company_id,
    load_timestamp,
    record_source
from dedup
where
    rn = 1

    {% if is_incremental() %}
        and {{ dbt_utils.generate_surrogate_key(['company_id']) }} not in (select company_hk from {{ this }})
    {% endif %}
