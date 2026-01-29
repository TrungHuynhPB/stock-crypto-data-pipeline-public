{{ config(
    materialized='incremental',
    unique_key='customer_id',
    views_enabled=false,
    incremental_strategy='append', tags=['trino']) }}
/*
hub_customer: This hub table contains unique customers derived from the raw customer data source.
*/
with dedup as (

    select
        customer_id,
        load_timestamp,
        source as record_source,

        row_number() over (
            partition by customer_id
            order by load_timestamp asc
        ) as rn

    from {{ source('raw_pg', 'raw_customers') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_hk,
    customer_id,
    load_timestamp,
    record_source
from dedup
where
    rn = 1

    {% if is_incremental() %}
        and {{ dbt_utils.generate_surrogate_key(['customer_id']) }} not in (select customer_hk from {{ this }})
    {% endif %}
