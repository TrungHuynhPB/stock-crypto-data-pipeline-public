{{ config(
    materialized='incremental',
    unique_key='customer_company_hk',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
link_customer_company:
Relationship between corporate customers and companies
(derived from customer master data).
*/

with source_data as (

    select
        customer_id,
        company_id,
        load_timestamp,
        source as record_source
    from {{ source('raw_pg', 'raw_customers') }}
    where company_id is not null
),

resolved_keys as (

    select
        hc.customer_hk,
        hco.company_hk,
        s.load_timestamp,
        s.record_source
    from source_data as s

    inner join {{ ref('hub_customer') }} as hc
        on s.customer_id = hc.customer_id

    inner join {{ ref('hub_company') }} as hco
        on s.company_id = hco.company_id
),

dedup as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'customer_hk',
            'company_hk'
        ]) }} as customer_company_hk,

        customer_hk,
        company_hk,
        load_timestamp,
        record_source,

        row_number() over (
            partition by customer_hk, company_hk
            order by load_timestamp asc
        ) as rn

    from resolved_keys
)

select
    customer_company_hk,
    customer_hk,
    company_hk,
    load_timestamp,
    record_source
from dedup
where
    rn = 1

    {% if is_incremental() %}
        and customer_company_hk not in (
            select customer_company_hk from {{ this }}
        )
    {% endif %}
