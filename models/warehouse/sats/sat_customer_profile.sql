{{ config(
    materialized='incremental',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
sat_customer_profile
- Insert-only Data Vault satellite
- Grain: customer_hk + load_timestamp
- Change detection via hashdiff
*/

with staged as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        country,
        customer_tier,
        risk_tolerance,
        registration_date,
        company_id,
        source as record_source,
        load_timestamp
    from {{ source('raw_pg', 'raw_customers') }}
),

resolved as (

    select
        hc.customer_hk,
        s.customer_id,
        s.first_name,
        s.last_name,
        s.email,
        s.country,
        s.customer_tier,
        s.risk_tolerance,
        s.registration_date,
        s.company_id,
        s.record_source,
        cast(s.load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,

        {{ dbt_utils.generate_surrogate_key([
            'first_name',
            'last_name',
            'email',
            'country',
            'customer_tier',
            'risk_tolerance',
            'registration_date',
            'company_id'
        ]) }} as hashdiff

    from staged as s
    inner join {{ ref('hub_customer') }} as hc
        on s.customer_id = hc.customer_id
)

select *
from resolved

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where
            t.customer_hk = resolved.customer_hk
            and t.hashdiff = resolved.hashdiff
    )
{% endif %}
