{{ config(
    materialized='incremental',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
sat_company_details
- Insert-only Data Vault satellite
- Grain: company_hk + load_timestamp
- Change detection via hashdiff
*/

with staged as (

    select
        company_id,
        company_name,
        company_type,
        company_email,
        country,
        year_founded,
        tax_number,
        office_primary_location,
        registration_date,
        source as record_source,
        load_timestamp
    from {{ source('raw_pg', 'raw_corporates') }}
),

resolved as (

    select
        hc.company_hk,
        s.company_id,
        s.company_name,
        s.company_type,
        s.company_email,
        s.country,
        s.year_founded,
        s.tax_number,
        s.office_primary_location,
        s.registration_date,
        s.record_source,
        cast(s.load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,

        {{ dbt_utils.generate_surrogate_key([
            'company_name',
            'company_type',
            'company_email',
            'country',
            'year_founded',
            'tax_number',
            'office_primary_location',
            'registration_date'
        ]) }} as hashdiff

    from staged as s
    inner join {{ ref('hub_company') }} as hc
        on s.company_id = hc.company_id
)

select *
from resolved

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where
            t.company_hk = resolved.company_hk
            and t.hashdiff = resolved.hashdiff
    )
{% endif %}
