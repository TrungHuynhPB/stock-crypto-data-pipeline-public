{{ config(
    materialized = 'view',
    tags = ['snowflake', 'mart']
) }}

/*
dim_company
- Current snapshot of company attributes
- Source: sat_company_details (insert-only satellite)
- Grain: company_hk
*/

with ranked as (

    select
        company_hk,
        company_id,
        company_name,
        company_type,
        company_email,
        country,
        year_founded,
        tax_number,
        office_primary_location,
        registration_date,
        load_timestamp,
        record_source,

        row_number() over (
            partition by company_hk
            order by load_timestamp desc
        ) as rn

    from {{ ref('sat_company_details') }}
)

select
    company_hk,
    company_id,
    company_name,
    company_type,
    company_email,
    country,
    year_founded,
    tax_number,
    office_primary_location,
    registration_date,
    load_timestamp,
    record_source
from ranked
where rn = 1
