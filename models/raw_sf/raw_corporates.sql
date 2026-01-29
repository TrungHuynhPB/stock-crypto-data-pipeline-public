{{ config(
    materialized='incremental',
    unique_key='company_id',
    views_enabled=false,
    incremental_strategy='append', tags=['trino','kafka']) }}
/*
raw_corporates: This raw table contains corporate data ingested from the source system.
*/
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
    load_timestamp,
    source
from {{ source('raw_pg', 'raw_corporates') }}

{% if is_incremental() %}
    where
        load_timestamp
        > (select coalesce(max(load_timestamp), timestamp '1900-01-01') from {{ this }})
{% endif %}
