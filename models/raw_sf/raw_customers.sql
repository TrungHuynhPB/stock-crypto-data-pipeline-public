{{ config(
    materialized='incremental',
    unique_key=['customer_id','load_timestamp'],
    views_enabled=false,
    incremental_strategy='append', tags=['trino','kafka']) }}
/*
raw_customers: This raw table contains customer data ingested from the source system.
*/
select
    customer_id,
    first_name,
    last_name,
    email,
    gender,
    age_group,
    country,
    registration_date,
    customer_tier,
    risk_tolerance,
    customer_type,
    company_id,
    load_timestamp,
    source
from {{ source('raw_pg', 'raw_customers') }}

{% if is_incremental() %}
    where
        load_timestamp
        > (select coalesce(max(load_timestamp), timestamp '1900-01-01') from {{ this }})
{% endif %}
