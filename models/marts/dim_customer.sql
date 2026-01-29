{{ config(materialized='view', tags=['mart','snowflake']) }}

/*
dim_customer
- Current customer dimension snapshot
*/

select
    customer_hk,
    customer_id,
    company_hk,
    company_id,

    first_name,
    last_name,
    company_name,
    email_addr,
    country,
    customer_tier,
    risk_tolerance,

    effective_from,
    record_source

from {{ ref('dim_customer_history') }}
where effective_to = timestamp '9999-12-31'
