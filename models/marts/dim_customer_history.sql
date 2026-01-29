{{ config(materialized='view', tags=['mart','snowflake']) }}

/*
dim_customer_history
- Type 2 SCD
- Grain: customer_hk + effective_from
*/

with customer_versions as (

    select
        -- Keys
        h.customer_hk,
        h.customer_id,

        -- Attributes
        s.first_name,
        s.last_name,
        s.email,
        s.country,
        s.customer_tier,
        s.risk_tolerance,

        -- SCD metadata
        s.load_timestamp as effective_from,
        s.record_source,

        lead(s.load_timestamp) over (
            partition by h.customer_hk
            order by s.load_timestamp
        ) as next_effective_from

    from {{ ref('hub_customer') }} as h
    inner join {{ ref('sat_customer_profile') }} as s
        on h.customer_hk = s.customer_hk
),

company_context as (

    select
        l.customer_hk,
        cd.company_hk,
        cd.company_id,
        cd.company_name,
        cd.company_email
    from {{ ref('link_customer_company') }} as l
    inner join {{ ref('sat_company_details') }} as cd
        on l.company_hk = cd.company_hk
)

select
    cv.customer_hk,
    cc.company_hk,
    cv.customer_id,
    cc.company_id,

    cv.first_name,
    cv.last_name,
    cc.company_name,

    cv.country,
    cv.customer_tier,
    cv.risk_tolerance,
    cv.effective_from,

    cv.record_source,

    coalesce(cv.email, cc.company_email) as email_addr,

    coalesce(
        cv.next_effective_from,
        timestamp '9999-12-31'
    ) as effective_to

from customer_versions as cv
left join company_context as cc
    on cv.customer_hk = cc.customer_hk
