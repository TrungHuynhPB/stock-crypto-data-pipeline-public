{{ config(materialized='view', tags=['snowflake', 'semantic']) }}

/*
semantic_customer_overview
- Grain: customer_hk
- Purpose: customer profile + transaction KPIs
*/

with txn_stats as (

    select
        customer_hk,
        count(*) as total_transactions,
        sum(transaction_amount) as total_transaction_amount,
        sum(fee_amount) as total_fee_amount,
        max(transaction_timestamp) as last_transaction_at
    from {{ ref('fct_transactions') }}
    group by customer_hk
)

select
    dc.customer_hk,
    dc.customer_id,

    -- identity
    dc.email_addr,

    dc.country,
    dc.customer_tier,
    dc.risk_tolerance,
    ts.last_transaction_at,

    -- activity
    coalesce(
        trim(dc.first_name || ' ' || dc.last_name),
        dc.company_name
    ) as customer_name,

    -- measures
    coalesce(ts.total_transactions, 0) as total_transactions,
    coalesce(ts.total_transaction_amount, 0) as total_transaction_amount,
    coalesce(ts.total_fee_amount, 0) as total_fee_amount

from {{ ref('dim_customer') }} as dc
left join txn_stats as ts
    on dc.customer_hk = ts.customer_hk
