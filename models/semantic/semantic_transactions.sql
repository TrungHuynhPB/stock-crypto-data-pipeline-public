{{ config(materialized='view', tags=['snowflake', 'semantic']) }}

/*
semantic_transactions
- Grain: transaction
- Purpose: analytics-ready transaction view
*/

select
    t.transaction_hk,
    t.transaction_id,
    t.transaction_timestamp,
    t.data_date,

    -- customer
    dc.customer_hk,
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.company_name,
    dc.email_addr,
    dc.country,
    dc.customer_tier,
    dc.risk_tolerance,

    -- asset
    da.asset_hk,
    da.asset_symbol,
    da.asset_type,

    -- measures
    t.transaction_type,
    t.quantity,
    t.price_per_unit,
    t.transaction_amount,
    t.fee_amount,
    t.record_source

from {{ ref('fct_transactions') }} as t

left join {{ ref('dim_customer') }} as dc
    on t.customer_hk = dc.customer_hk

left join {{ ref('dim_asset') }} as da
    on t.asset_hk = da.asset_hk
