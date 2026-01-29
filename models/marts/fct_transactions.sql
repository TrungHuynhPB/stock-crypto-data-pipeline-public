{{ config(materialized='view', tags=['mart', 'snowflake']) }}

/*
fct_transactions
- Grain: transaction_hk
- Measures come from transaction satellites
- Dimensions resolved via keys (not attributes)
*/

select
    -- Business & surrogate keys
    t.transaction_hk,
    t.transaction_id,
    c.customer_id,
    lct.customer_hk,
    lta.asset_hk,

    -- Transaction attributes (facts)
    r.transaction_type,
    r.quantity,
    r.price_per_unit,
    r.transaction_amount,
    r.fee_amount,

    cast(r.transaction_timestamp as timestamp_ntz) as transaction_timestamp,
    r.data_date,

    -- Metadata
    r.data_source as record_source,
    t.load_timestamp

from {{ ref('hub_transaction') }} as t

inner join {{ ref('link_customer_transaction') }} as lct
    on t.transaction_hk = lct.transaction_hk
inner join {{ ref('hub_customer') }} as c
    on lct.customer_hk = c.customer_hk
inner join {{ ref('link_transaction_asset') }} as lta
    on t.transaction_hk = lta.transaction_hk

inner join {{ ref('ephemeral_sat_transaction_full') }} as r
    on t.transaction_hk = r.transaction_hk
