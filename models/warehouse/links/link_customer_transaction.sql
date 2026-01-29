{{ config(
    materialized='incremental',
    unique_key='customer_transaction_hk',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
link_customer_transaction:
Relationship between customers and transactions
(derived from transaction events).
*/

with source_transactions as (

    select
        transaction_id,
        customer_id,
        load_timestamp,
        source as record_source
    from {{ ref('raw_transaction_personal') }}

    union all

    select
        transaction_id,
        customer_id,
        load_timestamp,
        source as record_source
    from {{ ref('raw_transaction_corporate') }}
),

resolved_keys as (

    select
        hc.customer_hk,
        ht.transaction_hk,
        s.load_timestamp,
        s.record_source
    from source_transactions as s

    inner join {{ ref('hub_customer') }} as hc
        on s.customer_id = hc.customer_id

    inner join {{ ref('hub_transaction') }} as ht
        on s.transaction_id = ht.transaction_id
),

dedup as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'customer_hk',
            'transaction_hk'
        ]) }} as customer_transaction_hk,

        customer_hk,
        transaction_hk,
        load_timestamp,
        record_source,

        row_number() over (
            partition by customer_hk, transaction_hk
            order by load_timestamp asc
        ) as rn

    from resolved_keys
)

select
    customer_transaction_hk,
    customer_hk,
    transaction_hk,
    load_timestamp,
    record_source
from dedup
where
    rn = 1

    {% if is_incremental() %}
        and customer_transaction_hk not in (
            select customer_transaction_hk from {{ this }}
        )
    {% endif %}
