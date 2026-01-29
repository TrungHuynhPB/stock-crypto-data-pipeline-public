{{ config(
    materialized='incremental',
    unique_key='company_transaction_hk',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
link_company_transaction:
Relationship between companies and transactions
(derived from corporate transactions only).
*/

with source_transactions as (

    -- ONLY corporate transactions have company context
    select
        transaction_id,
        customer_id as company_id,
        load_timestamp,
        source as record_source
    from {{ source('raw_sf','raw_transaction_corporate') }}
),

resolved_keys as (

    select
        hc.company_hk,
        ht.transaction_hk,
        s.load_timestamp,
        s.record_source
    from source_transactions as s

    inner join {{ ref('hub_company') }} as hc
        on s.company_id = hc.company_id

    inner join {{ ref('hub_transaction') }} as ht
        on s.transaction_id = ht.transaction_id
),

dedup as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'company_hk',
            'transaction_hk'
        ]) }} as company_transaction_hk,

        company_hk,
        transaction_hk,
        load_timestamp,
        record_source,

        row_number() over (
            partition by company_hk, transaction_hk
            order by load_timestamp asc
        ) as rn

    from resolved_keys
)

select
    company_transaction_hk,
    company_hk,
    transaction_hk,
    load_timestamp,
    record_source
from dedup
where
    rn = 1

    {% if is_incremental() %}
        and company_transaction_hk not in (
            select company_transaction_hk from {{ this }}
        )
    {% endif %}
