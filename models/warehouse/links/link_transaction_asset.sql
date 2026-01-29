{{ config(
    materialized='incremental',
    unique_key='transaction_asset_hk',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino','kafka']
) }}

/*
link_transaction_asset:
Relationship between transactions and traded assets
(derived from transaction events).
*/

with source_transactions as (

    select
        transaction_id,
        asset_symbol,
        asset_type,
        data_source as asset_source,
        load_timestamp,
        source as record_source
    from {{ ref('raw_transaction_personal') }}

    union all

    select
        transaction_id,
        asset_symbol,
        asset_type,
        data_source as asset_source,
        load_timestamp,
        source as record_source
    from {{ ref('raw_transaction_corporate') }}
),

resolved_keys as (

    select
        ht.transaction_hk,
        ha.asset_hk,
        s.load_timestamp,
        s.record_source
    from source_transactions as s

    inner join {{ ref('hub_transaction') }} as ht
        on s.transaction_id = ht.transaction_id

    inner join {{ ref('hub_asset') }} as ha
        on
            s.asset_symbol = ha.asset_symbol
            and upper(s.asset_type) = upper(ha.asset_type)
),

dedup as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'transaction_hk',
            'asset_hk'
        ]) }} as transaction_asset_hk,

        transaction_hk,
        asset_hk,
        load_timestamp,
        record_source,

        row_number() over (
            partition by transaction_hk, asset_hk
            order by load_timestamp asc
        ) as rn

    from resolved_keys
)

select
    transaction_asset_hk,
    transaction_hk,
    asset_hk,
    load_timestamp,
    record_source
from dedup
where
    rn = 1

    {% if is_incremental() %}
        and transaction_asset_hk not in (
            select transaction_asset_hk from {{ this }}
        )
    {% endif %}
