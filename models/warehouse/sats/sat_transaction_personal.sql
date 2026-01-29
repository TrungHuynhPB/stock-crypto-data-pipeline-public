{{ config(
    materialized='incremental',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
sat_transaction_personal
- Insert-only Data Vault satellite
- Grain: transaction_hk + hashdiff
- Personal-customer transaction attributes
*/

with staged as (

    select
        transaction_id,
        customer_id,
        asset_type,
        asset_symbol,
        transaction_type,
        quantity,
        price_per_unit,
        transaction_amount,
        fee_amount,
        transaction_timestamp,
        data_date,
        customer_tier,
        customer_risk_tolerance,
        customer_type,
        data_source,
        source as record_source,
        load_timestamp
    from {{ source('raw_pg', 'raw_transaction_personal') }}
),

resolved as (

    select
        ht.transaction_hk,
        s.transaction_id,
        s.customer_id,
        s.asset_type,
        s.asset_symbol,
        s.transaction_type,
        s.quantity,
        s.price_per_unit,
        s.transaction_amount,
        s.fee_amount,
        s.transaction_timestamp,
        s.data_date,
        s.customer_tier,
        s.customer_risk_tolerance,
        s.customer_type,
        s.data_source,
        s.record_source,
        cast(s.load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,

        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'asset_type',
            'asset_symbol',
            'transaction_type',
            'quantity',
            'price_per_unit',
            'transaction_amount',
            'fee_amount',
            'transaction_timestamp',
            'data_date',
            'customer_tier',
            'customer_risk_tolerance',
            'customer_type',
            'data_source'
        ]) }} as hashdiff

    from staged as s
    inner join {{ ref('hub_transaction') }} as ht
        on s.transaction_id = ht.transaction_id
)

select *
from resolved

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where
            t.transaction_hk = resolved.transaction_hk
            and t.hashdiff = resolved.hashdiff
    )
{% endif %}
