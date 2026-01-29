{{ config(
    materialized='incremental',
    unique_key='transaction_hk',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
hub_transaction: Hub table containing unique transaction business keys.
*/

with unioned as (

    select
        transaction_id,
        load_timestamp,
        source as record_source
    from {{ source('raw_pg', 'raw_transaction_personal') }}

    union all

    select
        transaction_id,
        load_timestamp,
        source as record_source
    from {{ source('raw_pg', 'raw_transaction_corporate') }}

),

dedup as (

    select
        transaction_id,
        load_timestamp,
        record_source,

        row_number() over (
            partition by transaction_id
            order by load_timestamp asc
        ) as rn

    from unioned
)

select
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as transaction_hk,
    transaction_id,
    load_timestamp,
    record_source

from dedup
where
    rn = 1

    {% if is_incremental() %}
        and {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} not in (
            select transaction_hk from {{ this }}
        )
    {% endif %}
