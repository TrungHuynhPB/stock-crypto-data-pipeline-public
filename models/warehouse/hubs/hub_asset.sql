{{ config(
    materialized='incremental',
    unique_key='asset_hk',
    incremental_strategy='append',
    views_enabled=false,
    tags=['trino']
) }}

/*
hub_asset: Hub table containing unique financial instruments.
*/

with ranked_assets as (

    select
        asset_symbol,
        asset_type,
        load_timestamp,
        record_source,

        row_number() over (
            partition by asset_symbol, asset_type
            order by load_timestamp asc
        ) as rn

    from {{ ref('ephemeral_asset') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['asset_symbol', 'asset_type']) }} as asset_hk,
    asset_symbol,
    asset_type,
    load_timestamp,
    record_source

from ranked_assets
where
    rn = 1

    {% if is_incremental() %}
        and {{ dbt_utils.generate_surrogate_key(['asset_symbol', 'asset_type']) }} not in (
            select asset_hk from {{ this }}
        )
    {% endif %}
