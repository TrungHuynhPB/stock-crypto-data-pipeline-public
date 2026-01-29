{{ config(materialized='view', tags=['snowflake', 'semantic']) }}

/*
semantic_asset_performance
- Grain: asset_symbol + asset_type + price_date + price_source
- Purpose: analytical view for asset performance over time
*/

select
    ap.asset_symbol,
    ap.asset_type,
    ap.asset_class,

    cast(ap.observed_at as date) as price_date,
    ap.price_source,

    -- measures
    ap.price,
    ap.volume

from {{ ref('fct_asset_prices') }} as ap
