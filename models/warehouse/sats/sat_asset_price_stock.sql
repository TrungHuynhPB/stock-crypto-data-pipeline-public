{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='asset_price_stock_hk',
    views_enabled=false,
    tags=['trino']
) }}

/*
sat_asset_price_stock
- Insert-only DV satellite
- Grain: asset_hk + observed_at + record_source + hashdiff
*/

with source_prices as (

    select
        'stock' as asset_type,
        source as asset_source,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close_price,
        volume,
        dividends,
        stock_splits,
        market_cap,
        pe_ratio,
        week_52_high,
        week_52_low,
        avg_volume,
        observed_at,
        load_timestamp,
        upper(ticker) as asset_symbol
    from {{ source('raw_pg', 'raw_stock_prices_yfinance') }}
),

resolved as (

    select
        ha.asset_hk,
        s.asset_symbol,
        s.asset_type,
        s.open_price,
        s.high_price,
        s.low_price,
        s.close_price,
        s.adj_close_price,
        s.volume,
        s.dividends,
        s.stock_splits,
        s.market_cap,
        s.pe_ratio,
        s.week_52_high,
        s.week_52_low,
        s.avg_volume,
        s.asset_source as record_source,
        s.observed_at,
        cast(s.load_timestamp as {{ dbt.type_timestamp() }}) as load_timestamp,

        {{ dbt_utils.generate_surrogate_key([
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'adj_close_price',
            'volume',
            'market_cap'
        ]) }} as hashdiff

    from source_prices as s
    inner join {{ ref('hub_asset') }} as ha
        on
            s.asset_symbol = ha.asset_symbol
            and s.asset_type = ha.asset_type
            and s.asset_source = ha.record_source
)

select *
from resolved

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where
            t.asset_hk = resolved.asset_hk
            and t.hashdiff = resolved.hashdiff
    )
{% endif %}
