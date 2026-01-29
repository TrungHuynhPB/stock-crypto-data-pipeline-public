-- Test: Trading volume should be non-negative
-- This test ensures volume values are not negative
select
    asset_hk,
    asset_symbol,
    observed_at,
    volume
from {{ ref('fct_asset_prices') }}
where
    volume is not null
    and volume < 0
