-- Test: Asset type should be valid (Stock or Crypto)
-- This test ensures asset types are within expected values
select
    asset_hk,
    asset_symbol,
    asset_type
from {{ ref('dim_asset') }}
where upper(trim(asset_type)) not in ('STOCK', 'CRYPTO', 'CRYPTOCURRENCY')
