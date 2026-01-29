-- Test: 52-week high should be >= 52-week low
-- This test ensures 52-week price ranges are logically consistent
select
    asset_hk,
    asset_symbol,
    week_52_high,
    week_52_low
from {{ ref('dim_asset') }}
where
    week_52_high is not null
    and week_52_low is not null
    and week_52_high < week_52_low
