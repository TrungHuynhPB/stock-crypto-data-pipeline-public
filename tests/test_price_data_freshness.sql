-- Test: Price data should be recent (within last 7 days for active assets)
-- This test ensures we have recent price data for monitoring data pipeline health
-- Note: This test may fail if there's a legitimate gap in data, adjust threshold as needed
select
    asset_hk,
    asset_symbol,
    max(observed_at) as last_observed_at,
    datediff('day', max(observed_at), current_date()) as days_since_last_price
from {{ ref('fct_asset_prices') }}
group by asset_hk, asset_symbol
having datediff('day', max(observed_at), current_date()) > 7
