-- Test: Stock prices should have valid ranges (high >= low, close between low and high)
-- This test ensures stock price data follows logical constraints
select
    asset_hk,
    asset_symbol,
    observed_at,
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close_price
from {{ ref('sat_asset_price_stock') }}
where
    high_price < low_price
    or close_price < low_price
    or close_price > high_price
    or open_price < low_price
    or open_price > high_price
    or (adj_close_price is not null and adj_close_price < 0)
