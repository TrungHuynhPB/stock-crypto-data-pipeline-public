-- Test: News impact returns should match calculated values (return = (price_tN - price_t0) / price_t0)
-- This test ensures calculated return values are accurate
select
    news_hk,
    asset_hk,
    news_date,
    price_t0,
    price_t1,
    return_1d,
    round((price_t1 - price_t0) / nullif(price_t0, 0), 6) as calculated_return_1d,
    abs(return_1d - round((price_t1 - price_t0) / nullif(price_t0, 0), 6)) as difference_1d
from {{ ref('fct_asset_news_impact') }}
where
    price_t0 is not null
    and price_t1 is not null
    and return_1d is not null
    and abs(return_1d - round((price_t1 - price_t0) / nullif(price_t0, 0), 6)) > 0.0001  -- Small tolerance for rounding
