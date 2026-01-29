-- Test: Price comparison across sources should be within reasonable variance (20%)
-- This test ensures prices from different sources are reasonably consistent
with price_comparison as (
    select
        asset_hk,
        price_date,
        price_binance,
        price_coingecko,
        price_yfinance,
        coalesce(price_binance, price_coingecko, price_yfinance) as reference_price
    from {{ ref('fct_asset_price_comparison') }}
    where
        price_binance is not null
        or price_coingecko is not null
        or price_yfinance is not null
)

select
    asset_hk,
    price_date,
    price_binance,
    price_coingecko,
    price_yfinance,
    reference_price,
    abs(price_binance - reference_price) / nullif(reference_price, 0) * 100 as binance_variance_pct,
    abs(price_coingecko - reference_price) / nullif(reference_price, 0) * 100 as coingecko_variance_pct,
    abs(price_yfinance - reference_price) / nullif(reference_price, 0) * 100 as yfinance_variance_pct
from price_comparison
where
    (price_binance is not null and abs(price_binance - reference_price) / nullif(reference_price, 0) > 0.20)
    or (price_coingecko is not null and abs(price_coingecko - reference_price) / nullif(reference_price, 0) > 0.20)
    or (price_yfinance is not null and abs(price_yfinance - reference_price) / nullif(reference_price, 0) > 0.20)
