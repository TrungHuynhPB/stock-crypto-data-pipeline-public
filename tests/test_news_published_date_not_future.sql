-- Test: News published date should not be in the future
-- This test ensures no news articles are dated in the future
select
    news_hk,
    asset_symbol,
    published_date,
    title
from {{ ref('fct_news_events') }}
where published_date > current_date()
