-- Test: News URLs should have valid format
-- This test ensures news URLs follow basic URL format rules
select
    news_hk,
    url,
    title
from {{ ref('fct_news_events') }}
where
    url is not null
    and (
        url not like 'http%://%'
        or length(url) < 10
    )
