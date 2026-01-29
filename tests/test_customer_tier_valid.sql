-- Test: Customer tier should be a valid value
-- This test ensures customer tier values are within expected set
select
    customer_hk,
    customer_id,
    customer_tier
from {{ ref('dim_customer') }}
where
    customer_tier is not null
    and upper(trim(customer_tier)) not in ('SILVER', 'GOLD', 'PLATINUM', 'BRONZE', 'BASIC')
