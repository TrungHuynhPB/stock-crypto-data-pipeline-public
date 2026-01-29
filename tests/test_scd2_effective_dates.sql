-- Test: SCD Type 2 effective dates should be valid (effective_from <= effective_to)
-- This test ensures historical dimension records have valid date ranges
select
    customer_hk,
    customer_id,
    effective_from,
    effective_to
from {{ ref('dim_customer_history') }}
where
    effective_to is not null
    and effective_from > effective_to
