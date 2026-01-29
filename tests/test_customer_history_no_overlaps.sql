-- Test: Customer history records should not have overlapping effective date ranges
-- This test ensures SCD Type 2 records don't have temporal overlaps for the same customer
with customer_history as (
    select
        customer_hk,
        customer_id,
        effective_from,
        effective_to,
        lead(effective_from) over (
            partition by customer_hk
            order by effective_from
        ) as next_effective_from
    from {{ ref('dim_customer_history') }}
)

select
    customer_hk,
    customer_id,
    effective_from,
    effective_to,
    next_effective_from
from customer_history
where
    effective_to is not null
    and next_effective_from is not null
    and effective_to > next_effective_from
