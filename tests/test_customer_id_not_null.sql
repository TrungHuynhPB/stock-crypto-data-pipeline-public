-- Test: Customer ID should not be null
-- This test ensures all customer records have a valid customer_id
select *
from {{ ref('dim_customer') }}
where customer_id is null
