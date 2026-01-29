-- Test: Risk tolerance should be a valid value
-- This test ensures risk tolerance values are within expected set
select
    customer_hk,
    customer_id,
    risk_tolerance
from {{ ref('dim_customer') }}
where
    risk_tolerance is not null
    and upper(trim(risk_tolerance)) not in ('LOW', 'MEDIUM', 'HIGH', 'CONSERVATIVE', 'MODERATE', 'AGGRESSIVE')
