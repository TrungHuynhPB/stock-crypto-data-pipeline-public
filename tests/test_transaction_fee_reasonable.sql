-- Test: Fee amount should be reasonable (not negative, not greater than transaction amount)
-- This test ensures fee amounts are within business logic constraints
select
    transaction_hk,
    transaction_id,
    transaction_amount,
    fee_amount,
    round(fee_amount / nullif(transaction_amount, 0) * 100, 2) as fee_percentage
from {{ ref('fct_transactions') }}
where
    fee_amount < 0
    or (fee_amount is not null and fee_amount > transaction_amount)
    or (fee_amount is not null and fee_amount / nullif(transaction_amount, 0) > 0.10)  -- Fee should not exceed 10%
