-- Test: Transaction type should be valid (Buy or Sell)
-- This test ensures transaction_type contains only expected values
select
    transaction_hk,
    transaction_id,
    transaction_type
from {{ ref('fct_transactions') }}
where upper(trim(transaction_type)) not in ('BUY', 'SELL')
