-- Test: Transaction timestamp date should match data_date
-- This test ensures temporal consistency between timestamp and date fields
select
    transaction_hk,
    transaction_id,
    transaction_timestamp,
    cast(transaction_timestamp as date) as timestamp_date,
    data_date
from {{ ref('fct_transactions') }}
where cast(transaction_timestamp as date) != cast(data_date as date)
