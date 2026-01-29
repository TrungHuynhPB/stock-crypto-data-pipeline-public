-- Test: Email addresses should have valid format
-- This test ensures email addresses follow basic email format rules
select
    customer_hk,
    customer_id,
    email_addr
from {{ ref('dim_customer') }}
where
    email_addr is not null
    and (
        email_addr not like '%@%.%'
        or email_addr like '@%'
        or email_addr like '%@'
        or length(email_addr) < 5
        or email_addr not like '%_@_%._%'
    )
