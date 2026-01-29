-- Test: Year founded should be reasonable (not in future, not before 1000)
-- This test ensures company founding years are within logical bounds
select
    company_hk,
    company_id,
    company_name,
    year_founded
from {{ ref('dim_company') }}
where
    year_founded is not null
    and (
        year_founded > year(current_date())
        or year_founded < 1000
    )
