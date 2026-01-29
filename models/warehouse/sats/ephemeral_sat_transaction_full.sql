{{ config(materialized='ephemeral',
    views_enabled=false, unique_key='transaction_hk') }}

select * from {{ ref('sat_transaction_corp') }}
union distinct
select * from {{ ref('sat_transaction_personal') }}
