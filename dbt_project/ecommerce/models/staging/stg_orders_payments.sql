-- dbt/models/staging/stg_orders_payments.sql

with payments as (
    select *
    from {{ ref('stg_payments') }}
),
aggregated as (
    select
        order_id,
        sum(payment_value) as payment_total,
        max(payment_installments) as max_installments,
        count(*) as payment_count,
        string_agg(distinct payment_type, ',' order by payment_type) as payment_types
    from payments
    where order_id is not null
    group by order_id
)

select *
from aggregated
