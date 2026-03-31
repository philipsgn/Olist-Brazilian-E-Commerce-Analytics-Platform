-- dbt/models/marts/fact_payments.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with orders as (
    select *
    from {{ ref('stg_orders') }}
),
order_payments as (
    select *
    from {{ ref('stg_orders_payments') }}
)

select
    o.order_id,
    o.customer_id,
    date_trunc('day', o.purchased_at)::date as order_date_id,
    p.payment_total,
    p.payment_count,
    p.max_installments,
    p.payment_types
from orders o
join order_payments p on o.order_id = p.order_id

{% if is_incremental() %}
where date_trunc('day', o.purchased_at)::date >= (
    select coalesce(max(order_date_id), date '1900-01-01') from {{ this }}
)
{% endif %}
