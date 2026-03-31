-- dbt/models/marts/mart_revenue_daily.sql

{{ config(
    materialized='incremental',
    unique_key=['year', 'month', 'quarter', 'product_category', 'customer_state'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with base as (
    select
        d.year,
        d.month,
        d.quarter,
        p.product_category,
        c.customer_state,
        f.price,
        f.freight_value,
        f.order_id,
        f.customer_id,
        f.order_date_id
    from {{ ref('fact_order_items') }} f
    join {{ ref('dim_date') }} d on f.order_date_id = d.date_id
    join {{ ref('dim_products') }} p on f.product_id = p.product_id
    join {{ ref('dim_customers') }} c on f.customer_id = c.customer_id
)

{% if is_incremental() %}
, max_existing as (
    select coalesce(max(order_date_id), date '1900-01-01') as max_date
    from {{ this }}
)
{% endif %}

select
    year,
    month,
    quarter,
    product_category,
    customer_state,
    sum(price) as gross_revenue,
    sum(freight_value) as freight_cost,
    count(distinct order_id) as total_orders,
    count(distinct customer_id) as unique_customers,
    max(order_date_id) as order_date_id
from base
{% if is_incremental() %}
where base.order_date_id >= (select max_date from max_existing)
{% endif %}
group by 1, 2, 3, 4, 5
