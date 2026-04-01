-- mart_revenue_daily.sql
-- Daily revenue aggregated for Superset dashboard
-- Materialized as table for simplicity (no incremental complexity)

{{ config(
    materialized='table'
) }}

with fact as (
    select * from {{ ref('fact_order_items') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

joined as (
    select
        d.year,
        d.month,
        d.quarter,
        d.day,
        p.product_category,
        c.customer_state,
        f.price,
        f.freight_value,
        f.order_id,
        f.customer_id

    from fact f
    join dim_date     d on f.order_date_id  = d.date_id
    join dim_products p on f.product_id     = p.product_id
    join dim_customers c on f.customer_id   = c.customer_id
)

select
    year,
    month,
    quarter,
    day,
    product_category,
    customer_state,

    -- Revenue metrics
    round(sum(price)::numeric, 2)          as gross_revenue,
    round(sum(freight_value)::numeric, 2)  as freight_cost,
    round(sum(price + freight_value)::numeric, 2) as total_revenue,

    -- Order metrics
    count(distinct order_id)               as total_orders,
    count(distinct customer_id)            as unique_customers,

    -- Average metrics
    round(avg(price)::numeric, 2)          as avg_order_value

from joined
group by 1, 2, 3, 4, 5, 6
order by year, month, product_category
