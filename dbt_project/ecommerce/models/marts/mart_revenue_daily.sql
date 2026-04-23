-- mart_revenue_daily.sql
-- Daily revenue aggregated for Superset dashboard
-- Materialized as table for simplicity (no incremental complexity)

{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date_day'], 'type': 'btree'},
      {'columns': ['product_category'], 'type': 'btree'},
      {'columns': ['customer_state'], 'type': 'btree'}
    ],
    post_hook=[
      "analyze {{ this }}"
    ]
) }}

-- Pre-aggregate at grain (date, product, customer) BEFORE joining heavy dims
with aggregated_fact as (
    select
        order_date_id,
        product_id,
        customer_id,
        count(distinct order_id) as total_orders,
        sum(price) as gross_revenue,
        sum(freight_value) as freight_cost,
        sum(price + freight_value) as total_revenue
    from {{ ref('fact_order_items') }}
    group by order_date_id, product_id, customer_id
),

fact_collapsed as (
    select
        order_date_id,
        product_id,
        customer_id,
        sum(gross_revenue) as gross_revenue,
        sum(freight_cost) as freight_cost,
        sum(total_revenue) as total_revenue,
        sum(total_orders) as total_orders
    from aggregated_fact
    group by order_date_id, product_id, customer_id
),

dim_date as (
    select
        date_id,
        year_number,
        month_number,
        quarter_number,
        day_number
    from {{ ref('dim_date') }}
),

dim_products as (
    select product_id, product_category from {{ ref('dim_products') }}
),

dim_customers as (
    select customer_id, customer_state from {{ ref('dim_customers') }}
),

joined as (
    select
        d.date_id as date_day,
        d.year_number,
        d.month_number,
        d.quarter_number,
        d.day_number,
        p.product_category,
        c.customer_state,
        f.gross_revenue,
        f.freight_cost,
        f.total_revenue,
        f.total_orders,
        f.customer_id
    from fact_collapsed as f
    inner join dim_date as d on f.order_date_id = d.date_id
    inner join dim_products as p on f.product_id = p.product_id
    inner join dim_customers as c on f.customer_id = c.customer_id
),

final as (
    select
        date_day,
        year_number,
        month_number,
        quarter_number,
        day_number,
        product_category,
        customer_state,
        -- Final Aggregation
        round(sum(gross_revenue)::numeric, 2) as gross_revenue,
        round(sum(freight_cost)::numeric, 2) as freight_cost,
        round(sum(total_revenue)::numeric, 2) as total_revenue,
        sum(total_orders) as total_orders,
        count(distinct customer_id) as unique_customers,
        round(avg(gross_revenue)::numeric, 2) as avg_order_value
    from joined
    group by
        date_day,
        year_number,
        month_number,
        quarter_number,
        day_number,
        product_category,
        customer_state
    order by date_day, product_category
)

select * from final
