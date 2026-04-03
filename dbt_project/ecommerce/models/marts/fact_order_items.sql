-- dbt/models/marts/fact_order_items.sql

{{ config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['order_id'], 'type': 'btree'},
      {'columns': ['product_id'], 'type': 'btree'},
      {'columns': ['order_date_id'], 'type': 'btree'},
      {'columns': ['customer_id'], 'type': 'btree'}
    ],
    post_hook=[
      "analyze {{ this }}"
    ]
) }}

with order_items as (
    select 
        order_id, 
        order_item_id, 
        product_id, 
        seller_id, 
        price, 
        freight_value 
    from {{ ref('stg_order_items') }}
),
orders as (
    select 
        order_id, 
        customer_id, 
        purchased_at 
    from {{ ref('stg_orders') }}
)

select
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    date_trunc('day', o.purchased_at)::date as order_date_id,
    oi.price,
    oi.freight_value
from order_items oi
join orders o on oi.order_id = o.order_id

{% if is_incremental() %}
where date_trunc('day', o.purchased_at)::date >= (
    select coalesce(max(order_date_id), date '1900-01-01') from {{ this }}
)
{% endif %}
