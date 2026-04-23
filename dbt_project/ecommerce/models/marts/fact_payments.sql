-- dbt/models/marts/fact_payments.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['order_id'], 'type': 'btree'},
      {'columns': ['order_date_id'], 'type': 'btree'},
      {'columns': ['customer_id'], 'type': 'btree'}
    ],
    post_hook=[
      "analyze {{ this }}"
    ]
) }}

with orders as (
    select
        order_id,
        customer_id,
        purchased_at
    from {{ ref('stg_orders') }}
),

order_payments as (
    select
        order_id,
        payment_total,
        payment_count,
        max_installments,
        payment_types
    from {{ ref('stg_orders_payments') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        date_trunc('day', o.purchased_at)::date as order_date_id,
        p.payment_total,
        p.payment_count,
        p.max_installments,
        p.payment_types
    from orders as o
    inner join order_payments as p on o.order_id = p.order_id

    {% if is_incremental() %}
    -- Only process records newer than the most recent record in the existing table
    where
        date_trunc('day', o.purchased_at)::date >= (
            select coalesce(max(fp.order_date_id), date '1900-01-01')
            from {{ this }} as fp
        )
    {% endif %}
)

select * from final
