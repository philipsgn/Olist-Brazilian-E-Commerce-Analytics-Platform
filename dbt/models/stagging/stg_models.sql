-- dbt/models/staging/stg_orders.sql
with source as (
    select * from {{ source('raw', 'raw_orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        order_status,
        cast(order_purchase_timestamp as timestamp) as purchased_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at
    from source
    where order_id is not null
)
select * from renamed