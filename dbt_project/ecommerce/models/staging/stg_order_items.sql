-- dbt/models/staging/stg_order_items.sql

with source as (
    select *
    from {{ source('raw', 'order_items') }}
),
renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_at,
        price,
        freight_value
    from source
    where order_id is not null
)

select *
from renamed
