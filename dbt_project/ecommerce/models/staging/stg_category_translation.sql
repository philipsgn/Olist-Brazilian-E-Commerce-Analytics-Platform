-- dbt/models/staging/stg_category_translation.sql

with source as (
    select *
    from {{ source('raw', 'category_translation') }}
),
renamed as (
    select
        product_category_name,
        product_category_name_english
    from source
    where product_category_name is not null
)

select *
from renamed
