-- dbt/models/staging/stg_sellers.sql

with source as (
    select *
    from {{ source('raw', 'sellers') }}
),
renamed as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from source
    where seller_id is not null
)

select *
from renamed
