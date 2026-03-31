-- dbt/models/staging/stg_customers.sql

with source as (
    select *
    from {{ source('raw', 'customers') }}
),
renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from source
    where customer_id is not null
)

select *
from renamed
