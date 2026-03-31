-- dbt/models/staging/stg_reviews.sql

with source as (
    select *
    from {{ source('raw', 'reviews') }}
),
renamed as (
    select
        review_id,
        order_id,
        review_score,
        cast(review_creation_date as timestamp) as review_created_at,
        cast(review_answer_timestamp as timestamp) as review_answered_at
    from source
    where review_id is not null
)

select *
from renamed
