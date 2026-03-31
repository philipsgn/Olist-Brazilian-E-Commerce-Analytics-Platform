-- dbt/models/marts/dim_geolocation.sql

select
    geolocation_zip_code_prefix as zip_code_prefix,
    min(geolocation_city) as city,
    min(geolocation_state) as state,
    avg(geolocation_lat) as avg_lat,
    avg(geolocation_lng) as avg_lng
from {{ ref('stg_geolocation') }}
group by geolocation_zip_code_prefix
