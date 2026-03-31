-- dbt/models/marts/dim_date.sql

{{ config(
    materialized='incremental',
    unique_key='date_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with dates as (
    select distinct date_trunc('day', purchased_at)::date as date_id
    from {{ ref('stg_orders') }}
    where purchased_at is not null
)

select
    date_id,
    extract(year from date_id) as year,
    extract(month from date_id) as month,
    extract(day from date_id) as day,
    extract(quarter from date_id) as quarter,
    case when extract(isodow from date_id) in (6, 7) then true else false end as is_weekend
from dates

{% if is_incremental() %}
where date_id >= (
    select coalesce(max(date_id), date '1900-01-01') from {{ this }}
)
{% endif %}
