-- dim_date.sql
-- Production-grade: Auto-generate calendar up to 2030
-- Ensures simulated orders (2026-2030) can be joined successfully

{{ config(materialized='table') }}

with date_series as (
    -- Use Postgres generate_series to create all dates from 2016 to 2030
    select
        generate_series(
            '2016-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )::date as date_day
),

final as (
    select
        date_day as date_id,
        extract(year from date_day) as year_number,
        extract(month from date_day) as month_number,
        extract(quarter from date_day) as quarter_number,
        extract(day from date_day) as day_number,
        extract(dow from date_day) as day_of_week,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Day') as day_name,
        coalesce(extract(isodow from date_day) in (6, 7), false) as is_weekend
    from date_series
)

select * from final
