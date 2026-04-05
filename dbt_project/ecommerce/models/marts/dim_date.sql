-- dim_date.sql
-- Nâng cấp lên phiên bản "Production-grade": Tự động sinh lịch đến năm 2030
-- Điều này đảm bảo các đơn hàng giả (simulated data) năm 2026-2030 đều được join thành công

{{ config(materialized='table') }}

with date_series as (
    -- Dùng hàm generate_series của Postgres để tạo ra tất cả các ngày từ 2016 đến 2030
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
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(quarter from date_day) as quarter,
        extract(day from date_day) as day,
        extract(dow from date_day) as day_of_week,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Day') as day_name,
        case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
    from date_series
)

select * from final
