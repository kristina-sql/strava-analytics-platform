{{ config(
    materialized='table',
    unique_key='date_id'
) }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

select
    date_day as date_id,
    to_char(date_day, 'day') as day_of_week,
    extract(isodow from date_day) as day_of_week_num,
    extract(week from date_day) as week_of_year,
    extract(month from date_day) as month,
    to_char(date_day, 'month') as month_name,
    extract(quarter from date_day) as quarter,
    extract(year from date_day) as year,
    case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from date_spine