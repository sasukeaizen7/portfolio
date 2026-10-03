-- Calendar dimension generated with dbt_utils.date_spine (one row per day, sales or not).
with days as (
    {{ dbt_utils.date_spine(datepart="day", start_date="cast('2009-12-01' as date)", end_date="cast('2012-01-01' as date)") }}
)

select
    to_char(date_day, 'YYYYMMDD')::int         as date_key,
    date_day::date                             as date,
    extract(year from date_day)::int           as year,
    extract(quarter from date_day)::int        as quarter,
    extract(month from date_day)::int          as month,
    to_char(date_day, 'FMMonth')               as month_name,
    extract(isodow from date_day)::int         as iso_weekday,
    extract(isodow from date_day) in (6, 7)    as is_weekend
from days
