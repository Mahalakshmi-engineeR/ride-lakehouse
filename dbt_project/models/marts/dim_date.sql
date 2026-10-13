with days as (
    select cast(generate_series as date) as date_day
    from generate_series(timestamp '2024-01-01', timestamp '2025-12-31', interval 1 day)
)
select
    date_day,
    extract('year' from date_day)    as year,
    extract('month' from date_day)   as month,
    strftime(date_day, '%B')         as month_name,
    dayofweek(date_day)              as day_of_week,   -- 0 = Sunday
    dayofweek(date_day) in (0, 6)    as is_weekend
from days
