select
    pickup_date,
    count(*)                         as trips,
    sum(total_amount)                as revenue,
    avg(fare_amount)                 as avg_fare,
    avg(tip_pct)                     as avg_tip_pct,
    sum(case when payment_type = 1 then 1 else 0 end) * 1.0 / count(*) as card_share
from {{ ref('fct_trips') }}
group by 1
