select
    trip_id,
    pickup_ts,
    dropoff_ts,
    cast(pickup_ts as date)                      as pickup_date,
    date_trunc('hour', pickup_ts)                as pickup_hour,
    date_diff('minute', pickup_ts, dropoff_ts)   as trip_minutes,
    pickup_zone_id,
    dropoff_zone_id,
    payment_type,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    case when fare_amount > 0 then tip_amount / fare_amount end as tip_pct
from {{ ref('stg_trips') }}
