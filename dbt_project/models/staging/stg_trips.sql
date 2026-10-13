select
    event_id                           as trip_id,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    cast(passenger_count as integer)   as passenger_count,
    trip_distance,
    pu_location_id                     as pickup_zone_id,
    do_location_id                     as dropoff_zone_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,
    airport_fee,
    processed_ts
from {{ source('lake', 'silver_trips') }}
