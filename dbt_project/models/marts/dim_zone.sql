select
    cast(LocationID as integer) as zone_id,
    Borough                     as borough,
    Zone                        as zone_name,
    service_zone
from {{ ref('taxi_zone_lookup') }}
