{{ config(
    materialized='incremental',
    unique_key='zone_hour_key',
    incremental_strategy='delete+insert'
) }}

-- Incremental with a 3-hour look-back: late-arriving trips re-open recent hours, so those hours
-- are fully recomputed (delete+insert on the key) instead of only appending newer rows.
select
    cast(pickup_zone_id as varchar) || '-' || strftime(pickup_hour, '%Y%m%d%H') as zone_hour_key,
    pickup_zone_id,
    pickup_hour,
    count(*)           as trips,
    sum(total_amount)  as revenue,
    avg(trip_distance) as avg_distance,
    avg(trip_minutes)  as avg_minutes
from {{ ref('fct_trips') }}
{% if is_incremental() %}
where pickup_hour >= (select max(pickup_hour) - interval 3 hour from {{ this }})
{% endif %}
group by 1, 2, 3
