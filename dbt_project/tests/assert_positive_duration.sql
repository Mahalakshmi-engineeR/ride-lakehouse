-- Singular test: any returned row is a failure.
select * from {{ ref('fct_trips') }} where trip_minutes <= 0
