"""Creates a tiny synthetic raw.silver_trips so CI can run `dbt seed/run/test` without Spark or MinIO."""
import os

import duckdb

con = duckdb.connect(os.getenv("DUCKDB_PATH", "ci.duckdb"))
con.execute("create schema if not exists raw")
con.execute("""
create or replace table raw.silver_trips as
select
    lpad(cast(i as varchar), 8, '0')                        as event_id,
    1                                                       as vendor_id,
    timestamp '2024-01-01 00:00:00' + i * interval 7 minute as pickup_ts,
    timestamp '2024-01-01 00:00:00' + i * interval 7 minute + interval 12 minute as dropoff_ts,
    1.0                                                     as passenger_count,
    1.0 + (i % 9)                                           as trip_distance,
    1                                                       as rate_code_id,
    'N'                                                     as store_and_fwd_flag,
    (i % 263) + 1                                           as pu_location_id,
    ((i * 7) % 263) + 1                                     as do_location_id,
    (i % 2) + 1                                             as payment_type,
    5.0 + (i % 40)                                          as fare_amount,
    0.5 as extra, 0.5 as mta_tax, 1.0 + (i % 5) as tip_amount, 0.0 as tolls_amount,
    1.0 as improvement_surcharge,
    (5.0 + (i % 40)) * 1.2                                  as total_amount,
    2.5 as congestion_surcharge, 0.0 as airport_fee,
    cast(timestamp '2024-01-01 00:00:00' + i * interval 7 minute as date) as pickup_date,
    now()                                                   as processed_ts
from range(2000) t(i)
""")
print("sample data ready")
