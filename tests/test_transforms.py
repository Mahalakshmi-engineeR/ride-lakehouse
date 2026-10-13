import json

import pytest

from streaming.transforms import add_validation, parse_trips

BASE = {
    "event_id": "1", "VendorID": 1,
    "tpep_pickup_datetime": "2024-01-01 10:00:00", "tpep_dropoff_datetime": "2024-01-01 10:20:00",
    "passenger_count": 1.0, "trip_distance": 3.2, "RatecodeID": 1.0, "store_and_fwd_flag": "N",
    "PULocationID": 161, "DOLocationID": 236, "payment_type": 1, "fare_amount": 15.5, "extra": 1.0,
    "mta_tax": 0.5, "tip_amount": 2.0, "tolls_amount": 0.0, "improvement_surcharge": 1.0,
    "total_amount": 20.0, "congestion_surcharge": 2.5, "Airport_fee": 0.0,
}


def run(spark, **overrides):
    payload = json.dumps({**BASE, **overrides})
    df = spark.createDataFrame([(payload,)], ["payload"])
    return add_validation(parse_trips(df)).collect()[0]


def test_valid_trip_has_no_reject_reason_and_correct_types(spark):
    row = run(spark)
    assert row.reject_reason is None
    assert row.pu_location_id == 161
    assert row.pickup_date.isoformat() == "2024-01-01"


@pytest.mark.parametrize("overrides,expected", [
    ({"fare_amount": -5.0}, "invalid_fare"),
    ({"tpep_dropoff_datetime": None}, "missing_timestamp"),
    ({"tpep_dropoff_datetime": "2024-01-01 09:00:00"}, "non_positive_duration"),
    ({"PULocationID": None}, "missing_location"),
    ({"trip_distance": 9999.0}, "invalid_distance"),
])
def test_bad_rows_get_a_reason(spark, overrides, expected):
    assert run(spark, **overrides).reject_reason == expected


def test_float_encoded_ints_do_not_null_the_record(spark):
    row = run(spark, passenger_count=2.0, RatecodeID=5.0)
    assert row.passenger_count == 2 and row.rate_code_id == 5
