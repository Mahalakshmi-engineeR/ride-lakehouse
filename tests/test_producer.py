import random

import pandas as pd

from producer.producer import inject_defects, load_trips, replay_order, to_events


def sample_df(n=10):
    return pd.DataFrame({
        "tpep_pickup_datetime": pd.date_range("2024-01-01", periods=n, freq="min")[::-1],
        "tpep_dropoff_datetime": pd.date_range("2024-01-01 00:30", periods=n, freq="min")[::-1],
        "fare_amount": [10.0] * n, "airport_fee": [0.0] * n,
    })


def test_load_sorts_by_pickup_and_renames_airport_fee(tmp_path):
    p = tmp_path / "t.parquet"
    sample_df().to_parquet(p)
    df = load_trips(str(p), max_rows=100)
    assert df["tpep_pickup_datetime"].is_monotonic_increasing
    assert "Airport_fee" in df.columns


def test_events_are_json_safe_with_ids():
    events = to_events(sample_df(3))
    assert [e["event_id"] for e in events] == ["00000000", "00000001", "00000002"]
    assert isinstance(events[0]["tpep_pickup_datetime"], str)


def test_defect_injection():
    ev = to_events(sample_df(1))[0]
    assert inject_defects(ev, random.Random(1), null_p=0, neg_p=1)["fare_amount"] < 0
    assert inject_defects(ev, random.Random(1), null_p=1, neg_p=0)["tpep_dropoff_datetime"] is None
    assert ev["fare_amount"] == 10.0  # original untouched


def test_duplicates_and_late_events_never_lose_data():
    events = to_events(sample_df(50))
    dup = list(replay_order(events, random.Random(1), dup_p=1.0, late_p=0.0, late_delay=5))
    assert len(dup) == 100
    late = list(replay_order(events, random.Random(1), dup_p=0.0, late_p=1.0, late_delay=5))
    assert sorted(e["event_id"] for e in late) == sorted(e["event_id"] for e in events)
