"""Replays NYC TLC yellow-taxi trips into Kafka as a live, *messy* event stream.

Injected defects (all configurable) so downstream layers have real work to do:
  - duplicates          (same event_id sent twice)      -> silver dedup
  - late events         (delivered N events later)       -> watermark handling
  - null dropoff times  (missing field)                  -> quarantine
  - negative fares      (bad value)                      -> quarantine
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time

import pandas as pd

PICKUP, DROPOFF = "tpep_pickup_datetime", "tpep_dropoff_datetime"


def load_trips(path: str, max_rows: int) -> pd.DataFrame:
    """Load one TLC parquet file, keep its dominant month (the files contain stray dates), sort by time."""
    df = pd.read_parquet(path).rename(columns={"airport_fee": "Airport_fee"})
    df = df.dropna(subset=[PICKUP, DROPOFF])
    months = df[PICKUP].dt.to_period("M")
    df = df[months == months.mode()[0]]
    return df.sort_values(PICKUP).head(max_rows).reset_index(drop=True)


def to_events(df: pd.DataFrame) -> list[dict]:
    d = df.copy()
    for c in (PICKUP, DROPOFF):
        d[c] = d[c].dt.strftime("%Y-%m-%d %H:%M:%S")
    records = json.loads(d.to_json(orient="records"))  # NaN -> null, numpy types -> json
    for i, r in enumerate(records):
        r["event_id"] = f"{i:08d}"
    return records


def inject_defects(event: dict, rng: random.Random, null_p: float, neg_p: float) -> dict:
    ev = dict(event)
    r = rng.random()
    if r < neg_p:
        ev["fare_amount"] = -abs(ev.get("fare_amount") or 1.0)
    elif r < neg_p + null_p:
        ev[DROPOFF] = None
    return ev


def replay_order(events, rng: random.Random, dup_p: float, late_p: float, late_delay: int):
    """Yield events in delivery order, with duplicates and late arrivals."""
    held: list[tuple[int, dict]] = []
    for i, ev in enumerate(events):
        if rng.random() < late_p:
            held.append((i + late_delay, ev))
        else:
            yield ev
            if rng.random() < dup_p:
                yield ev
        due = [h for h in held if h[0] <= i]
        held = [h for h in held if h[0] > i]
        for _, e in due:
            yield e
    for _, e in held:
        yield e


def main() -> None:
    from confluent_kafka import Producer  # imported lazily so unit tests don't need it

    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/yellow_tripdata_2024-01.parquet")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default="trips")
    ap.add_argument("--max-rows", type=int, default=200_000)
    ap.add_argument("--rate", type=int, default=1000, help="events per second")
    ap.add_argument("--dup-p", type=float, default=0.02)
    ap.add_argument("--late-p", type=float, default=0.01)
    ap.add_argument("--late-delay", type=int, default=300, help="late events arrive this many events later")
    ap.add_argument("--null-p", type=float, default=0.01)
    ap.add_argument("--neg-p", type=float, default=0.005)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    events = [inject_defects(e, rng, args.null_p, args.neg_p) for e in to_events(load_trips(args.file, args.max_rows))]
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})

    sent = 0
    for ev in replay_order(events, rng, args.dup_p, args.late_p, args.late_delay):
        while True:
            try:
                producer.produce(args.topic, key=ev["event_id"], value=json.dumps(ev))
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            time.sleep(100 / args.rate)  # throttle
        if sent % 10_000 == 0:
            print(f"sent {sent} events", flush=True)
    producer.flush()
    print(f"done: {sent} events sent for {len(events)} unique trips", flush=True)


if __name__ == "__main__":
    main()
