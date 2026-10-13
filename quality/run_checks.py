"""Data-quality gates (Great Expectations 1.x). Exit code 1 fails the Airflow task and blocks downstream work.

    python -m quality.run_checks --layer silver
    python -m quality.run_checks --layer gold
"""
import argparse
import os
import sys

import duckdb
import great_expectations as gx
import pandas as pd
from deltalake import DeltaTable

from common.lake import DUCKDB_PATH, TABLES, storage_options

gxe = gx.expectations
MAX_REJECT_RATE = float(os.getenv("MAX_REJECT_RATE", "0.10"))
FRESHNESS_MAX_HOURS = float(os.getenv("FRESHNESS_MAX_HOURS", "24"))


def validate(df: pd.DataFrame, name: str, expectations: list) -> list[str]:
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas(f"{name}_src").add_dataframe_asset(f"{name}_asset")
    batch = asset.add_batch_definition_whole_dataframe(f"{name}_batch").get_batch(
        batch_parameters={"dataframe": df}
    )
    failures = []
    for exp in expectations:
        result = batch.validate(exp)
        label = f"{type(exp).__name__}({getattr(exp, 'column', '')})"
        print(("PASS " if result.success else "FAIL ") + label)
        if not result.success:
            failures.append(label)
    return failures


def check_silver() -> list[str]:
    opts = storage_options()
    silver = DeltaTable(TABLES["silver"], storage_options=opts).to_pandas()
    failures = validate(silver, "silver", [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="pickup_ts"),
        gxe.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0, max_value=1000, mostly=0.999),
        gxe.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0, max_value=500),
        gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=[0, 1, 2, 3, 4, 5, 6], mostly=0.99),
    ])

    # Reject-rate gate: too many quarantined rows means upstream/schema trouble.
    try:
        n_bad = len(DeltaTable(TABLES["quarantine"], storage_options=opts).to_pyarrow_table(columns=["event_id"]))
    except Exception:
        n_bad = 0
    rate = n_bad / max(n_bad + len(silver), 1)
    print(f"reject rate = {rate:.2%} (limit {MAX_REJECT_RATE:.0%})")
    if rate > MAX_REJECT_RATE:
        failures.append("reject_rate_too_high")

    # Freshness is a warning only: the demo replays a finite file, so data goes stale by design.
    age_h = (pd.Timestamp.now(tz="UTC") - pd.to_datetime(silver["processed_ts"], utc=True).max()).total_seconds() / 3600
    if age_h > FRESHNESS_MAX_HOURS:
        print(f"WARN silver data is {age_h:.1f}h old (limit {FRESHNESS_MAX_HOURS}h)")
    return failures


def check_gold() -> list[str]:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    agg = con.execute("select * from main.agg_demand_by_zone_hour").df()
    daily = con.execute("select * from main.agg_daily_revenue").df()
    con.close()
    return validate(agg, "gold_zone_hour", [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToBeUnique(column="zone_hour_key"),
        gxe.ExpectColumnValuesToNotBeNull(column="pickup_zone_id"),
        gxe.ExpectColumnValuesToBeBetween(column="trips", min_value=1),
    ]) + validate(daily, "gold_daily", [
        gxe.ExpectColumnValuesToBeUnique(column="pickup_date"),
        gxe.ExpectColumnValuesToBeBetween(column="revenue", min_value=0),
    ])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", choices=["silver", "gold"], required=True)
    layer = ap.parse_args().layer
    failures = check_silver() if layer == "silver" else check_gold()
    if failures:
        print(f"\n{len(failures)} data-quality check(s) failed: {failures}")
        sys.exit(1)
    print("\nall data-quality checks passed")


if __name__ == "__main__":
    main()
