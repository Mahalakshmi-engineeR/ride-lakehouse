"""Publish Silver (+ quarantine) from the lakehouse into the DuckDB warehouse that dbt models on top of.

Full refresh is fine at this scale. At larger scale: incremental load on `processed_ts`, or point
dbt at an engine that reads Delta natively (Spark/Trino/Databricks).
"""
import duckdb
from deltalake import DeltaTable

from common.lake import DUCKDB_PATH, TABLES, storage_options


def load(con, name: str, target: str, required: bool) -> None:
    try:
        df = DeltaTable(TABLES[name], storage_options=storage_options()).to_pandas()
    except Exception as exc:  # table may not exist yet (e.g. no rejects so far)
        if required:
            raise
        print(f"skip {name}: {exc}")
        return
    con.register("src_df", df)
    con.execute(f"create or replace table {target} as select * from src_df")
    con.unregister("src_df")
    print(f"loaded {len(df):,} rows into {target}")


def main() -> None:
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("create schema if not exists raw")
    load(con, "silver", "raw.silver_trips", required=True)
    load(con, "quarantine", "raw.quarantine_trips", required=False)
    con.close()


if __name__ == "__main__":
    main()
