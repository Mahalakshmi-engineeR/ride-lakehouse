"""Streamlit dashboard over the Gold marts in DuckDB (read-only)."""
import os
import time

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB = os.getenv("DUCKDB_PATH", "/data/warehouse.duckdb")
st.set_page_config(page_title="Ride Lakehouse", layout="wide")


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    last = None
    for attempt in range(5):  # dbt may hold the DuckDB write lock for a few seconds
        try:
            con = duckdb.connect(DB, read_only=True)
            try:
                return con.execute(sql).df()
            finally:
                con.close()
        except Exception as exc:
            last = exc
            time.sleep(1 + attempt)
    raise last


st.title("Ride Lakehouse - Gold layer")
try:
    daily = q("select * from main.agg_daily_revenue order by pickup_date")
except Exception:
    st.info("Gold tables not built yet. Trigger the `gold_refresh` DAG in Airflow (http://localhost:8080).")
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Trips", f"{int(daily['trips'].sum()):,}")
c2.metric("Revenue", f"${daily['revenue'].sum():,.0f}")
c3.metric("Avg tip %", f"{daily['avg_tip_pct'].mean():.1%}")

t1, t2, t3 = st.tabs(["Revenue & demand", "Zones heatmap", "Data quality"])

with t1:
    st.plotly_chart(px.bar(daily, x="pickup_date", y="revenue", title="Daily revenue"), use_container_width=True)
    hourly = q("""select pickup_hour, sum(trips) trips from main.agg_demand_by_zone_hour
                  group by 1 order by 1""")
    st.plotly_chart(px.line(hourly, x="pickup_hour", y="trips", title="Trips per hour"), use_container_width=True)
    top = q("""select z.zone_name, sum(a.trips) trips from main.agg_demand_by_zone_hour a
               join main.dim_zone z on z.zone_id = a.pickup_zone_id
               group by 1 order by 2 desc limit 10""")
    st.plotly_chart(px.bar(top, x="trips", y="zone_name", orientation="h", title="Top 10 pickup zones")
                    .update_yaxes(autorange="reversed"), use_container_width=True)

with t2:
    heat = q("""
        select z.zone_name, cast(extract('hour' from a.pickup_hour) as integer) as hr, sum(a.trips) trips
        from main.agg_demand_by_zone_hour a join main.dim_zone z on z.zone_id = a.pickup_zone_id
        where a.pickup_zone_id in (select pickup_zone_id from main.agg_demand_by_zone_hour
                                   group by 1 order by sum(trips) desc limit 15)
        group by 1, 2""")
    st.plotly_chart(px.imshow(heat.pivot(index="zone_name", columns="hr", values="trips").fillna(0),
                              aspect="auto", title="Demand: top 15 zones x hour of day"),
                    use_container_width=True)

with t3:
    try:
        rej = q("select reject_reason, count(*) as rows from raw.quarantine_trips group by 1 order by 2 desc")
        st.subheader("Quarantined records by reason")
        st.plotly_chart(px.bar(rej, x="reject_reason", y="rows"), use_container_width=True)
        total_valid = int(daily["trips"].sum())
        st.metric("Reject rate", f"{rej['rows'].sum() / (rej['rows'].sum() + total_valid):.2%}")
    except Exception:
        st.write("No quarantine data yet.")
