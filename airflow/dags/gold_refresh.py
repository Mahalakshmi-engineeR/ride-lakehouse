"""Hourly batch refresh: Silver (Delta) -> DuckDB -> quality gate -> dbt Gold -> tests -> maintenance.

Streaming jobs (bronze/silver) run continuously outside Airflow; Airflow owns the batch layer.
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"
VENV = "/home/airflow/venv/bin"
DBT = f"{VENV}/dbt"
DBT_ARGS = f"--project-dir {PROJECT}/dbt_project --profiles-dir {PROJECT}/dbt_project"


def notify_failure(context) -> None:
    ti = context["task_instance"]
    msg = f":red_circle: Airflow task failed: {ti.dag_id}.{ti.task_id} (run {context['run_id']})"
    print(msg)
    url = os.getenv("SLACK_WEBHOOK_URL")
    if url:
        req = urllib.request.Request(url, json.dumps({"text": msg}).encode(), {"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)


def py(module: str) -> str:
    return f"cd {PROJECT} && {VENV}/python -m {module}"


with DAG(
    dag_id="gold_refresh",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2),
                  "on_failure_callback": notify_failure},
    tags=["lakehouse", "dbt"],
) as dag:
    load = BashOperator(task_id="load_silver_to_duckdb", bash_command=py("loader.load_to_duckdb"))
    quality_silver = BashOperator(task_id="quality_silver", bash_command=py("quality.run_checks --layer silver"))
    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"{DBT} seed {DBT_ARGS}")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run {DBT_ARGS}")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test {DBT_ARGS}")
    quality_gold = BashOperator(task_id="quality_gold", bash_command=py("quality.run_checks --layer gold"))
    optimize = BashOperator(task_id="delta_maintenance", bash_command=py("maintenance.delta_maintenance"))

    load >> quality_silver >> dbt_seed >> dbt_run >> dbt_test >> quality_gold >> optimize
