FROM apache/airflow:2.9.3-python3.11
USER root
RUN mkdir -p /data && chmod 777 /data
USER airflow
# dbt / delta-rs / Great Expectations live in their own venv so they never fight Airflow's pinned deps
RUN python -m venv /home/airflow/venv && /home/airflow/venv/bin/pip install --no-cache-dir \
    "dbt-duckdb~=1.8.0" "duckdb==1.1.3" "deltalake==0.20.2" pandas pyarrow "great-expectations>=1.0,<1.4"
