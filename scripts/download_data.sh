#!/usr/bin/env bash
# Downloads one month of NYC TLC yellow-taxi trips + the taxi-zone lookup (used as a dbt seed).
set -euo pipefail
MONTH="${1:-2024-01}"
BASE="https://d37ci6vzurychx.cloudfront.net"
mkdir -p data dbt_project/seeds
[ -f "data/yellow_tripdata_${MONTH}.parquet" ] || curl -fL -o "data/yellow_tripdata_${MONTH}.parquet" "${BASE}/trip-data/yellow_tripdata_${MONTH}.parquet"
[ -f dbt_project/seeds/taxi_zone_lookup.csv ] || curl -fL -o dbt_project/seeds/taxi_zone_lookup.csv "${BASE}/misc/taxi_zone_lookup.csv"
echo "data ready"
