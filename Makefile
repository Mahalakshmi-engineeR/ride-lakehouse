LIMIT ?= 200000
RATE  ?= 500

.PHONY: data up produce demo down reset logs test lint dbt-ci

data:            ## download one month of TLC data + zone lookup
	mkdir -p data
	[ -f data/yellow_tripdata_2024-01.parquet ] || curl -L -o data/yellow_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
	[ -f data/taxi_zone_lookup.csv ] || curl -L -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

up:              ## start the platform
	docker compose up -d --build

produce:         ## replay trips into Kafka (LIMIT, RATE overridable)
	docker compose --profile producer run --rm producer --limit $(LIMIT) --rate $(RATE)

demo: data up    ## one-command demo
	@echo "Waiting 60s for services..." && sleep 60
	$(MAKE) produce
	@echo "Airflow: http://localhost:8080 (admin/admin) | MinIO: http://localhost:9001 | Dashboard: http://localhost:8501"

down:
	docker compose down

reset:           ## wipe all state
	docker compose down -v

logs:
	docker compose logs -f spark-bronze spark-silver

test:
	pytest -q

lint:
	ruff check .

dbt-ci:          ## run dbt against the bundled sample data
	cd dbt_project && dbt build --target ci --profiles-dir .
