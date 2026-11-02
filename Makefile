-include .env
WAREHOUSE_USER ?= warehouse
WAREHOUSE_DB ?= weather_dw
DATE ?=
START ?=
END ?=

COMPOSE = docker compose
AF = $(COMPOSE) exec -T airflow-scheduler
PSQL = $(COMPOSE) exec -T warehouse psql -U $(WAREHOUSE_USER) -d $(WAREHOUSE_DB)

.PHONY: init up down reset logs test trigger backfill fault-api fault-data fault-off psql counts runs

init:
	mkdir -p logs data/raw data/silver
	test -f .env || cp .env.example .env

up: init
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Airflow UI : http://localhost:8082  (login with AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD from .env, default admin / admin)"
	@echo "Dashboard  : http://localhost:8501"
	@echo "Warehouse  : localhost:5434  (user/password/db from .env)"

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v
	find data/raw data/silver -mindepth 1 ! -name .gitkeep -delete

logs:
	$(COMPOSE) logs -f airflow-scheduler

test:
	$(COMPOSE) run --rm --no-deps airflow-scheduler python -m pytest /opt/airflow/tests -q -p no:cacheprovider

trigger:
	$(AF) airflow dags trigger weather_pipeline $(if $(DATE),--conf '{"target_date": "$(DATE)"}',)

backfill:
	test -n "$(START)" -a -n "$(END)" || (echo "Usage: make backfill START=YYYY-MM-DD END=YYYY-MM-DD" && exit 1)
	$(AF) airflow dags backfill weather_pipeline --start-date $(START) --end-date $(END) --reset-dagruns --yes

fault-api:
	$(AF) airflow variables set weather_inject_fault api_down

fault-data:
	$(AF) airflow variables set weather_inject_fault bad_data

fault-off:
	$(AF) airflow variables set weather_inject_fault none

runs:
	$(AF) airflow dags list-runs -d weather_pipeline

psql:
	$(COMPOSE) exec warehouse psql -U $(WAREHOUSE_USER) -d $(WAREHOUSE_DB)

counts:
	$(PSQL) -c "SELECT 'silver.weather_hourly' AS table_name, COUNT(*) FROM silver.weather_hourly UNION ALL SELECT 'gold.weather_daily', COUNT(*) FROM gold.weather_daily UNION ALL SELECT 'dq.check_results', COUNT(*) FROM dq.check_results UNION ALL SELECT 'ops.alerts', COUNT(*) FROM ops.alerts;"
