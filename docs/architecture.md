# Weather Pipeline — Architecture

Daily weather ETL: **Open-Meteo REST API + `cities.csv` → bronze JSON → silver (validated) → Postgres → gold daily stats**, orchestrated by Airflow 2.10.5 and visualised by a Streamlit dashboard. Everything runs locally in Docker Compose.

## 1. Components

| Service | Image | Host ports | Role |
|---|---|---|---|
| `airflow-webserver` | `weather-pipeline-airflow:1.0` (based on `apache/airflow:2.10.5-python3.11`) | **8082** → 8080 | Airflow UI (admin/admin) |
| `airflow-scheduler` | same image | — | Runs the DAG, retries, callbacks |
| `airflow-init` | same image | — | One-shot `airflow db migrate` + admin user creation |
| `airflow-db` | `postgres:16-alpine` | — (internal) | Airflow metadata DB |
| `warehouse` | `postgres:16-alpine` | **5434** → 5432 | Analytics warehouse (`weather_dw`) with `silver` / `gold` / `dq` / `ops` schemas |
| `dashboard` | `weather-pipeline-dashboard:1.0` | **8501** | Streamlit monitoring & insights app |

Ports 5434 (not 5433) and 8082 (not 8080) were chosen because those ports were already taken on the build machine — see `docs/DEVIATIONS.md`.

Data flow between services:

```
Open-Meteo API ──HTTPS──> extract ──> data/raw/date=YYYY-MM-DD/*.json        (bronze, on host disk)
                                        │  validate_raw (data contract)
                                        ▼
                                   transform ──> data/silver/date=YYYY-MM-DD/weather_hourly.parquet
                                        │  quality_gate (10 checks, blocking)
                                        ▼
Postgres warehouse  <── psycopg2 upserts ── load_silver / build_gold / load_dim_city
      │
      ├── silver.dim_city, silver.weather_hourly   (hourly facts, PK (city, observed_at))
      ├── gold.weather_daily                      (daily aggregates, PK (city, obs_date))
      ├── dq.check_results                        (every check ever run)
      └── ops.pipeline_run_log, ops.alerts        (monitoring: stage log + failure alerts)
                │
                ▼
        Streamlit dashboard (read-only, SQLAlchemy + pandas)
```

## 2. Repository layout

```
.
├── docker-compose.yml          # 6 services, 2 named volumes
├── Dockerfile                  # airflow:2.10.5 + pip constraints file
├── requirements.txt            # requests, pandas, pyarrow, psycopg2-binary, pytest
├── Makefile                    # init/up/down/reset/logs/test/trigger/backfill/fault-*/psql/counts/runs
├── .env.example                # copied to .env by `make init` (never committed)
├── dags/
│   └── weather_pipeline_dag.py # the only DAG
├── include/weather_pipeline/   # importable package (PYTHONPATH=/opt/airflow/include)
│   ├── config.py               # paths, API url, hourly variables, fault names
│   ├── dates.py                # resolve_target_date(param, ds)
│   ├── extract.py              # CSV reader + Open-Meteo client with retries
│   ├── raw_validation.py       # bronze schema contract
│   ├── transform.py            # bronze → silver (rename, cast, clean, parquet)
│   ├── quality.py              # 10 DQ checks (error/warn) + blocking_failures()
│   ├── load.py                 # psycopg2 upserts, DQ/stage/alert writes
│   └── alerts.py               # on_failure_callback → ops.alerts
├── warehouse/init/001_schema.sql  # runs once on first container start
├── dashboard/app.py            # 3-tab Streamlit app (weather / quality / health)
├── data/{raw,silver}/date=.../ # partitioned by day (git-ignored except .gitkeep)
├── logs/                       # Airflow task logs (git-ignored except .gitkeep)
└── tests/                      # 27 pytest tests, run inside the Airflow container
```

## 3. The DAG: `weather_pipeline`

* **Schedule:** `@daily`, `catchup=False`, `max_active_runs=1`, `start_date=2026-10-01` (UTC).
* **Parameter:** `target_date` (string, default blank) — blank means "use the logical date (`ds`)", otherwise any `YYYY-MM-DD` is processed. This one parameter powers both manual reruns and backfills.
* **Default args:** `retries=3`, `retry_delay=20s`, `retry_exponential_backoff=True` (20 s → 40 s → 80 s, capped at 2 min), `execution_timeout=10 min`, `on_failure_callback=notify_failure`.

Task graph (TaskFlow API):

```
resolve_date ─┐
load_dim_city ┴─(dim_city >> silver_rows)─┐
                                          ├─> extract_weather ─> validate_raw ─> transform_to_silver
                                          │                                            │
                                          │                                       quality_gate
                                          │                                            │
                                          │                                        load_silver
                                          │                                            │
                                          └────────────────────────────────────────> build_gold
```

| Task | Layer | What it does | Failure behaviour |
|---|---|---|---|
| `resolve_date` | — | Picks `target_date` from param or `ds`, validates format | `ValueError` → fail (no retry, nothing to fix) |
| `load_dim_city` | silver | Upserts `data/reference/cities.csv` into `silver.dim_city` | upsert = idempotent |
| `extract_weather` | bronze | Calls Open-Meteo once per city (10 calls), writes one JSON per city under `data/raw/date=D/` | transient 5xx/429 retried by urllib3 (3×) *and* by Airflow (3×); HTTP 400 → `AirflowFailException` (no retry) |
| `validate_raw` | bronze | Data contract: `hourly` dict, 4 required keys + `time`, all length 24 | `AirflowFailException` (no retry) |
| `transform_to_silver` | silver | Rename columns, cast numerics, drop duplicates, sort; writes `data/silver/date=D/weather_hourly.parquet` | `bad_data` fault corrupts the frame here, on purpose |
| `quality_gate` | silver | Runs the 10 checks, inserts rows into `dq.check_results`, then **blocks** if any `severity='error'` check failed → `AirflowFailException` | `load_silver`/`build_gold` become `upstream_failed`, warehouse untouched |
| `load_silver` | warehouse | `INSERT ... ON CONFLICT (city, observed_at) DO UPDATE` → `silver.weather_hourly` | re-running the same day is a no-op (idempotent) |
| `build_gold` | warehouse | Aggregates the day's silver rows → `gold.weather_daily` (avg/min/max temp, humidity, precip, wind, 24 h check, weather category) | same upsert semantics |

Dependencies are declared with `>>` at the bottom; `dim_city >> silver_rows` guarantees the dimension exists before hourly rows reference it (FK).

## 4. Warehouse schema (star-ish)

* `silver.dim_city(city PK, country, latitude, longitude, population)` — Type-1 dimension from the CSV.
* `silver.weather_hourly` — hourly fact, **PK `(city, observed_at)`**, FK → `dim_city`. This PK is what makes every load an idempotent upsert.
* `gold.weather_daily` — one row per city-day, **PK `(city, obs_date)`**, `hours_count = 24` is asserted by the DQ gate.
* `dq.check_results` — append-only history: `run_id, target_date, check_name, severity(error|warn), status(pass|fail), failed_count, details, checked_at`.
* `ops.pipeline_run_log` — one row per stage per run (`extract`, `validate_raw`, `transform`, `quality_gate`, `load_silver`, `build_gold`) with `rows_processed`, `status`, `message`.
* `ops.alerts` — written by the `on_failure_callback`: `task_id, run_id, try_number, error_message, created_at`.

Schema is applied idempotently (`CREATE TABLE IF NOT EXISTS`) from `warehouse/init/001_schema.sql` on first container start.

## 5. Reliability features (the point of the mini-project)

1. **Retries with exponential backoff** at two levels: HTTP (`requests` `Retry(total=3, backoff_factor=1.0)` for 429/5xx/connection errors) and Airflow (`retries=3`, `retry_exponential_backoff=True`).
2. **Timeouts** — HTTP 30 s per request, `execution_timeout=10 min` per task, so a hung API can never wedge a DAG run.
3. **Idempotency** — raw files are written atomically (tmp + rename), silver/gold loads are `ON CONFLICT` upserts, so any run can be repeated safely (verified in check V3).
4. **Blocking data-quality gate** — 10 checks between transform and load; `error` severity stops the run *before* anything reaches the warehouse (verified in V4), `warn` severity is recorded but never blocks.
5. **Fault injection** — Airflow Variable `weather_inject_fault` ∈ `none | api_down | bad_data`; toggled by `make fault-api / fault-data / fault-off` without touching code.
6. **Alerts** — every failed task appends a row to `ops.alerts` (dashboard tab 3), including the try number and exception text.
7. **Run logging** — every stage writes `ops.pipeline_run_log`, which is what the dashboard's "Pipeline health" tab shows.

## 6. Testing

27 pytest tests in `tests/`, executed **inside** the Airflow image so they use the same dependencies:

```
docker compose run --rm --no-deps airflow-scheduler python -m pytest /opt/airflow/tests -q -p no:cacheprovider
```

Suites: `dates` (target-date resolution), `extract` (CSV contract, retry session, saved JSON shape, fault URL), `raw_validation` (24-hour contract), `transform` (renames/casts/cleaning, bad-data injection), `quality` (every check passes on good data, blocks on bad), `dag_integrity` (DAG parses, task ids, retries/backoff config, no import errors).

## 7. Ports & URLs (as actually running)

| What | URL |
|---|---|
| Airflow UI | http://localhost:8082 (admin/admin) |
| Streamlit dashboard | http://localhost:8501 |
| Warehouse psql | `localhost:5434`, db `weather_dw`, user/password `warehouse` |
| Open-Meteo API | `https://api.open-meteo.com/v1/forecast` (no key required) |
