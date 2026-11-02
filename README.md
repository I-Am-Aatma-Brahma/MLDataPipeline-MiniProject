# Weather Pipeline

Daily weather ETL mini project: **Open-Meteo REST API → Airflow 2.10.5 → bronze JSON → silver parquet → Postgres warehouse (gold) → Streamlit dashboard**, all running locally with Docker Compose.

**Repository:** https://github.com/I-Am-Aatma-Brahma/MLDataPipeline-MiniProject

* 10 cities × 24 h = **240 silver rows / day**, 10 gold city-day rows / day
* Blocking **10-check data-quality gate** before anything reaches the warehouse
* **Retries with exponential backoff**, execution timeouts, **idempotent upserts**, failure **alerts**, run log
* Parameterised **rerun / backfill** via `target_date`
* Fault-injection demos (`api_down`, `bad_data`) switched by an Airflow Variable — no code edits
* 27 pytest tests, run inside the Airflow image

## Quickstart

```powershell
git clone https://github.com/I-Am-Aatma-Brahma/MLDataPipeline-MiniProject.git
cd MLDataPipeline-MiniProject

# make up  (or see the "No make?" table below)
docker compose up -d --build
```

Then open:

| What | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8082 | admin / admin (from `.env`) |
| Streamlit dashboard | http://localhost:8501 | — |
| Postgres warehouse | `localhost:5434`, db `weather_dw` | warehouse / warehouse |

First start runs `airflow-init` (DB migration + admin user) and applies `warehouse/init/001_schema.sql` automatically. Wait until `docker compose ps` shows `airflow-webserver`, `airflow-scheduler` and `warehouse` as **healthy**.

Run a day's data:

```powershell
docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
```

Watch it: http://localhost:8082 → `weather_pipeline` → Graph view. Then refresh http://localhost:8501.

## No make? Use these commands

`make` is optional — it only wraps the commands below. If you don't have it (this build was verified on a Windows host without `make`), run the right-hand column directly:

| Make target | Raw command (no `make` needed) |
|---|---|
| `make init` | `mkdir -p logs data/raw data/silver; if [ ! -f .env ]; then cp .env.example .env; fi` |
| `make up` | `docker compose up -d --build` |
| `make down` | `docker compose down` |
| `make reset` | `docker compose down -v` then delete everything under `data/raw` and `data/silver` except `.gitkeep` |
| `make logs` | `docker compose logs -f airflow-scheduler` |
| `make test` | `docker compose run --rm --no-deps airflow-scheduler python -m pytest /opt/airflow/tests -q -p no:cacheprovider` |
| `make trigger DATE=2026-10-30` | `docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'` |
| `make backfill START=2026-10-25 END=2026-10-27` | `docker compose exec -T airflow-scheduler airflow dags backfill weather_pipeline --start-date 2026-10-25 --end-date 2026-10-27 --reset-dagruns --yes` |
| `make fault-api` | `docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault api_down` |
| `make fault-data` | `docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault bad_data` |
| `make fault-off` | `docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault none` |
| `make psql` | `docker compose exec -T warehouse psql -U warehouse -d weather_dw` |
| `make counts` | `docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT 'silver.weather_hourly' AS table_name, COUNT(*) FROM silver.weather_hourly UNION ALL SELECT 'gold.weather_daily', COUNT(*) FROM gold.weather_daily UNION ALL SELECT 'dq.check_results', COUNT(*) FROM dq.check_results UNION ALL SELECT 'ops.alerts', COUNT(*) FROM ops.alerts;"` |
| `make runs` | `docker compose exec -T airflow-scheduler airflow dags list-runs -d weather_pipeline` |

> On PowerShell, keep the `\"` escapes inside `--conf` (see `docs/DEVIATIONS.md` D4). On bash the plain form `'{"target_date": "2026-10-30"}'` works.

## How it works

```
Open-Meteo API ──> extract ──> data/raw/date=D/*.json        (bronze)
                                 │ validate_raw (24-hour data contract)
                                 ▼
                            transform ──> data/silver/date=D/weather_hourly.parquet
                                 │ quality_gate (10 checks, blocking)
                                 ▼
Postgres weather_dw  <── psycopg2 upserts ── load_silver / build_gold
   silver.dim_city, silver.weather_hourly | gold.weather_daily | dq.check_results | ops.*
                                                                 │
                                                                 ▼
                                                      Streamlit dashboard (read-only)
```

DAG `weather_pipeline` (`@daily`, `catchup=False`, `max_active_runs=1`):

`resolve_date` + `load_dim_city` → `extract_weather` → `validate_raw` → `transform_to_silver` → **`quality_gate`** → `load_silver` → `build_gold`

* **Retries:** HTTP 3× (1/2/4 s) for 429/5xx + Airflow 3× exponential (20 s → 40 s → 80 s, cap 2 min); HTTP timeout 30 s, task timeout 10 min.
* **Idempotency:** atomic raw-file writes and `INSERT … ON CONFLICT (city, observed_at) DO UPDATE` — re-running a day never duplicates rows.
* **Quality gate:** 9 `error` checks block the load; 1 `warn` check (extreme temperature) only records. Results land in `dq.check_results`.
* **Alerts:** any failed task appends to `ops.alerts` (task, run, try number, exception) via `on_failure_callback`.

Full details: [`docs/architecture.md`](docs/architecture.md).

## Fault-injection demos

```powershell
docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault bad_data   # make fault-data
docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
# -> quality_gate fails, load_silver/build_gold upstream_failed, warehouse untouched, alert row written
docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault none        # make fault-off

docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault api_down    # make fault-api
docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
# -> extract_weather retries 4 tries then fails, downstream upstream_failed, alert row, counts unchanged
docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault none        # make fault-off
```

Step-by-step with expected outputs: [`docs/demo_script.md`](docs/demo_script.md).

## Verification (all executed on this build)

| # | Check | Result |
|---|---|---|
| V1 | `airflow dags list-import-errors` → none; pytest | **27 passed** |
| V2 | Trigger D=2026-10-30 | silver **240**, gold **10**, 10/10 DQ checks pass, 10 raw JSON + parquet present |
| V3 | Re-run the same day | silver/gold unchanged, dq history +10 (idempotent) |
| V4 | `bad_data` fault | `quality_gate` failed, downstream `upstream_failed`, silver still 240 / max 33.4 °C, 6 fail checks, alert row |
| V5 | `api_down` fault | `extract_weather` failed at **try 4**, downstream `upstream_failed`, alert row, counts unchanged |
| V6 | `fault-off` + rerun | run **success**, 240/10 restored for D |
| V7 | Backfill 2026-10-25→27 | 3 runs success, **720 rows** (240/day) |
| V8 | Dashboard | `/ _stcore/health` → `ok`, no tracebacks in logs, all 5 dashboard queries return rows |

## Project layout

```
├── docker-compose.yml          # 6 services (airflow x4, warehouse, dashboard)
├── Dockerfile                  # apache/airflow:2.10.5 + pip constraints
├── Makefile                    # init up down reset logs test trigger backfill fault-* psql counts runs
├── dags/weather_pipeline_dag.py
├── include/weather_pipeline/   # config, dates, extract, raw_validation, transform, quality, load, alerts
├── warehouse/init/001_schema.sql
├── dashboard/app.py            # Streamlit: insights / data quality / pipeline health
├── data/{raw,silver}/date=…    # day-partitioned files (git-ignored)
├── tests/                      # 27 pytest tests
└── docs/                       # architecture, syllabus mapping, demo script, viva cheatsheet, deviations
```

## Documentation

| Doc | What's inside |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Components, data flow, task graph, warehouse schema, reliability features |
| [`docs/syllabus_mapping.md`](docs/syllabus_mapping.md) | Course topics → exact files/features in this repo |
| [`docs/demo_script.md`](docs/demo_script.md) | 10-minute walkthrough with real outputs (V1–V8) |
| [`docs/viva_cheatsheet.md`](docs/viva_cheatsheet.md) | Likely viva questions + short answers |
| [`docs/DEVIATIONS.md`](docs/DEVIATIONS.md) | Every deviation from the build prompt (ports, PowerShell quirks, API incident…) |

## Configuration

Everything is in `.env` (created from `.env.example` by `make init`, **never committed**): Airflow admin credentials, Fernet key, DB users/passwords. The warehouse connection reaches containers through `AIRFLOW_CONN_WAREHOUSE_DB`; the dashboard reads `WAREHOUSE_URL`. No secrets live in code.

## Teardown

```powershell
docker compose down        # stop, keep data (make down)
docker compose down -v     # full reset incl. volumes (make reset)
```
