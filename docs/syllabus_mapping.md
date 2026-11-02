# Syllabus Mapping — Weather Pipeline Mini Project

How each concept taught in the course shows up concretely in this repository. (Topic names follow the usual Sem-5 course headings; the column on the right points to the exact file/artefact you can open during the viva.)

## 1. Python programming & data handling

| Topic | Where it is used | File |
|---|---|---|
| File I/O, JSON | Raw API payloads written/read as JSON, atomic write via tmp+rename | `include/weather_pipeline/extract.py` (`save_raw`) |
| CSV handling | Cities reference file read with pandas, column-contract checked | `extract.py` (`read_cities`) |
| pandas DataFrames | Flattening nested JSON → typed tabular silver data, aggregations | `include/weather_pipeline/transform.py` |
| Type hints, dataclasses | `CheckResult` frozen dataclass, function signatures | `include/weather_pipeline/quality.py` |
| Exceptions & control flow | `ValueError` (permanent error) vs transient errors; custom failure messages | `extract.py`, `dags/weather_pipeline_dag.py` |
| Unit testing (pytest) | 27 tests: dates, extract, validation, transform, quality, DAG integrity | `tests/` |

## 2. Databases / DBMS

| Topic | Where it is used | File |
|---|---|---|
| Schema design | 4 schemas (`silver`, `gold`, `dq`, `ops`), 6 tables | `warehouse/init/001_schema.sql` |
| Normalisation | Cities normalised into `dim_city`; facts reference it by FK | `001_schema.sql` |
| Primary keys / composite PK | `(city, observed_at)` and `(city, obs_date)` — the key to idempotency | `001_schema.sql` |
| Referential integrity | `weather_hourly.city → dim_city.city` | `001_schema.sql` |
| UPSERT | `INSERT … ON CONFLICT … DO UPDATE` for every load | `include/weather_pipeline/load.py` |
| SQL aggregation | Gold layer = `GROUP BY` daily aggregates written from Python | `load.py` (`build_gold_daily`) |
| Star schema / warehouse layers | bronze (files) → silver (hourly fact) → gold (city-day aggregate) | `docs/architecture.md` |
| Running ad-hoc SQL | psql one-liners for every verification check | `Makefile` (`counts`, `psql`) |

## 3. Operating systems / system programming

| Topic | Where it is used | File |
|---|---|---|
| Processes & services | 6 long-running containers + one-shot init process | `docker-compose.yml` |
| Environment variables & secrets | `.env` → container env; secrets never committed (`.gitignore`) | `.env.example`, `docker-compose.yml` |
| File permissions / UID | `user: "${AIRFLOW_UID:-50000}:0"` so mounted logs are writable | `docker-compose.yml` |
| Health checks & restarts | `pg_isready`, `/health` endpoint, `restart: unless-stopped` | `docker-compose.yml` |
| Partitioned file system layout | `data/raw/date=YYYY-MM-DD/`, `data/silver/date=YYYY-MM-DD/` | `include/weather_pipeline/config.py` |

## 4. Computer networks

| Topic | Where it is used | File |
|---|---|---|
| REST APIs & HTTP | GET with query params to `api.open-meteo.com`, JSON responses | `extract.py` |
| Status codes & semantics | 400 = do not retry (client bug), 429/5xx = retry with backoff | `extract.py` (`fetch_city_weather`) |
| Timeouts | 30 s per request, 10 min per task | `config.py`, DAG `DEFAULT_ARGS` |
| Ports & port mapping | 8082→8080 (Airflow), 5434→5432 (Postgres), 8501 (Streamlit) | `docker-compose.yml` |
| Connection pooling | SQLAlchemy `pool_pre_ping` in the dashboard | `dashboard/app.py` |

## 5. Software engineering & testing

| Topic | Where it is used | File |
|---|---|---|
| Version control | Small conventional commits (`feat:`, `fix:`, `test:`, `docs:`), no secrets/data in git | `git log` |
| CI-style test suite | One command runs all 27 tests inside the production image | `Makefile` (`test`) |
| Configuration management | Central `config.py`, no hard-coded paths/URLs in logic | `include/weather_pipeline/config.py` |
| Documentation | architecture, demo script, viva cheatsheet, deviations, README | `docs/`, `README.md` |
| Fault injection for testing | `api_down` / `bad_data` toggles without touching code | `Makefile` (`fault-*`) |

## 6. Data engineering / ML pipeline concepts

| Topic | Where it is used | File |
|---|---|---|
| ETL / ELT stages | extract → validate → transform → quality-gate → load | `dags/weather_pipeline_dag.py` |
| Medallion architecture (bronze/silver/gold) | raw JSON → validated parquet → warehouse aggregates | `docs/architecture.md` |
| Orchestration & scheduling | Airflow `@daily` DAG, `catchup=False`, `max_active_runs=1` | `dags/weather_pipeline_dag.py` |
| Idempotent, reproducible reruns | `target_date` parameter; reruns and backfills produce identical rows | `dates.py`, `load.py` |
| Data quality engineering | 10 checks with `error`/`warn` severity, blocking gate before load | `include/weather_pipeline/quality.py` |
| Observability & alerting | `ops.pipeline_run_log`, `ops.alerts`, dashboard health tab | `load.py`, `alerts.py` |
| Retry theory (exponential backoff) | HTTP retries + Airflow retries: 20 s → 40 s → 80 s, cap 2 min | DAG `DEFAULT_ARGS`, `extract.py` |
| Batch backfill | `airflow dags backfill --start-date … --end-date …` | `Makefile` (`backfill`) |
| Monitoring dashboard | Streamlit: insights / data quality / pipeline health tabs | `dashboard/app.py` |
