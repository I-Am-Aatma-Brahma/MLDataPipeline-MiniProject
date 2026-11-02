# Viva Cheatsheet — Weather Pipeline

Short answers you can give in one breath. Numbers are from the actual build (10 cities × 24 h = 240 rows/day).

## The big picture

**Q: What does this project do?**
Every day Airflow calls the Open-Meteo REST API for 10 cities, stores raw JSON (bronze), validates and converts it to a typed table (silver), runs 10 data-quality checks, loads it into Postgres with idempotent upserts, and aggregates it into a city-day gold table that a Streamlit dashboard visualises.

**Q: Why Airflow?**
It gives scheduling (`@daily`), automatic retries with backoff, timeouts, per-task logs, a UI for run history, and parameterised reruns/backfills — everything a production ETL needs, without writing a scheduler yourself.

**Q: Why Docker Compose?**
One `docker compose up -d` starts 6 services (Airflow webserver/scheduler/init, Airflow Postgres, warehouse Postgres, dashboard) with pinned images, healthchecks and named volumes — identical on any machine.

**Q: Explain medallion / bronze-silver-gold here.**
Bronze = `data/raw/date=D/*.json` exactly as the API returned it (append-only files). Silver = `silver.weather_hourly`, one cleaned typed row per city-hour, parquet on disk + table in Postgres. Gold = `gold.weather_daily`, one aggregated row per city-day. Each layer is reproducible from the one before it.

## Reliability (the marks-bearing part)

**Q: What happens when the API is down?**
Two retry layers: `requests` retries 429/5xx/connection errors 3× with backoff (1 s, 2 s, 4 s), then Airflow retries the whole task 3× with exponential backoff (20 s → 40 s → 80 s, capped at 2 min) — so ~4 real attempts. After that the task fails, downstream tasks are `upstream_failed`, and `on_failure_callback` writes a row to `ops.alerts` with the try number and exception. Verified: `extract_weather` failed with `try_number = 4` on the `api_down` demo.

**Q: 400 vs 503 — why treat them differently?**
HTTP 400 means *our* request is wrong (bad date/params); retrying can never fix it, so it raises `AirflowFailException` immediately (fail-fast). 5xx/429 are transient server issues → retried with backoff.

**Q: How is the pipeline idempotent?**
Raw JSON is written atomically (tmp file + rename) and overwrites the same path; silver and gold loads are `INSERT … ON CONFLICT (city, observed_at) DO UPDATE`, so re-running a day changes zero rows. Proven: re-running 2026-10-30 kept silver at 480 and gold at 20.

**Q: How do you rerun a specific day?**
The DAG has a `target_date` param: `airflow dags trigger weather_pipeline --conf '{"target_date": "2026-10-30"}'`. Blank = use the schedule's `ds`. Same mechanism powers `airflow dags backfill --start-date … --end-date …`.

**Q: What is `max_active_runs=1` and why?**
At most one run of the DAG at a time — prevents two days racing on the same tables/files. (During the backfill you can see the log "max_active_runs limit … has been reached - waiting".)

## Data quality

**Q: How many checks, and what does blocking mean?**
10 checks (row count, no nulls, PK uniqueness, 4 range checks — temp/humidity/precip/wind, timestamps match target date, every city has 24 hours, plus a `warn`-severity extreme-temperature check). `severity=error` + `status=fail` → the gate raises before `load_silver`, so bad data never reaches the warehouse; `warn` is recorded but never blocks.

**Q: Show me a failed gate.**
`make fault-data` (Variable `weather_inject_fault=bad_data`) injects a 999 °C value and a duplicate row during transform → gate fails 6 checks → `quality_gate` failed, `load_silver`/`build_gold` upstream_failed, silver still has 240 rows with `MAX(temperature_c)=33.4`, and an alert row exists.

**Q: Where do check results live?**
`dq.check_results` — one row per check per run, append-only, so the dashboard can show "latest run" and "failure history".

## Warehouse

**Q: Why the composite primary key `(city, observed_at)`?**
It is the natural business key of an hourly reading and is exactly what makes upserts idempotent — the same key updates the row instead of duplicating it. Gold uses `(city, obs_date)` for the same reason.

**Q: Silver and gold — why both?**
Silver is the clean, granular source of truth (hourly, 240 rows/day); gold is pre-aggregated for fast dashboards (10 rows/day). You can rebuild gold from silver any time (`build_gold_daily` does exactly that).

**Q: How does Airflow talk to the warehouse?**
Through the `warehouse_db` Airflow connection (`AIRFLOW_CONN_WAREHOUSE_DB`) → `PostgresHook`/`psycopg2` with explicit SQL. No pandas `to_sql`/`read_sql` — every load is a real `INSERT … ON CONFLICT`.

**Q: Why is `dim_city` loaded first?**
`weather_hourly.city` has a foreign key to `dim_city.city`; the DAG enforces `dim_city >> silver_rows` so the dimension always exists before facts reference it.

## Fault injection demo (V4/V5/V6)

**Q: How do you demo failure without editing code?**
An Airflow Variable `weather_inject_fault` ∈ `none | api_down | bad_data`. `api_down` swaps the API URL for `api.open-meteo.invalid` (extract fails, retries, alerts). `bad_data` corrupts the silver frame (gate blocks). Set with `make fault-api / fault-data / fault-off` — flipping it back to `none` restores normal operation, proven by the recovery run.

**Q: What does the failure alert look like?**
`ops.alerts` row: `dag_id, task_id, run_id, try_number, error_message, created_at` — e.g. `quality_gate | 1 | AirflowFailException: Data quality gate FAILED. Nothing was loaded…`. It shows up in dashboard tab 3.

## Dashboard & tests

**Q: How does the dashboard get data?**
Read-only SQLAlchemy engine (`WAREHOUSE_URL` env var, `pool_pre_ping`), 5 cached queries (20 s TTL), 3 tabs: insights (charts), data quality (latest run + history), pipeline health (counts, stage log, alerts). It never writes.

**Q: What do the 27 tests cover?**
Date resolution, cities-CSV contract, retry session config, raw-JSON shape, raw validation, transform renames/casts/cleaning, bad-data injection, every DQ check (good data passes / bad data blocks), and DAG integrity (parses, task ids, retries/backoff, no import errors). They run inside the Airflow image so deps match production.

## Numbers to quote

* 10 cities, 24 h/day → **240 silver rows per day**, **10 gold rows per day**.
* 5 days loaded in this build → **1 200 silver / 50 gold** rows total.
* 10 DQ checks per run (9 `error` + 1 `warn`).
* Retries: HTTP 3× (1/2/4 s) + Airflow 3× (20/40/80 s, cap 2 min); timeout 30 s/HTTP, 10 min/task.
* Ports: Airflow UI **8082**, warehouse **5434**, dashboard **8501** (8080/5433 were taken on the build host).
* Stack: Airflow **2.10.5** (LocalExecutor), Postgres **16**, Streamlit, Python 3.11 in containers.

## Curveballs

**Q: Why not use `pandas.read_sql` / `to_sql`?**
Explicit SQL with `ON CONFLICT` is what gives idempotency and control; `to_sql` appends/duplicates and hides the transaction. (Also the assignment required psycopg2/PostgresHook.)

**Q: What would you change for production?**
Secrets in a vault instead of `.env`, `PostgresOperator`/SqlAlchemy with pooled connections, Celery/RemoteExecutor for parallelism, real alerting (Slack/email) instead of a table, partitioned tables by month, and CI running the pytest suite on every push.

**Q: What failed during this build and how did you handle it?**
The Open-Meteo API returned HTTP 503 "The service is overloaded" for a while (rate limiting after many rapid calls) — the pipeline behaved exactly as designed: 3 HTTP retries × 4 task tries → failed run + alert row, and the next run after the cooldown succeeded. All deviations are logged in `docs/DEVIATIONS.md`.
