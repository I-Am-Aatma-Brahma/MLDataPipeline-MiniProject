# Demo Script — Weather Pipeline

A 10-minute walkthrough with the commands as actually run on this machine (Windows PowerShell, Docker Desktop, no host `make`). D = a date 3 days before today; in this build **D = 2026-10-30**, backfill window **S = 2026-10-25 → E = 2026-10-27**.

> No `make` on this host? Every `make X` below has a raw equivalent in the "no make?" table in `README.md`. The commands shown here are the raw ones.

## 0. Start everything (≈2 min first time)

```powershell
# equivalent of `make up`
docker compose up -d --build
docker compose ps
```

Expected: `airflow-db`, `warehouse` become `healthy`; `airflow-init` exits 0; `airflow-webserver`, `airflow-scheduler` become `healthy`; `dashboard` is `Up`.

```powershell
curl.exe -fsS http://localhost:8082/health     # Airflow UI -> {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"} ...}
curl.exe -fsS http://localhost:8501/_stcore/health   # Streamlit -> ok
```

Open **http://localhost:8082** (admin/admin) and **http://localhost:8501**.

## 1. V1 — sanity: DAG parses, tests pass

```powershell
docker compose exec -T airflow-scheduler airflow dags list-import-errors
# No data found

docker compose run --rm --no-deps airflow-scheduler python -m pytest /opt/airflow/tests -q -p no:cacheprovider
# 27 passed in ...
```

## 2. V2 — happy-path run for D

```powershell
# equivalent of `make trigger DATE=2026-10-30`
docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
docker compose exec -T airflow-scheduler airflow dags list-runs -d weather_pipeline
# ... manual__2026-11-02T15:54:15+00:00   success   ...
```

```powershell
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT COUNT(*) FROM silver.weather_hourly WHERE observed_at::date='2026-10-30';"
# 240        (10 cities x 24 hours)
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT COUNT(*) FROM gold.weather_daily WHERE obs_date='2026-10-30';"
# 10         (one row per city)
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT check_name, status FROM dq.check_results WHERE target_date='2026-10-30' ORDER BY id DESC LIMIT 10;"
# 10 rows, all status = pass
```

Files: `data/raw/date=2026-10-30/` has **10 JSON files** (bengaluru.json … tokyo.json) and `data/silver/date=2026-10-30/weather_hourly.parquet` exists.

## 3. V3 — idempotency (re-run the same day)

```powershell
# counts before -> trigger the same DATE again -> counts after
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT 'silver' t, COUNT(*) FROM silver.weather_hourly UNION ALL SELECT 'gold', COUNT(*) FROM gold.weather_daily UNION ALL SELECT 'dq', COUNT(*) FROM dq.check_results;"
# before: silver 480, gold 20, dq 20
# after : silver 480, gold 20, dq 30      <-- silver/gold unchanged, dq history grows by 10
```

Re-running a day overwrites the raw JSON, re-writes the same parquet, and upserts the same warehouse rows: **no duplicates, no double counts.**

## 4. V4 — bad data must be blocked (fault = `bad_data`)

```powershell
docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault bad_data
# equivalent of `make fault-data`
docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
```

Expected task states (`airflow tasks states-for-dag-run …`):

| task | state |
|---|---|
| resolve_date, load_dim_city, extract_weather, validate_raw, transform_to_silver | success |
| **quality_gate** | **failed** |
| load_silver, build_gold | upstream_failed |

Warehouse proof that nothing bad was loaded:

```powershell
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT COUNT(*), MAX(temperature_c) FROM silver.weather_hourly WHERE observed_at::date='2026-10-30';"
# 240 | 33.4      (the injected 999 C never reached silver)
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT check_name, status FROM dq.check_results WHERE status='fail' ORDER BY id DESC;"
# 6 rows: row_count_matches_expected, no_nulls_in_required_columns, primary_key_is_unique,
#         temperature_in_valid_range, every_city_has_24_hours, extreme_temperature_warning
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT task_id, try_number, left(error_message,60) FROM ops.alerts ORDER BY id DESC LIMIT 2;"
# quality_gate | 1 | AirflowFailException: Data quality gate FAILED. Nothing was loaded...
```

Then turn it off: `airflow variables set weather_inject_fault none` (`make fault-off`).

## 5. V5 — API down must retry, then fail loudly (fault = `api_down`)

```powershell
docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault api_down   # make fault-api
docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
```

Expected: `extract_weather` retries with exponential backoff (tries 1→4: 20 s, 40 s, 80 s) and finally **fails**; everything downstream is `upstream_failed`; an alert row appears with the try number:

```
weather_pipeline | extract_weather | ... | try_number 4 | ConnectionError: HTTPSConnectionPool(host='api.open-meteo.invalid'...
```

Counts stay untouched (silver 480 / gold 20). Then `make fault-off`.

## 6. V6 — recovery

```powershell
docker compose exec -T airflow-scheduler airflow variables set weather_inject_fault none
docker compose exec -T airflow-scheduler airflow dags trigger weather_pipeline --conf '{\"target_date\": \"2026-10-30\"}'
# run ...15:54:15+00:00 -> success  (240 silver / 10 gold / 10 of 10 checks pass for D)
```

## 7. V7 — backfill 3 days

```powershell
# equivalent of `make backfill START=2026-10-25 END=2026-10-27`
docker compose exec -T airflow-scheduler airflow dags backfill weather_pipeline --start-date 2026-10-25 --end-date 2026-10-27 --reset-dagruns --yes
```

```
[backfill progress] | finished run 3 of 3 | tasks waiting: 0 | succeeded: 24 | failed: 0 ...
Backfill done for DAG <DAG: weather_pipeline>.
```

```powershell
docker compose exec -T warehouse psql -U warehouse -d weather_dw -c "SELECT observed_at::date d, COUNT(*) FROM silver.weather_hourly WHERE observed_at::date BETWEEN '2026-10-25' AND '2026-10-27' GROUP BY 1 ORDER BY 1;"
# 2026-10-25 | 240 / 2026-10-26 | 240 / 2026-10-27 | 240     (720 rows total)
```

## 8. V8 — dashboard

```powershell
curl.exe -fsS http://localhost:8501/_stcore/health   # ok
docker compose logs dashboard --tail 25              # no Traceback / Exception lines
```

Every dashboard query also run by hand in psql (all return rows):

```sql
SELECT * FROM gold.weather_daily ORDER BY obs_date, city LIMIT 3;
SELECT run_id, target_date, check_name, severity, status, failed_count, details, checked_at FROM dq.check_results ORDER BY checked_at DESC, id DESC LIMIT 3;
SELECT logged_at, dag_run_id, target_date, stage, rows_processed, status, message FROM ops.pipeline_run_log ORDER BY logged_at DESC, id DESC LIMIT 3;
SELECT created_at, task_id, run_id, try_number, error_message FROM ops.alerts ORDER BY created_at DESC LIMIT 3;
SELECT (SELECT COUNT(*) FROM silver.weather_hourly) silver_rows, (SELECT COUNT(*) FROM gold.weather_daily) gold_rows, (SELECT COUNT(*) FROM silver.dim_city) cities;
```

Browse http://localhost:8501 — three tabs: 🌡️ Weather insights (charts per city), ✅ Data quality (latest run's 10 checks + failure history), 🛠️ Pipeline health (row counts, `ops.pipeline_run_log`, `ops.alerts`).

## 9. Tests & wrap-up

```powershell
docker compose run --rm --no-deps airflow-scheduler python -m pytest /opt/airflow/tests -q -p no:cacheprovider   # 27 passed
docker compose exec -T airflow-scheduler airflow variables get weather_inject_fault   # none
docker compose exec -T airflow-scheduler airflow dags list-runs -d weather_pipeline    # latest run: success
```

Stop with `docker compose down` (add `-v` for a full reset, i.e. `make reset`).
