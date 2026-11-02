"""LOAD stage and monitoring writes. Uses psycopg2 through Airflow's PostgresHook (connection id: warehouse_db)."""
import logging

from psycopg2.extras import execute_values

from weather_pipeline import config

log = logging.getLogger(__name__)


def _connect():
    # Imported here so unit tests and DAG parsing do not need the Airflow provider at import time.
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=config.WAREHOUSE_CONN_ID).get_conn()


def upsert_dim_city(cities_df) -> int:
    rows = [
        (r.city, r.country, float(r.latitude), float(r.longitude), int(r.population))
        for r in cities_df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO silver.dim_city (city, country, latitude, longitude, population)
        VALUES %s
        ON CONFLICT (city) DO UPDATE SET
            country = EXCLUDED.country,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            population = EXCLUDED.population,
            loaded_at = now()
    """
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:  # `with conn` = one transaction: commit on success, rollback on error
            execute_values(cur, sql, rows)
    finally:
        conn.close()
    return len(rows)


def upsert_silver(df) -> int:
    """Idempotent load: the primary key (city, observed_at) means re-running updates rows instead of duplicating them."""
    rows = [
        (
            r.city,
            r.observed_at.to_pydatetime(),
            float(r.temperature_c),
            float(r.humidity_pct),
            float(r.precipitation_mm),
            float(r.wind_speed_kmh),
            r.source_date,
        )
        for r in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO silver.weather_hourly
            (city, observed_at, temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh, source_date)
        VALUES %s
        ON CONFLICT (city, observed_at) DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            humidity_pct = EXCLUDED.humidity_pct,
            precipitation_mm = EXCLUDED.precipitation_mm,
            wind_speed_kmh = EXCLUDED.wind_speed_kmh,
            source_date = EXCLUDED.source_date,
            ingested_at = now()
    """
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows)
    finally:
        conn.close()
    return len(rows)


def build_gold_daily(target_date: str) -> int:
    """Aggregate one day of silver data into gold.weather_daily (upsert, so re-runs are safe)."""
    sql = """
        INSERT INTO gold.weather_daily
            (city, country, obs_date, avg_temp_c, min_temp_c, max_temp_c,
             avg_humidity_pct, total_precip_mm, max_wind_kmh, hours_count, weather_category)
        SELECT
            w.city,
            c.country,
            w.observed_at::date AS obs_date,
            ROUND(AVG(w.temperature_c), 2),
            MIN(w.temperature_c),
            MAX(w.temperature_c),
            ROUND(AVG(w.humidity_pct), 2),
            SUM(w.precipitation_mm),
            MAX(w.wind_speed_kmh),
            COUNT(*),
            CASE
                WHEN SUM(w.precipitation_mm) >= 10 THEN 'Rainy'
                WHEN MAX(w.temperature_c) >= 35 THEN 'Hot'
                WHEN MIN(w.temperature_c) <= 5 THEN 'Cold'
                WHEN MAX(w.wind_speed_kmh) >= 40 THEN 'Windy'
                ELSE 'Mild'
            END
        FROM silver.weather_hourly AS w
        JOIN silver.dim_city AS c ON c.city = w.city
        WHERE w.observed_at >= %(d)s::date
          AND w.observed_at <  %(d)s::date + 1
        GROUP BY w.city, c.country, w.observed_at::date
        ON CONFLICT (city, obs_date) DO UPDATE SET
            country = EXCLUDED.country,
            avg_temp_c = EXCLUDED.avg_temp_c,
            min_temp_c = EXCLUDED.min_temp_c,
            max_temp_c = EXCLUDED.max_temp_c,
            avg_humidity_pct = EXCLUDED.avg_humidity_pct,
            total_precip_mm = EXCLUDED.total_precip_mm,
            max_wind_kmh = EXCLUDED.max_wind_kmh,
            hours_count = EXCLUDED.hours_count,
            weather_category = EXCLUDED.weather_category,
            updated_at = now()
    """
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, {"d": target_date})
            affected = cur.rowcount
    finally:
        conn.close()
    return affected


def insert_dq_results(run_id: str, target_date: str, results) -> None:
    rows = [
        (run_id, target_date, r.check_name, r.severity, r.status, r.failed_count, r.details)
        for r in results
    ]
    sql = """
        INSERT INTO dq.check_results
            (run_id, target_date, check_name, severity, status, failed_count, details)
        VALUES %s
    """
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows)
    finally:
        conn.close()


def log_stage(run_id: str, target_date: str, stage: str, rows_processed, status: str, message: str) -> None:
    sql = """
        INSERT INTO ops.pipeline_run_log (dag_run_id, target_date, stage, rows_processed, status, message)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (run_id, target_date, stage, rows_processed, status, message))
    finally:
        conn.close()


def insert_alert(dag_id: str, task_id: str, run_id: str, try_number: int, error_message: str) -> None:
    sql = """
        INSERT INTO ops.alerts (dag_id, task_id, run_id, try_number, error_message)
        VALUES (%s, %s, %s, %s, %s)
    """
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (dag_id, task_id, run_id, try_number, error_message))
    finally:
        conn.close()
