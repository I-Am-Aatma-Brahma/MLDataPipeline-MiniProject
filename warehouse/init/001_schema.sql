-- Runs automatically ONLY the first time the warehouse volume is created.
-- To re-run after editing: `make reset` (destroys data) then `make up`.

CREATE SCHEMA IF NOT EXISTS silver;   -- cleaned, validated hourly data + dimensions
CREATE SCHEMA IF NOT EXISTS gold;     -- business-ready aggregates
CREATE SCHEMA IF NOT EXISTS dq;       -- data-quality results
CREATE SCHEMA IF NOT EXISTS ops;      -- pipeline monitoring (run log, alerts)

CREATE TABLE IF NOT EXISTS silver.dim_city (
    city        TEXT PRIMARY KEY,
    country     TEXT NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    population  BIGINT,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.weather_hourly (
    city              TEXT NOT NULL REFERENCES silver.dim_city (city),
    observed_at       TIMESTAMP NOT NULL,            -- UTC
    temperature_c     NUMERIC(5,2) NOT NULL,
    humidity_pct      NUMERIC(5,2) NOT NULL,
    precipitation_mm  NUMERIC(6,2) NOT NULL,
    wind_speed_kmh    NUMERIC(6,2) NOT NULL,
    source_date       DATE NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, observed_at)                  -- makes upserts idempotent
);

CREATE TABLE IF NOT EXISTS gold.weather_daily (
    city              TEXT NOT NULL,
    country           TEXT NOT NULL,
    obs_date          DATE NOT NULL,
    avg_temp_c        NUMERIC(6,2) NOT NULL,
    min_temp_c        NUMERIC(6,2) NOT NULL,
    max_temp_c        NUMERIC(6,2) NOT NULL,
    avg_humidity_pct  NUMERIC(6,2) NOT NULL,
    total_precip_mm   NUMERIC(8,2) NOT NULL,
    max_wind_kmh      NUMERIC(6,2) NOT NULL,
    hours_count       INTEGER NOT NULL,
    weather_category  TEXT NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, obs_date)
);

CREATE TABLE IF NOT EXISTS dq.check_results (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT NOT NULL,
    target_date   DATE NOT NULL,
    check_name    TEXT NOT NULL,
    severity      TEXT NOT NULL,                     -- 'error' blocks the pipeline, 'warn' does not
    status        TEXT NOT NULL,                     -- 'pass' | 'fail'
    failed_count  INTEGER NOT NULL,
    details       TEXT,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
    id              BIGSERIAL PRIMARY KEY,
    dag_run_id      TEXT NOT NULL,
    target_date     DATE NOT NULL,
    stage           TEXT NOT NULL,
    rows_processed  INTEGER,
    status          TEXT NOT NULL,
    message         TEXT,
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.alerts (
    id             BIGSERIAL PRIMARY KEY,
    dag_id         TEXT NOT NULL,
    task_id        TEXT NOT NULL,
    run_id         TEXT NOT NULL,
    try_number     INTEGER,
    error_message  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
