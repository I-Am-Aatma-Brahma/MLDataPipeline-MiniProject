"""
# weather_pipeline

Daily ETL: Open-Meteo REST API + cities CSV -> bronze JSON -> silver (validated) -> Postgres -> gold daily stats.

Reliability features: retries with exponential backoff, execution timeouts, idempotent upserts,
a blocking data-quality gate, failure alerts, run logging, and a `target_date` parameter for reruns/backfills.

Fault demo: set the Airflow Variable `weather_inject_fault` to `api_down` or `bad_data` (reset to `none`).
"""
import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.param import Param

from weather_pipeline import config
from weather_pipeline.alerts import notify_failure
from weather_pipeline.dates import resolve_target_date

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "student",
    "retries": 3,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}


def _current_fault() -> str:
    fault = Variable.get(config.FAULT_VARIABLE, default_var="none")
    if fault not in config.VALID_FAULTS:
        raise AirflowFailException(
            f"Variable {config.FAULT_VARIABLE}={fault!r} is invalid. Allowed: {config.VALID_FAULTS}"
        )
    return fault


@dag(
    dag_id="weather_pipeline",
    description="Daily weather ETL with data-quality gate, retries and idempotent loads",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "target_date": Param(
            "",
            type="string",
            description="Optional YYYY-MM-DD day to process. Leave blank to use the scheduled day.",
        )
    },
    tags=["weather", "etl", "mini-project"],
    doc_md=__doc__,
)
def weather_pipeline():
    @task
    def resolve_date(**context) -> str:
        target = resolve_target_date(context["params"].get("target_date"), context["ds"])
        log.info("Processing weather for target_date=%s (run_id=%s)", target, context["run_id"])
        return target

    @task
    def load_dim_city(**context) -> int:
        """Load the cities CSV into silver.dim_city (upsert)."""
        from weather_pipeline import extract, load

        count = load.upsert_dim_city(extract.read_cities())
        log.info("Upserted %s cities into silver.dim_city", count)
        return count

    @task
    def extract_weather(target_date: str, **context) -> list[str]:
        """BRONZE: call the REST API for each city and store the raw JSON."""
        from weather_pipeline import extract, load

        fault = _current_fault()
        if fault == "api_down":
            log.warning("FAULT INJECTION ACTIVE: api_down. The API URL is invalid, so this task will fail and retry.")
        try:
            paths = extract.extract_all(target_date, fault=fault)
        except ValueError as exc:  # bad request: retrying cannot fix it
            raise AirflowFailException(str(exc)) from exc
        load.log_stage(context["run_id"], target_date, "extract", len(paths), "success",
                       f"{len(paths)} raw JSON files written")
        return paths

    @task
    def validate_raw(raw_paths: list[str], target_date: str, **context) -> list[str]:
        """Schema check on the raw JSON (a lightweight data contract)."""
        from weather_pipeline import load, raw_validation

        try:
            raw_validation.validate_raw_files(raw_paths)
        except ValueError as exc:
            raise AirflowFailException(str(exc)) from exc
        load.log_stage(context["run_id"], target_date, "validate_raw", len(raw_paths), "success",
                       "raw schema valid")
        return raw_paths

    @task
    def transform_to_silver(raw_paths: list[str], target_date: str, **context) -> str:
        """SILVER: flatten, rename, cast types, clean, write parquet."""
        from weather_pipeline import extract, load, transform

        fault = _current_fault()
        df = transform.transform_raw_files(raw_paths, extract.read_cities(), target_date)
        if fault == "bad_data":
            log.warning("FAULT INJECTION ACTIVE: bad_data. Corrupting the silver data so the quality gate must fail.")
            df = transform.inject_bad_data(df)
        path = transform.write_silver(df, target_date)
        load.log_stage(context["run_id"], target_date, "transform", len(df), "success",
                       f"silver parquet written: {path}")
        return path

    @task
    def quality_gate(silver_path: str, target_date: str, **context) -> str:
        """Run data-quality checks. Any failed 'error' check stops the pipeline BEFORE anything is loaded."""
        import pandas as pd

        from weather_pipeline import extract, load, quality

        df = pd.read_parquet(silver_path)
        results = quality.run_checks(df, expected_cities=len(extract.read_cities()), target_date=target_date)
        load.insert_dq_results(context["run_id"], target_date, results)

        for r in results:
            level = logging.INFO if r.status == "pass" else (logging.ERROR if r.severity == "error" else logging.WARNING)
            log.log(level, "DQ %-32s [%s/%s] failed=%s %s", r.check_name, r.severity, r.status, r.failed_count, r.details)

        blocking = quality.blocking_failures(results)
        if blocking:
            names = ", ".join(r.check_name for r in blocking)
            load.log_stage(context["run_id"], target_date, "quality_gate", len(df), "failed",
                           f"blocked by: {names}")
            raise AirflowFailException(f"Data quality gate FAILED. Nothing was loaded. Failed checks: {names}")
        load.log_stage(context["run_id"], target_date, "quality_gate", len(df), "success",
                       f"{len(results)} checks run, no blocking failures")
        return silver_path

    @task
    def load_silver(silver_path: str, target_date: str, **context) -> int:
        """Idempotent upsert of validated hourly rows into Postgres."""
        import pandas as pd

        from weather_pipeline import load

        df = pd.read_parquet(silver_path)
        count = load.upsert_silver(df)
        load.log_stage(context["run_id"], target_date, "load_silver", count, "success",
                       f"{count} rows upserted into silver.weather_hourly")
        return count

    @task
    def build_gold(target_date: str, silver_rows: int, **context) -> int:
        """GOLD: daily aggregates per city (upsert)."""
        from weather_pipeline import load

        count = load.build_gold_daily(target_date)
        load.log_stage(context["run_id"], target_date, "build_gold", count, "success",
                       f"{count} city-day rows upserted into gold.weather_daily")
        return count

    target_date = resolve_date()
    dim_city = load_dim_city()
    raw_paths = extract_weather(target_date)
    validated = validate_raw(raw_paths, target_date)
    silver_path = transform_to_silver(validated, target_date)
    checked = quality_gate(silver_path, target_date)
    silver_rows = load_silver(checked, target_date)
    gold_rows = build_gold(target_date, silver_rows)

    dim_city >> silver_rows  # the city dimension must exist before hourly rows reference it


weather_pipeline()
