"""Failure alerting. Called by Airflow when a task fails for good (after all retries are used up)."""
import logging

log = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    """Log a loud ERROR line and store the failure in ops.alerts. Never raises (an alert must not hide the real error)."""
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    message = f"{type(exception).__name__}: {exception}" if exception else "unknown error"
    task_id = getattr(ti, "task_id", "unknown")
    dag_id = getattr(ti, "dag_id", "weather_pipeline")
    run_id = getattr(dag_run, "run_id", "unknown")
    try_number = int(getattr(ti, "try_number", 0) or 0)

    log.error("PIPELINE ALERT | dag=%s task=%s run=%s try=%s | %s", dag_id, task_id, run_id, try_number, message)
    try:
        from weather_pipeline import load

        load.insert_alert(dag_id, task_id, run_id, try_number, message[:2000])
    except Exception:  # noqa: BLE001
        log.exception("Could not persist the alert to ops.alerts")
