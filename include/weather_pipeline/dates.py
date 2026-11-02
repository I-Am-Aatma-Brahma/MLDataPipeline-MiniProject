"""Resolve which calendar day a pipeline run should process."""
from datetime import datetime


def resolve_target_date(param_value: str | None, ds: str) -> str:
    """Return the YYYY-MM-DD day to process.

    `param_value` is the optional 'target_date' DAG parameter (blank = use the schedule).
    `ds` is Airflow's logical date as YYYY-MM-DD.
    """
    chosen = (param_value or "").strip() or ds
    try:
        datetime.strptime(chosen, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"target_date must be YYYY-MM-DD, got {chosen!r}") from exc
    return chosen
