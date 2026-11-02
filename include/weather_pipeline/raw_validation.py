"""Light schema check on raw API payloads (a simple 'data contract'): are the fields we depend on present and well shaped?

Content problems (nulls, out-of-range values) are NOT checked here; that is the job of quality.py.
"""
import json
from pathlib import Path

from weather_pipeline import config

REQUIRED_HOURLY_KEYS = ["time", *config.HOURLY_VARIABLES]


def validate_raw_payload(payload: dict) -> list[str]:
    """Return a list of human-readable problems (empty list = payload is valid)."""
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return ["missing 'hourly' object"]

    errors = []
    for key in REQUIRED_HOURLY_KEYS:
        if key not in hourly:
            errors.append(f"missing hourly key '{key}'")
    if errors:
        return errors

    lengths = {key: len(hourly[key]) for key in REQUIRED_HOURLY_KEYS}
    if len(set(lengths.values())) != 1:
        errors.append(f"hourly arrays have different lengths: {lengths}")
    elif lengths["time"] != config.HOURS_PER_DAY:
        errors.append(f"expected {config.HOURS_PER_DAY} hourly values, found {lengths['time']}")
    return errors


def validate_raw_files(paths: list[str]) -> None:
    """Validate every raw file. Raises ValueError listing all problems if any file is invalid."""
    problems = {}
    for path in paths:
        payload = json.loads(Path(path).read_text())
        errors = validate_raw_payload(payload)
        if errors:
            problems[path] = errors
    if problems:
        raise ValueError(f"Raw schema validation failed: {problems}")
