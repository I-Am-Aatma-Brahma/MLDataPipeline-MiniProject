"""DATA QUALITY stage: programmatic checks (the same idea as Soda Core / Great Expectations, written in plain pandas).

severity 'error' = blocks the pipeline when it fails; 'warn' = logged and stored but does not block.
"""
from dataclasses import dataclass

import pandas as pd

from weather_pipeline import config

REQUIRED_COLUMNS = [
    "city",
    "observed_at",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
]


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    severity: str      # "error" | "warn"
    status: str        # "pass" | "fail"
    failed_count: int
    details: str


def _make(name: str, severity: str, failed_count: int, failure_message: str) -> CheckResult:
    failed_count = int(failed_count)
    status = "pass" if failed_count == 0 else "fail"
    return CheckResult(name, severity, status, failed_count, "ok" if status == "pass" else failure_message)


def _out_of_range(series: pd.Series, low: float, high: float) -> int:
    """Count non-null values outside [low, high]."""
    return int((series.notna() & ~series.between(low, high)).sum())


def run_checks(df: pd.DataFrame, expected_cities: int, target_date: str) -> list[CheckResult]:
    results = []

    expected_rows = expected_cities * config.HOURS_PER_DAY
    row_gap = 0 if len(df) == expected_rows else abs(len(df) - expected_rows)
    results.append(_make("row_count_matches_expected", "error", row_gap,
                         f"expected {expected_rows} rows, found {len(df)}"))

    null_rows = int(df[REQUIRED_COLUMNS].isna().any(axis=1).sum())
    results.append(_make("no_nulls_in_required_columns", "error", null_rows,
                         f"{null_rows} rows contain null values"))

    duplicate_rows = int(df.duplicated(subset=["city", "observed_at"], keep="first").sum())
    results.append(_make("primary_key_is_unique", "error", duplicate_rows,
                         f"{duplicate_rows} duplicate (city, observed_at) rows"))

    bad_temp = _out_of_range(df["temperature_c"], -90, 60)
    results.append(_make("temperature_in_valid_range", "error", bad_temp,
                         f"{bad_temp} temperatures outside -90..60 C"))

    bad_humidity = _out_of_range(df["humidity_pct"], 0, 100)
    results.append(_make("humidity_in_valid_range", "error", bad_humidity,
                         f"{bad_humidity} humidity values outside 0..100 %"))

    bad_precip = _out_of_range(df["precipitation_mm"], 0, 500)
    results.append(_make("precipitation_in_valid_range", "error", bad_precip,
                         f"{bad_precip} precipitation values outside 0..500 mm"))

    bad_wind = _out_of_range(df["wind_speed_kmh"], 0, 400)
    results.append(_make("wind_speed_in_valid_range", "error", bad_wind,
                         f"{bad_wind} wind speeds outside 0..400 km/h"))

    observed = pd.to_datetime(df["observed_at"])
    wrong_day = int((observed.dt.strftime("%Y-%m-%d") != target_date).sum())
    results.append(_make("timestamps_match_target_date", "error", wrong_day,
                         f"{wrong_day} rows are not on {target_date}"))

    rows_per_city = df.groupby("city").size()
    bad_cities = int((rows_per_city != config.HOURS_PER_DAY).sum()) + max(0, expected_cities - len(rows_per_city))
    results.append(_make("every_city_has_24_hours", "error", bad_cities,
                         f"{bad_cities} cities do not have exactly {config.HOURS_PER_DAY} hourly rows"))

    extreme = int(((df["temperature_c"] > 45) | (df["temperature_c"] < -30)).sum())
    results.append(_make("extreme_temperature_warning", "warn", extreme,
                         f"{extreme} readings above 45 C or below -30 C (unusual, please review)"))

    return results


def blocking_failures(results: list[CheckResult]) -> list[CheckResult]:
    """Failed checks that must stop the pipeline (severity 'error')."""
    return [r for r in results if r.status == "fail" and r.severity == "error"]
