"""TRANSFORM stage: raw JSON -> clean, typed, renamed pandas DataFrame (SILVER)."""
import json
from pathlib import Path

import pandas as pd

from weather_pipeline import config

COLUMN_RENAMES = {
    "time": "observed_at",
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
}
NUMERIC_COLUMNS = ["temperature_c", "humidity_pct", "precipitation_mm", "wind_speed_kmh"]
SILVER_COLUMNS = ["city", "country", "observed_at", *NUMERIC_COLUMNS, "source_date"]


def payload_to_frame(payload: dict, city: str, country: str, target_date: str) -> pd.DataFrame:
    """Flatten one city's hourly arrays into rows; rename, cast types, add city/country/source_date."""
    hourly = payload["hourly"]
    source_columns = ["time", *config.HOURLY_VARIABLES]
    df = pd.DataFrame({col: hourly[col] for col in source_columns})
    df = df.rename(columns=COLUMN_RENAMES)
    df["observed_at"] = pd.to_datetime(df["observed_at"])
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    df["city"] = city
    df["country"] = country
    df["source_date"] = pd.to_datetime(target_date).date()
    return df[SILVER_COLUMNS]


def clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning: trim city names, drop exact duplicate keys, sort deterministically."""
    df = df.copy()
    df["city"] = df["city"].str.strip()
    df = df.drop_duplicates(subset=["city", "observed_at"], keep="last")
    return df.sort_values(["city", "observed_at"]).reset_index(drop=True)


def transform_raw_files(paths: list[str], cities_df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """Read every raw JSON file and return one combined, cleaned DataFrame."""
    country_by_city = dict(zip(cities_df["city"], cities_df["country"]))
    frames = []
    for path in paths:
        payload = json.loads(Path(path).read_text())
        city = payload["_meta"]["city"]
        frames.append(payload_to_frame(payload, city, country_by_city[city], target_date))
    return clean_frame(pd.concat(frames, ignore_index=True))


def inject_bad_data(df: pd.DataFrame) -> pd.DataFrame:
    """FAULT INJECTION for the live demo: add three realistic defects that the quality gate must catch.

    1. An impossible temperature (999 C)
    2. A missing humidity value
    3. A duplicated primary key (city, observed_at)
    """
    bad = df.copy()
    bad.loc[0, "temperature_c"] = 999.0
    bad.loc[1, "humidity_pct"] = float("nan")
    duplicate_row = bad.iloc[[2]]
    return pd.concat([bad, duplicate_row], ignore_index=True)


def write_silver(df: pd.DataFrame, target_date: str, base_dir=None) -> str:
    """Write the silver parquet file (overwrites the same path on re-run, so it is idempotent)."""
    base = Path(base_dir) if base_dir else config.SILVER_DIR
    path = base / f"date={target_date}" / "weather_hourly.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return str(path)
