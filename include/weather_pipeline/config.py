"""Central configuration: paths, API settings, names. No secrets here."""
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("WEATHER_DATA_DIR", "/opt/airflow/data"))
REFERENCE_DIR = DATA_DIR / "reference"
RAW_DIR = DATA_DIR / "raw"        # BRONZE layer
SILVER_DIR = DATA_DIR / "silver"  # SILVER layer (parquet copy)
CITIES_CSV = REFERENCE_DIR / "cities.csv"

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
BROKEN_API_BASE_URL = "https://api.open-meteo.invalid/v1/forecast"  # used by the api_down fault
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]
HTTP_TIMEOUT_SECONDS = 30
HOURS_PER_DAY = 24

WAREHOUSE_CONN_ID = "warehouse_db"  # provided by env var AIRFLOW_CONN_WAREHOUSE_DB
FAULT_VARIABLE = "weather_inject_fault"
VALID_FAULTS = ("none", "api_down", "bad_data")
