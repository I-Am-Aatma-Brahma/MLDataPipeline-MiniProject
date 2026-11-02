"""EXTRACT stage: read the cities CSV and pull hourly weather JSON from the Open-Meteo REST API."""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from weather_pipeline import config

log = logging.getLogger(__name__)

REQUIRED_CITY_COLUMNS = ["city", "country", "latitude", "longitude", "population"]


def read_cities(csv_path=None) -> pd.DataFrame:
    """Read the reference CSV (source #1) and check it has the expected columns."""
    path = csv_path or config.CITIES_CSV
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_CITY_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"cities CSV is missing columns: {missing}")
    return df


def build_session() -> requests.Session:
    """HTTP session that retries transient errors (429/5xx and connection errors) with backoff."""
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_city_weather(session, city, latitude, longitude, target_date, base_url=None) -> dict:
    """Call the REST API (source #2) for one city and one day. Returns the parsed JSON."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(config.HOURLY_VARIABLES),
        "start_date": target_date,
        "end_date": target_date,
        "timezone": "UTC",
    }
    response = session.get(base_url or config.API_BASE_URL, params=params, timeout=config.HTTP_TIMEOUT_SECONDS)
    if response.status_code == 400:
        # A 400 means our request is wrong (e.g. date outside the supported range). Retrying cannot help.
        raise ValueError(f"Open-Meteo rejected the request for {city} (HTTP 400): {response.text[:300]}")
    response.raise_for_status()
    return response.json()


def raw_path_for(target_date: str, city: str) -> Path:
    """Where the raw (bronze) JSON for one city/day lives."""
    safe_name = city.lower().replace(" ", "_")
    return config.RAW_DIR / f"date={target_date}" / f"{safe_name}.json"


def save_raw(payload: dict, path: Path) -> None:
    """Write JSON atomically. Re-running overwrites the same file, so extract is idempotent."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload))
    tmp_path.replace(path)


def extract_all(target_date: str, fault: str = "none") -> list[str]:
    """Fetch every city for `target_date`, save raw JSON, and return the list of file paths."""
    cities = read_cities()
    base_url = config.BROKEN_API_BASE_URL if fault == "api_down" else config.API_BASE_URL
    session = build_session()
    paths = []
    for row in cities.itertuples(index=False):
        payload = fetch_city_weather(
            session, row.city, float(row.latitude), float(row.longitude), target_date, base_url=base_url
        )
        payload["_meta"] = {
            "city": row.city,
            "target_date": target_date,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        path = raw_path_for(target_date, row.city)
        save_raw(payload, path)
        paths.append(str(path))
        log.info("Saved raw weather for %s -> %s", row.city, path)
    return paths
