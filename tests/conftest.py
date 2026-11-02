import pandas as pd
import pytest

from weather_pipeline.transform import payload_to_frame


@pytest.fixture
def make_payload():
    """Build a fake Open-Meteo response for one city and one day."""

    def _make(day="2026-10-26", temp=25.0, humidity=60.0, precip=0.0, wind=10.0, hours=24):
        times = [f"{day}T{h:02d}:00" for h in range(hours)]
        return {
            "latitude": 1.0,
            "longitude": 2.0,
            "hourly": {
                "time": times,
                "temperature_2m": [temp] * hours,
                "relative_humidity_2m": [humidity] * hours,
                "precipitation": [precip] * hours,
                "wind_speed_10m": [wind] * hours,
            },
        }

    return _make


@pytest.fixture
def clean_day_frame(make_payload):
    """A perfect silver DataFrame: 10 cities x 24 hours = 240 rows on 2026-10-26."""
    frames = [
        payload_to_frame(make_payload("2026-10-26"), f"City{i}", "Country", "2026-10-26")
        for i in range(10)
    ]
    return pd.concat(frames, ignore_index=True)
