import json

import pytest

from weather_pipeline import config
from weather_pipeline.extract import fetch_city_weather, raw_path_for, read_cities, save_raw


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body if body is not None else {"hourly": {}}
        self.text = json.dumps(self._body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.url = None
        self.params = None

    def get(self, url, params, timeout):
        self.url, self.params = url, params
        return self.response


def test_fetch_builds_correct_request():
    session = FakeSession(FakeResponse())
    fetch_city_weather(session, "Mumbai", 19.07, 72.87, "2026-10-26")
    assert session.url == config.API_BASE_URL
    assert session.params["start_date"] == session.params["end_date"] == "2026-10-26"
    assert session.params["timezone"] == "UTC"
    assert session.params["hourly"] == ",".join(config.HOURLY_VARIABLES)


def test_fetch_http_400_raises_value_error():
    session = FakeSession(FakeResponse(status_code=400, body={"reason": "date out of range"}))
    with pytest.raises(ValueError):
        fetch_city_weather(session, "Mumbai", 19.07, 72.87, "1990-01-01")


def test_raw_path_is_partitioned_by_date_and_slugged():
    path = str(raw_path_for("2026-10-26", "New York"))
    assert path.endswith("date=2026-10-26/new_york.json")


def test_save_raw_is_idempotent(tmp_path):
    target = tmp_path / "date=2026-10-26" / "x.json"
    save_raw({"a": 1}, target)
    save_raw({"a": 2}, target)  # second write overwrites, does not duplicate
    assert json.loads(target.read_text()) == {"a": 2}
    assert len(list(target.parent.iterdir())) == 1


def test_read_cities_checks_columns(tmp_path):
    good = tmp_path / "good.csv"
    good.write_text("city,country,latitude,longitude,population\nX,Y,1.0,2.0,3\n")
    assert len(read_cities(good)) == 1

    bad = tmp_path / "bad.csv"
    bad.write_text("city,country\nX,Y\n")
    with pytest.raises(ValueError):
        read_cities(bad)
