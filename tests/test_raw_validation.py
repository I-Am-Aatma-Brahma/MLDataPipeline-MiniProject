import json

import pytest

from weather_pipeline.raw_validation import validate_raw_files, validate_raw_payload


def test_valid_payload_has_no_errors(make_payload):
    assert validate_raw_payload(make_payload()) == []


def test_missing_hourly_object():
    assert validate_raw_payload({}) == ["missing 'hourly' object"]


def test_missing_variable_is_reported(make_payload):
    payload = make_payload()
    del payload["hourly"]["precipitation"]
    assert any("precipitation" in e for e in validate_raw_payload(payload))


def test_length_mismatch_is_reported(make_payload):
    payload = make_payload()
    payload["hourly"]["wind_speed_10m"].pop()
    assert any("lengths" in e for e in validate_raw_payload(payload))


def test_wrong_number_of_hours_is_reported(make_payload):
    assert any("24" in e for e in validate_raw_payload(make_payload(hours=12)))


def test_validate_raw_files_raises_on_bad_file(tmp_path, make_payload):
    good = tmp_path / "good.json"
    good.write_text(json.dumps(make_payload()))
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"hourly": {}}))
    validate_raw_files([str(good)])  # must not raise
    with pytest.raises(ValueError):
        validate_raw_files([str(good), str(bad)])
