import pytest

from weather_pipeline.dates import resolve_target_date


def test_blank_param_uses_schedule_date():
    assert resolve_target_date("", "2026-10-26") == "2026-10-26"
    assert resolve_target_date(None, "2026-10-26") == "2026-10-26"
    assert resolve_target_date("   ", "2026-10-26") == "2026-10-26"


def test_param_overrides_schedule_date():
    assert resolve_target_date("2026-10-20", "2026-10-26") == "2026-10-20"


def test_invalid_date_raises():
    with pytest.raises(ValueError):
        resolve_target_date("26/10/2026", "2026-10-26")
