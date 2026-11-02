from weather_pipeline.quality import blocking_failures, run_checks
from weather_pipeline.transform import inject_bad_data

DAY = "2026-10-26"


def test_clean_data_passes_all_blocking_checks(clean_day_frame):
    results = run_checks(clean_day_frame, expected_cities=10, target_date=DAY)
    assert len(results) == 10
    assert blocking_failures(results) == []


def test_injected_bad_data_is_caught(clean_day_frame):
    bad = inject_bad_data(clean_day_frame)
    failed = {r.check_name for r in blocking_failures(run_checks(bad, expected_cities=10, target_date=DAY))}
    expected = {
        "row_count_matches_expected",
        "no_nulls_in_required_columns",
        "primary_key_is_unique",
        "temperature_in_valid_range",
        "every_city_has_24_hours",
    }
    assert expected <= failed


def test_missing_city_is_caught(clean_day_frame):
    missing = clean_day_frame[clean_day_frame["city"] != "City0"]
    failed = {r.check_name for r in blocking_failures(run_checks(missing, expected_cities=10, target_date=DAY))}
    assert "row_count_matches_expected" in failed
    assert "every_city_has_24_hours" in failed


def test_wrong_day_is_caught(clean_day_frame):
    failed = {r.check_name for r in blocking_failures(run_checks(clean_day_frame, 10, "2026-10-27"))}
    assert "timestamps_match_target_date" in failed


def test_warning_check_does_not_block(clean_day_frame):
    frame = clean_day_frame.copy()
    frame.loc[0, "temperature_c"] = 46.0  # unusual but physically possible
    results = run_checks(frame, expected_cities=10, target_date=DAY)
    warn = [r for r in results if r.check_name == "extreme_temperature_warning"][0]
    assert warn.status == "fail" and warn.severity == "warn"
    assert blocking_failures(results) == []
