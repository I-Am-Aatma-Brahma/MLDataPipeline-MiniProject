from datetime import date

import pandas as pd

from weather_pipeline.transform import (
    NUMERIC_COLUMNS,
    SILVER_COLUMNS,
    clean_frame,
    inject_bad_data,
    payload_to_frame,
    write_silver,
)


def test_payload_to_frame_columns_and_types(make_payload):
    df = payload_to_frame(make_payload(), "Mumbai", "India", "2026-10-26")
    assert list(df.columns) == SILVER_COLUMNS
    assert len(df) == 24
    assert str(df["observed_at"].dtype).startswith("datetime64")
    for col in NUMERIC_COLUMNS:
        assert df[col].dtype == "float64"
    assert df["source_date"].iloc[0] == date(2026, 10, 26)


def test_missing_values_become_nan(make_payload):
    payload = make_payload()
    payload["hourly"]["temperature_2m"][3] = None
    df = payload_to_frame(payload, "Mumbai", "India", "2026-10-26")
    assert df["temperature_c"].isna().sum() == 1


def test_clean_frame_removes_duplicates_and_sorts(clean_day_frame):
    duplicated = pd.concat([clean_day_frame, clean_day_frame.iloc[[5]]])
    cleaned = clean_frame(duplicated.sample(frac=1, random_state=1))
    assert len(cleaned) == 240
    expected_order = cleaned.sort_values(["city", "observed_at"]).reset_index(drop=True)
    assert cleaned.equals(expected_order)


def test_inject_bad_data_adds_three_defects(clean_day_frame):
    bad = inject_bad_data(clean_day_frame)
    assert len(bad) == 241
    assert bad["temperature_c"].max() == 999.0
    assert bad["humidity_pct"].isna().sum() == 1


def test_write_silver_roundtrip(tmp_path, clean_day_frame):
    path = write_silver(clean_day_frame, "2026-10-26", base_dir=tmp_path)
    assert path.endswith("date=2026-10-26/weather_hourly.parquet")
    assert len(pd.read_parquet(path)) == 240
