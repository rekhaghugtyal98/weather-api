from weather_pipeline.common.config import table_name
from weather_pipeline.silver.validation import valid_hourly_record


def valid_record():
    return {
        "location_id": "bengaluru",
        "forecast_time": "2026-10-10T10:00",
        "temperature_c": 25.0,
        "humidity_pct": 60,
        "precipitation_mm": 0.0,
        "wind_speed_kmh": 12.0,
    }


def test_valid_hourly_record_passes():
    assert valid_hourly_record(valid_record()) == (True, None)


def test_hourly_record_with_missing_humidity_fails():
    record = valid_record()
    record["humidity_pct"] = None

    valid, reason = valid_hourly_record(record)

    assert not valid
    assert "humidity_pct" in reason


def test_hourly_record_with_out_of_range_humidity_fails():
    record = valid_record()
    record["humidity_pct"] = 101

    valid, reason = valid_hourly_record(record)

    assert not valid
    assert "humidity_pct" in reason


def test_hourly_record_with_negative_precipitation_fails():
    record = valid_record()
    record["precipitation_mm"] = -0.1

    valid, reason = valid_hourly_record(record)

    assert not valid
    assert "negative" in reason


def test_silver_flattens_hourly_payload_into_expected_records(silver_output):
    context = silver_output
    rows = context["spark"].table(table_name("silver")).where(
        f"run_id = '{context['run_id']}'"
    ).orderBy("location_id", "forecast_time").collect()
    expected = context["silver_expected"]

    assert len(rows) == len(expected)
    for actual, wanted in zip(rows, expected):
        for field in (
            "location_id", "location_name", "forecast_time", "temperature_c",
            "humidity_pct", "precipitation_mm", "wind_speed_kmh",
        ):
            assert actual[field] == wanted[field]
        assert actual["forecast_date"].isoformat() == wanted["forecast_date"]


def test_silver_does_not_quarantine_valid_fixture_rows(silver_output):
    context = silver_output
    quarantine_table = table_name("quarantine")
    if not context["spark"].catalog.tableExists(quarantine_table):
        rows = 0
    else:
        rows = context["spark"].table(quarantine_table).where(
            f"run_id = '{context['run_id']}'"
        ).count()

    assert rows == 0
