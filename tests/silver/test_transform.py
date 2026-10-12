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


