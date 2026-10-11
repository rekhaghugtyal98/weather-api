from weather_pipeline.silver.validation import valid_hourly_record
from weather_pipeline.common.schemas import detect_payload_schema_changes

def valid_record():
    return {
        "location_id": "bengaluru",
        "forecast_time": "2026-10-10T10:00",
        "temperature_c": 25.0,
        "humidity_pct": 60,
        "precipitation_mm": 0.0,
        "wind_speed_kmh": 12.0,
    }

def test_valid_record_passes():
    assert valid_hourly_record(valid_record()) == (True, None)

def test_missing_humidity_fails():
    record = valid_record()
    record["humidity_pct"] = None
    valid, reason = valid_hourly_record(record)
    assert not valid
    assert "humidity_pct" in reason

def test_humidity_out_of_range_fails():
    record = valid_record()
    record["humidity_pct"] = 101
    valid, reason = valid_hourly_record(record)
    assert not valid
    assert "humidity_pct" in reason

def test_negative_precipitation_fails():
    record = valid_record()
    record["precipitation_mm"] = -0.1
    valid, reason = valid_hourly_record(record)
    assert not valid
    assert "negative" in reason


def test_payload_schema_accepts_expected_shape():
    payload = {
        "latitude": 12.9,
        "longitude": 77.5,
        "timezone": "UTC",
        "hourly": {
            "time": ["2026-10-10T10:00"],
            "temperature_2m": [25],
            "relative_humidity_2m": [60],
            "precipitation": [0],
            "wind_speed_10m": [12],
        },
    }
    assert detect_payload_schema_changes(payload) == []


def test_payload_schema_reports_missing_new_and_changed_fields():
    payload = {
        "latitude": "12.9",
        "longitude": 77.5,
        "timezone": "UTC",
        "hourly": {
            "time": ["2026-10-10T10:00"],
            "temperature_2m": [25],
            "relative_humidity_2m": [60],
            "precipitation": [0],
            "wind_speed_10m": [12],
            "cloud_cover": [20],
        },
        "new_top_level": True,
    }
    changes = detect_payload_schema_changes(payload)
    observed = {(x["field_path"], x["change_type"]) for x in changes}
    assert ("latitude", "type_changed") in observed
    assert ("hourly.cloud_cover", "new") in observed
    assert ("new_top_level", "new") in observed
    assert ("hourly.relative_humidity_2m", "missing") not in observed


def test_payload_schema_reports_missing_field():
    payload = {"hourly": {"time": [], "temperature_2m": [], "precipitation": [], "wind_speed_10m": []}}
    changes = detect_payload_schema_changes(payload)
    assert any(x["field_path"] == "hourly.relative_humidity_2m" and x["change_type"] == "missing" for x in changes)
