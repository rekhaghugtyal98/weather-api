from weather_pipeline.common.config import table_name
from weather_pipeline.common.schemas import detect_payload_schema_changes


def test_bronze_persists_raw_api_payload_and_location_metadata(bronze_output):
    context = bronze_output
    rows = context["spark"].table(table_name("bronze")).where(
        f"run_id = '{context['run_id']}'"
    ).collect()

    import json
    assert len(rows) == len(context["bronze_expected"])
    actual_by_id = {row["location_id"]: row.asDict() for row in rows}
    for expected in context["bronze_expected"]:
        row = actual_by_id[expected["location_id"]]
        assert row["location_name"] == expected["location_name"]
        assert row["latitude"] == expected["latitude"]
        assert row["longitude"] == expected["longitude"]
        assert json.loads(row["payload_json"]) == context["api_responses"][expected["location_id"]]


def test_bronze_records_success_in_location_audit(bronze_output):
    context = bronze_output
    rows = context["spark"].table(table_name("location_audit")).where(
        f"run_id = '{context['run_id']}'"
    ).collect()

    assert len(rows) == len(bronze_output["bronze_expected"])
    assert {row["location_id"] for row in rows} == {
        expected["location_id"] for expected in bronze_output["bronze_expected"]
    }
    assert {row["status"] for row in rows} == {"SUCCESS"}


def test_api_schema_accepts_expected_payload():
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


def test_api_schema_reports_missing_new_and_changed_fields():
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
    observed = {(change["field_path"], change["change_type"]) for change in changes}

    assert ("latitude", "type_changed") in observed
    assert ("hourly.cloud_cover", "new") in observed
    assert ("new_top_level", "new") in observed
    assert ("hourly.relative_humidity_2m", "missing") not in observed


def test_api_schema_reports_missing_field():
    payload = {
        "hourly": {
            "time": [],
            "temperature_2m": [],
            "precipitation": [],
            "wind_speed_10m": [],
        }
    }
    changes = detect_payload_schema_changes(payload)

    assert any(
        change["field_path"] == "hourly.relative_humidity_2m"
        and change["change_type"] == "missing"
        for change in changes
    )
