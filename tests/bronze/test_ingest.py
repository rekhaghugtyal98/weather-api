from unittest.mock import MagicMock
import logging

import pytest

from weather_pipeline.bronze import ingest
from weather_pipeline.common.config import ALL_LOCATIONS
from weather_pipeline.common.schemas import detect_payload_schema_changes


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


def test_ensure_schemas_creates_data_and_audit_schemas(monkeypatch):
    spark = MagicMock()
    monkeypatch.setenv("WEATHER_CATALOG", "test_catalog")
    monkeypatch.setenv("WEATHER_SCHEMA", "weather_data")
    monkeypatch.setenv("WEATHER_AUDIT_SCHEMA", "weather_audit")

    ingest._ensure_schemas(spark)

    assert spark.sql.call_args_list[0].args[0] == (
        "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`weather_data`"
    )
    assert spark.sql.call_args_list[1].args[0] == (
        "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`weather_audit`"
    )


def test_initialize_metrics_sets_counts_and_city_lists():
    metrics = {"previous": "value"}

    ingest._initialize_metrics(metrics, ALL_LOCATIONS[:2], ["unknown"])

    assert metrics == {
        "previous": "value",
        "input_row_count": 3,
        "output_row_count": 0,
        "rejected_row_count": 1,
        "successful_cities": [],
        "failed_cities": ["unknown: unknown city ID; skipped"],
    }


def test_initialize_metrics_accepts_no_metrics():
    assert ingest._initialize_metrics(None, ALL_LOCATIONS[:1], []) is None


def test_record_skipped_locations_creates_audit_rows():
    rows = ingest._record_skipped_locations("run-1", ["Atlantis"])

    assert len(rows) == 1
    run_id, location_id, name, status, message, event_time = rows[0]
    assert (run_id, location_id, name, status) == (
        "run-1", "Atlantis", "Atlantis", "SKIPPED"
    )
    assert "Unknown city ID" in message
    assert "bengaluru" in message
    assert event_time


def test_ingest_location_appends_payload_and_success_audit(monkeypatch):
    location = ALL_LOCATIONS[0]
    payload = {"hourly": {"time": ["2026-10-10T10:00"]}}
    monkeypatch.setattr(ingest, "fetch_forecast", lambda _session, _location: payload)
    monkeypatch.setattr(ingest, "detect_payload_schema_changes", lambda _payload: [
        {
            "field_path": "hourly.new_field",
            "change_type": "new",
            "expected_type": None,
            "observed_type": "array",
        }
    ])
    raw_rows, audit_rows, schema_rows, failures = [], [], [], []
    metrics = {"successful_cities": [], "failed_cities": []}

    ingest._ingest_location(
        MagicMock(), "run-2", location, raw_rows, audit_rows,
        schema_rows, failures, metrics,
    )

    assert len(raw_rows) == 1
    assert raw_rows[0][:5] == (
        "run-2", location.location_id, location.name,
        str(location.latitude), str(location.longitude),
    )
    import json
    assert json.loads(raw_rows[0][6]) == payload
    assert audit_rows[0][:5] == (
        "run-2", location.location_id, location.name, "SUCCESS", None,
    )
    assert schema_rows[0][:6] == (
        "run-2", location.location_id, "hourly.new_field", "new", None, "array",
    )
    assert metrics["successful_cities"] == [location.name]
    assert failures == []


def test_ingest_location_records_fetch_failure(monkeypatch):
    location = ALL_LOCATIONS[0]

    def fail_fetch(_session, _location):
        raise OSError("service unavailable")

    monkeypatch.setattr(ingest, "fetch_forecast", fail_fetch)
    raw_rows, audit_rows, schema_rows, failures = [], [], [], []
    metrics = {"successful_cities": [], "failed_cities": []}

    ingest._ingest_location(
        MagicMock(), "run-3", location, raw_rows, audit_rows,
        schema_rows, failures, metrics,
    )

    assert raw_rows == []
    assert schema_rows == []
    assert audit_rows[0][:4] == (
        "run-3", location.location_id, location.name, "FAILED",
    )
    assert "OSError: service unavailable" in audit_rows[0][4]
    assert failures == [f"{location.name} ({location.location_id}): OSError: service unavailable"]
    assert metrics["failed_cities"] == [f"{location.name}: OSError: service unavailable"]


def test_ingest_locations_calls_single_location_method_for_each_city(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ingest, "_ingest_location", lambda *args: calls.append(args)
    )
    locations = ALL_LOCATIONS[:2]
    shared = ([], [], [], [], None)

    ingest._ingest_locations(MagicMock(), "run-4", locations, *shared)

    assert len(calls) == 2
    assert [call[2] for call in calls] == list(locations)
    assert all(call[1] == "run-4" for call in calls)


def test_log_ingestion_counts_updates_metrics(caplog):
    metrics = {"output_row_count": 0, "rejected_row_count": 0}
    caplog.set_level(logging.INFO, logger=ingest.logger.name)

    ingest._log_ingestion_counts(
        "run-5", ALL_LOCATIONS[:2], ["unknown"], [("raw",)],
        ["failed city"], [("schema change",)], metrics,
    )

    assert metrics["output_row_count"] == 1
    assert metrics["rejected_row_count"] == 2
    assert "attempted=3 ingested=1 failed=1 skipped=1 schema_changes=1" in caplog.text


def test_write_outputs_writes_nonempty_datasets(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr(ingest, "write_data", MagicMock())
    raw, audit = [("raw",)], [("audit",)]
    schema = [("run-1", "bengaluru", "hourly.new", "new", None, "array", "2026-10-10T10:00:00Z")]

    ingest._write_outputs(spark, raw, audit, schema)

    assert spark.createDataFrame.call_count == 3
    assert ingest.write_data.call_count == 3
    assert [call.args[1] for call in ingest.write_data.call_args_list] == [
        "delta", "delta", "delta",
    ]


def test_write_outputs_skips_empty_datasets(monkeypatch):
    spark = MagicMock()
    write_data = MagicMock()
    monkeypatch.setattr(ingest, "write_data", write_data)

    ingest._write_outputs(spark, [], [], [])

    spark.createDataFrame.assert_not_called()
    write_data.assert_not_called()


def test_send_warning_summary_sends_failures_and_skips(monkeypatch):
    send_summary = MagicMock()
    monkeypatch.setattr(ingest, "send_failure_summary", send_summary)

    ingest._send_warning_summary("run-6", ["Delhi failed"], ["unknown"])

    subject, body = send_summary.call_args.args
    assert subject == "Open-Meteo pipeline: city ingestion warnings"
    assert "Delhi failed" in body
    assert "unknown: unknown city ID; skipped" in body


def test_send_warning_summary_does_nothing_without_issues(monkeypatch):
    send_summary = MagicMock()
    monkeypatch.setattr(ingest, "send_failure_summary", send_summary)

    ingest._send_warning_summary("run-7", [], [])

    send_summary.assert_not_called()


def test_raise_if_no_payloads_returns_when_payload_exists(monkeypatch):
    send_summary = MagicMock()
    monkeypatch.setattr(ingest, "send_failure_summary", send_summary)

    assert ingest._raise_if_no_payloads("run-8", [("payload",)], [], []) is None
    send_summary.assert_not_called()


def test_raise_if_no_payloads_sends_summary_and_raises(monkeypatch):
    send_summary = MagicMock()
    monkeypatch.setattr(ingest, "send_failure_summary", send_summary)

    with pytest.raises(RuntimeError, match="No city payloads ingested"):
        ingest._raise_if_no_payloads("run-9", [], ["Delhi failed"], ["unknown"])

    send_summary.assert_called_once()
    assert "Delhi failed" in send_summary.call_args.args[1]
    assert "unknown: unknown city ID; skipped" in send_summary.call_args.args[1]
