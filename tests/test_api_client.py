from unittest.mock import Mock, patch

import pytest
import requests

from weather_pipeline.common.api_client import _invalid_hourly_rows, fetch_forecast
from weather_pipeline.common.config import ALL_LOCATIONS


def test_invalid_hourly_rows_detects_empty_and_mismatched_arrays():
    assert "missing/empty hourly time array" in _invalid_hourly_rows({"time": []})
    problems = _invalid_hourly_rows({"time": ["2026-10-10T00:00"], "temperature_2m": []})
    assert any("array lengths differ" in issue for issue in problems)


def test_invalid_hourly_rows_detects_null_and_out_of_range_values():
    issues = _invalid_hourly_rows({
        "time": ["2026-10-10T00:00", "2026-10-10T01:00"],
        "temperature_2m": [None, 20],
        "relative_humidity_2m": [50, 101],
        "precipitation": [0, 0],
        "wind_speed_10m": [0, 0],
    })
    assert any("temperature_2m is null" in issue for issue in issues)
    assert any("relative_humidity_2m=101.0 outside valid range" in issue for issue in issues)


def test_invalid_hourly_rows_detects_duplicate_timestamps():
    issues = _invalid_hourly_rows({
        "time": ["2026-10-10T00:00", "2026-10-10T00:00"],
        "temperature_2m": [20, 21], "relative_humidity_2m": [50, 50],
        "precipitation": [0, 0], "wind_speed_10m": [0, 0],
    })
    assert any("duplicate timestamp" in issue for issue in issues)


def test_fetch_forecast_retries_transient_api_failure_then_succeeds():
    session = Mock()
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"hourly": {
        "time": ["2026-10-10T00:00"], "temperature_2m": [20],
        "relative_humidity_2m": [50], "precipitation": [0], "wind_speed_10m": [3],
    }}
    session.get.side_effect = [requests.Timeout("slow"), response]
    with patch("weather_pipeline.common.api_client.time.sleep"):
        result = fetch_forecast(session, ALL_LOCATIONS[0])
    assert result["hourly"]["temperature_2m"] == [20]
    assert session.get.call_count == 2


def test_fetch_forecast_requests_one_location_and_returns_its_response():
    location = ALL_LOCATIONS[0]
    payload = {
        "hourly": {
            "time": ["2026-10-10T00:00"],
            "temperature_2m": [20],
            "relative_humidity_2m": [50],
            "precipitation": [0],
            "wind_speed_10m": [3],
        }
    }
    session = Mock()
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = payload
    session.get.return_value = response

    result = fetch_forecast(session, location)

    assert result == payload
    session.get.assert_called_once()
    params = session.get.call_args.kwargs["params"]
    assert params["latitude"] == location.latitude
    assert params["longitude"] == location.longitude


def test_fetch_forecast_raises_after_three_failed_attempts():
    session = Mock()
    session.get.side_effect = requests.ConnectionError("unreachable")
    with patch("weather_pipeline.common.api_client.time.sleep"):
        with pytest.raises(RuntimeError, match="failed after 3 API attempts"):
            fetch_forecast(session, ALL_LOCATIONS[0])
    assert session.get.call_count == 3


def test_fetch_forecast_returns_malformed_object_for_quarantine_and_schema_audit():
    session = Mock()
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"latitude": 12.9, "unexpected": "new"}
    session.get.return_value = response
    with patch("weather_pipeline.common.api_client.time.sleep"):
        result = fetch_forecast(session, ALL_LOCATIONS[0])
    assert result["unexpected"] == "new"
    assert "_pipeline_quality_warning" in result
    assert session.get.call_count == 3
