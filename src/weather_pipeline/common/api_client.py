"""HTTP client for Open-Meteo with bounded retries for API and data-quality errors."""
import time
import requests
from .config import API_URL, FORECAST_DAYS, HOURLY_VARIABLES, requested_date_range

def _invalid_hourly_rows(hourly: dict) -> list[str]:
    """Collect data-quality issues found in an Open-Meteo hourly response section.

    Checks required arrays, matching array lengths, timestamps, duplicate times,
    numeric values, and accepted measurement ranges.

    Args:
        hourly: The response's ``hourly`` object, or an empty dictionary if absent.

    Returns:
        A list of issue descriptions; an empty list means the data passed checks.
    """
    fields = {
        "temperature_2m": (-90, 65),
        "relative_humidity_2m": (0, 100),
        "precipitation": (0, float("inf")),
        "wind_speed_10m": (0, float("inf")),
    }
    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        return ["missing/empty hourly time array"]
    lengths = {"time": len(times)}
    missing = []
    for field in fields:
        values = hourly.get(field)
        if not isinstance(values, list):
            missing.append(f"missing hourly array: {field}")
        else:
            lengths[field] = len(values)
    issues = list(missing)
    if len(set(lengths.values())) != 1:
        issues.append(f"hourly array lengths differ: {lengths}")
    if issues:
        return issues

    issues = []
    seen_timestamps = set()
    for index, timestamp in enumerate(times):
        if not timestamp:
            issues.append(f"row {index}: missing timestamp")
            continue
        if timestamp in seen_timestamps:
            issues.append(f"row {index} ({timestamp}): duplicate timestamp")
        seen_timestamps.add(timestamp)
        for field, (minimum, maximum) in fields.items():
            value = hourly[field][index]
            if value is None:
                issues.append(f"row {index} ({timestamp}): {field} is null")
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                issues.append(f"row {index} ({timestamp}): {field} is not numeric")
                continue
            if not minimum <= number <= maximum:
                issues.append(f"row {index} ({timestamp}): {field}={number} outside valid range")
    return issues

def fetch_forecast(session, location, timeout: int = 30) -> dict:
    """Fetch a location forecast and validate hourly data with up to three attempts.

    Uses the configured date range when present, otherwise requests the configured
    forecast window. Transient request errors and invalid hourly data are retried.
    On the final data-quality failure, returns the payload with a quality-warning
    field so downstream processing can quarantine invalid rows.

    Args:
        session: Requests-compatible session used to make the HTTP request.
        location: Location object providing ``latitude``, ``longitude``, and ``name``.
        timeout: HTTP request timeout in seconds; defaults to 30.

    Returns:
        The decoded Open-Meteo JSON response as a dictionary.

    Raises:
        RuntimeError: If all attempts fail due to request or response errors.
    """
    start_date, end_date = requested_date_range()
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
    }
    if start_date and end_date:
        params.update({"start_date": start_date, "end_date": end_date})
    else:
        params["forecast_days"] = FORECAST_DAYS

    last_error = None
    last_payload = None
    for attempt in range(1, 4):  # initial attempt + two retries
        try:
            response = session.get(API_URL, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Invalid API response for {location.name}: expected a JSON object")
            hourly = payload.get("hourly")
            issues = _invalid_hourly_rows(hourly if isinstance(hourly, dict) else {})
            if issues:
                last_error = ValueError("; ".join(issues[:10]))
                last_payload = payload
                if attempt < 3:
                    time.sleep(attempt)
                    continue
                # Return the final payload so Silver can quarantine bad hourly rows,
                # notify with affected dates, and continue if other valid rows remain.
                payload["_pipeline_quality_warning"] = issues[:100]
                return payload
            return payload
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt)
    raise RuntimeError(
        f"{location.name} failed after 3 API attempts: "
        f"{type(last_error).__name__}: {last_error}"
    ) from last_error
