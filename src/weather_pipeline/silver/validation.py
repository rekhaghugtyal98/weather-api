"""Pure validation helpers, suitable for unit tests."""
from datetime import datetime

def valid_hourly_record(record: dict) -> tuple[bool, str | None]:
    """Check that an hourly weather record has valid required fields and values.

    Args:
        record: Mapping-like dictionary containing location, timestamp, and weather
            measurements using the Silver field names.

    Returns:
        A pair of ``(is_valid, reason)``. The reason is ``None`` for valid records
        and a human-readable explanation otherwise.
    """
    required = ("location_id", "forecast_time", "temperature_c",
                "humidity_pct", "precipitation_mm", "wind_speed_kmh")
    missing = [key for key in required if record.get(key) is None]
    if missing:
        return False, f"Missing required values: {', '.join(missing)}"
    try:
        datetime.fromisoformat(str(record["forecast_time"]).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False, "Invalid forecast_time"
    try:
        temperature = float(record["temperature_c"])
        humidity = float(record["humidity_pct"])
        precipitation = float(record["precipitation_mm"])
        wind = float(record["wind_speed_kmh"])
    except (TypeError, ValueError):
        return False, "Weather values must be numeric"
    if not -90 <= temperature <= 65:
        return False, "temperature_c outside accepted range"
    if not 0 <= humidity <= 100:
        return False, "humidity_pct outside accepted range"
    if precipitation < 0:
        return False, "precipitation_mm cannot be negative"
    if wind < 0:
        return False, "wind_speed_kmh cannot be negative"
    return True, None
