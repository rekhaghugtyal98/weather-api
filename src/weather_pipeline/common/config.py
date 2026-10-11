"""Central configuration for the Open-Meteo weather pipeline."""
from dataclasses import dataclass
import os
from datetime import date

@dataclass(frozen=True)
class Location:
    """Immutable metadata for a weather forecast location.

    Attributes:
        location_id: Stable identifier used in configuration and table records.
        name: Display name for the location.
        latitude: Geographic latitude in decimal degrees.
        longitude: Geographic longitude in decimal degrees.
    """
    location_id: str
    name: str
    latitude: float
    longitude: float

ALL_LOCATIONS = (
    Location("bengaluru", "Bengaluru", 12.9716, 77.5946),
    Location("delhi", "Delhi", 28.6139, 77.2090),
    Location("mumbai", "Mumbai", 19.0760, 72.8777),
    Location("chennai", "Chennai", 13.0827, 80.2707),
    Location("hyderabad", "Hyderabad", 17.3850, 78.4867),
)

def selected_locations():
    """Return all configured locations or valid IDs selected by environment.

    The ``WEATHER_CITIES`` variable accepts comma-separated location IDs. An
    unset or blank value selects every configured location. Unknown IDs are
    ignored when at least one valid ID is present.

    Returns:
        A tuple of :class:`Location` objects in the configured order.

    Raises:
        ValueError: If no valid location IDs were selected.
    """
    locations, unknown = resolve_location_selection()
    if not locations:
        raise ValueError(
            f"No valid city IDs selected. Unknown IDs: {', '.join(unknown)}. "
            f"Allowed IDs: {', '.join(loc.location_id for loc in ALL_LOCATIONS)}"
        )
    return locations


def resolve_location_selection():
    """Return valid configured locations and any unknown requested IDs.

    Returns:
        A pair containing a tuple of valid :class:`Location` objects and a tuple
        of unknown city IDs. An unset or blank variable selects every location.

    Raises:
        ValueError: If ``WEATHER_CITIES`` is set but contains no IDs.
    """
    requested = os.getenv("WEATHER_CITIES", "").strip()
    if not requested:
        return ALL_LOCATIONS, ()
    wanted = {item.strip().lower() for item in requested.split(",") if item.strip()}
    if not wanted:
        raise ValueError("WEATHER_CITIES was set but no city IDs were supplied")
    known = {loc.location_id: loc for loc in ALL_LOCATIONS}
    unknown = wanted - set(known)
    locations = tuple(loc for loc in ALL_LOCATIONS if loc.location_id in wanted)
    return locations, tuple(sorted(unknown))

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)
FORECAST_DAYS = int(os.getenv("WEATHER_FORECAST_DAYS", "7"))
API_URL = os.getenv("OPEN_METEO_API_URL", "https://api.open-meteo.com/v1/forecast")
MAX_API_ATTEMPTS = 3  # initial attempt + two retries

def requested_date_range():
    """Read and validate the optional date range from environment variables.

    Returns:
        A ``(start_date, end_date)`` ISO date string pair, or ``(None, None)``
        when no date range is configured.

    Raises:
        ValueError: If only one date is set, either date is malformed, or the end
            date precedes the start date.
    """
    start = os.getenv("WEATHER_START_DATE", "").strip()
    end = os.getenv("WEATHER_END_DATE", "").strip()
    if bool(start) != bool(end):
        raise ValueError("Set both WEATHER_START_DATE and WEATHER_END_DATE, or neither")
    if start:
        start_date, end_date = date.fromisoformat(start), date.fromisoformat(end)
        if end_date < start_date:
            raise ValueError("WEATHER_END_DATE must be on or after WEATHER_START_DATE")
        return start, end
    return None, None

def table_name(layer: str) -> str:
    """Build a quoted Unity Catalog table name for a pipeline data layer.

    Args:
        layer: Logical table key such as ``bronze``, ``silver``, ``gold``, or
            ``job_audit``.

    Returns:
        A three-part, backtick-quoted catalog, schema, and table name.

    Raises:
        ValueError: If ``layer`` is not a supported table key.
    """
    catalog = os.getenv("WEATHER_CATALOG", "workspace")
    schema = os.getenv("WEATHER_SCHEMA", "weather_api_dev")
    audit_schema = os.getenv("WEATHER_AUDIT_SCHEMA", "audit")
    table_names = {
        "bronze": "weather_api_raw",
        "silver": "weather_hourly_transformed",
        "gold": "weather_daily_aggregation",
        "quarantine": "weather_rejected_hourly_records",
        "location_audit": "weather_ingestion_audit",
        "schema_audit": "weather_api_schema_changes",
        "job_audit": "weather_job_audit",
    }
    if layer not in table_names:
        raise ValueError(f"Unsupported table layer: {layer}")
    selected_schema = audit_schema if layer in {
        "quarantine", "location_audit", "schema_audit", "job_audit"
    } else schema
    return f"`{catalog}`.`{selected_schema}`.`{table_names[layer]}`"
