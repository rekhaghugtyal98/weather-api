"""Central configuration for the Open-Meteo weather pipeline."""
from dataclasses import dataclass
import os
from datetime import date

@dataclass(frozen=True)
class Location:
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
    """Select all cities by default, or a comma-separated subset via WEATHER_CITIES."""
    requested = os.getenv("WEATHER_CITIES", "").strip()
    if not requested:
        return ALL_LOCATIONS
    wanted = {item.strip().lower() for item in requested.split(",") if item.strip()}
    known = {loc.location_id: loc for loc in ALL_LOCATIONS}
    unknown = wanted - set(known)
    if unknown:
        raise ValueError(
            f"Unknown WEATHER_CITIES value(s): {', '.join(sorted(unknown))}. "
            f"Allowed IDs: {', '.join(sorted(known))}"
        )
    if not wanted:
        raise ValueError("WEATHER_CITIES was set but no city IDs were supplied")
    return tuple(loc for loc in ALL_LOCATIONS if loc.location_id in wanted)

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
    """Optional date scope. Open-Meteo forecast endpoint only supports dates in its available window."""
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
    """Build a three-part Unity Catalog table name from environment variables."""
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
