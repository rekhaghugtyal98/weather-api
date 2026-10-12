"""Task 1: ingest raw Open-Meteo responses into Bronze."""
import json
import logging
import os
from collections.abc import Sequence
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from ..common.api_client import fetch_forecast
from ..common.config import ALL_LOCATIONS, Location, resolve_location_selection, table_name
from ..common.notifications import send_failure_summary
from ..common.utils import utc_now_iso, safe_identifier
from ..common.data_io import write_data
from ..common.schemas import detect_payload_schema_changes

logger = logging.getLogger(__name__)

BRONZE_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("location_name", StringType(), False),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("fetched_at", StringType(), False),
    StructField("payload_json", StringType(), False),
])
AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("location_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("event_time", StringType(), False),
])
SCHEMA_AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("field_path", StringType(), False),
    StructField("change_type", StringType(), False),
    StructField("expected_type", StringType(), True),
    StructField("observed_type", StringType(), True),
    StructField("event_time", StringType(), False),
])

def _ensure_schemas(spark: SparkSession) -> None:
    catalog = safe_identifier(os.getenv("WEATHER_CATALOG", "workspace"))
    schema = safe_identifier(os.getenv("WEATHER_SCHEMA", "weather_api_dev"))
    audit_schema = safe_identifier(os.getenv("WEATHER_AUDIT_SCHEMA", "audit"))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{audit_schema}`")

def _initialize_metrics(
    metrics: dict | None,
    locations: Sequence[Location],
    skipped_city_ids: Sequence[str],
) -> None:
    if metrics is not None:
        metrics.update(
            input_row_count=len(locations) + len(skipped_city_ids),
            output_row_count=0,
            rejected_row_count=len(skipped_city_ids),
            successful_cities=[],
            failed_cities=[f"{city_id}: unknown city ID; skipped" for city_id in skipped_city_ids],
        )


def _record_skipped_locations(run_id: str, skipped_city_ids: Sequence[str]) -> list[tuple]:
    allowed_ids = ", ".join(location.location_id for location in ALL_LOCATIONS)
    audit_rows = []
    for city_id in skipped_city_ids:
        logger.warning("Skipping unknown city ID: run_id=%s city_id=%s", run_id, city_id)
        audit_rows.append((
            str(run_id), city_id, city_id, "SKIPPED",
            f"Unknown city ID; skipped. Allowed IDs: {allowed_ids}", utc_now_iso(),
        ))
    return audit_rows


def _ingest_location(
    session,
    run_id: str,
    location: Location,
    raw_rows: list[tuple],
    audit_rows: list[tuple],
    schema_audit_rows: list[tuple],
    failures: list[str],
    metrics: dict | None,
) -> None:
    """Fetch one city response and append its raw and audit records."""
    now = utc_now_iso()
    try:
        payload = fetch_forecast(session, location)
        fetched_at = utc_now_iso()
        raw_rows.append((
            str(run_id), location.location_id, location.name,
            str(location.latitude), str(location.longitude),
            fetched_at, json.dumps(payload, separators=(",", ":")),
        ))
        if metrics is not None:
            metrics["successful_cities"].append(location.name)
        logger.info("Fetched forecast: run_id=%s location_id=%s", run_id, location.location_id)
        for change in detect_payload_schema_changes(payload):
            schema_audit_rows.append((
                str(run_id), location.location_id, change["field_path"],
                change["change_type"], change["expected_type"],
                change["observed_type"], fetched_at,
            ))
        audit_rows.append((str(run_id), location.location_id, location.name, "SUCCESS", None, now))
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"[:4000]
        failures.append(f"{location.name} ({location.location_id}): {message}")
        if metrics is not None:
            metrics["failed_cities"].append(f"{location.name}: {message}")
        logger.error(
            "Forecast failed: run_id=%s location_id=%s error=%s",
            run_id,
            location.location_id,
            message,
        )
        audit_rows.append((str(run_id), location.location_id, location.name, "FAILED", message, now))


def _ingest_locations(
    session,
    run_id: str,
    locations: Sequence[Location],
    raw_rows: list[tuple],
    audit_rows: list[tuple],
    schema_audit_rows: list[tuple],
    failures: list[str],
    metrics: dict | None,
) -> None:
    """Fetch forecasts for each selected location."""
    for location in locations:
        _ingest_location(
            session,
            run_id,
            location,
            raw_rows,
            audit_rows,
            schema_audit_rows,
            failures,
            metrics,
        )


def _log_ingestion_counts(
    run_id: str,
    locations: Sequence[Location],
    skipped_city_ids: Sequence[str],
    raw_rows: Sequence[tuple],
    failures: Sequence[str],
    schema_audit_rows: Sequence[tuple],
    metrics: dict | None,
) -> None:
    if metrics is not None:
        metrics.update(
            output_row_count=len(raw_rows),
            rejected_row_count=len(skipped_city_ids) + len(failures),
        )
    logger.info(
        "Bronze ingestion counts: run_id=%s attempted=%s ingested=%s failed=%s skipped=%s schema_changes=%s",
        run_id,
        len(locations) + len(skipped_city_ids),
        len(raw_rows),
        len(failures),
        len(skipped_city_ids),
        len(schema_audit_rows),
    )


def _write_outputs(
    spark: SparkSession,
    raw_rows: Sequence[tuple],
    audit_rows: Sequence[tuple],
    schema_audit_rows: Sequence[tuple],
) -> None:
    if raw_rows:
        write_data(
            spark.createDataFrame(raw_rows, BRONZE_SCHEMA),
            "delta",
            table_name("bronze"),
        )
    if audit_rows:
        write_data(spark.createDataFrame(audit_rows, AUDIT_SCHEMA), "delta", table_name("location_audit"))
    if schema_audit_rows:
        write_data(
            spark.createDataFrame(schema_audit_rows, SCHEMA_AUDIT_SCHEMA),
            "delta",
            table_name("schema_audit"),
        )
        for row in schema_audit_rows:
            logger.warning(
                "Open-Meteo schema change: run_id=%s location_id=%s field=%s change=%s expected=%s observed=%s",
                *row[:6],
            )


def _send_warning_summary(
    run_id: str,
    failures: Sequence[str],
    skipped_city_ids: Sequence[str],
) -> None:
    if not failures and not skipped_city_ids:
        return
    requested_dates = (
        f"{os.getenv('WEATHER_START_DATE', 'default forecast window')} to "
        f"{os.getenv('WEATHER_END_DATE', 'default forecast window')}"
    )
    issues = list(failures)
    issues.extend(f"{city_id}: unknown city ID; skipped" for city_id in skipped_city_ids)
    body = (
        f"Weather ingestion had city-level failures or skipped IDs.\nRun ID: {run_id}\n"
        f"Requested cities: {os.getenv('WEATHER_CITIES', 'all')}\n"
        f"Requested date scope: {requested_dates}\n\nCity issues:\n- "
        + "\n- ".join(issues)
    )
    send_failure_summary("Open-Meteo pipeline: city ingestion warnings", body)
    logger.warning(body)


def _raise_if_no_payloads(
    run_id: str,
    raw_rows: Sequence[tuple],
    failures: Sequence[str],
    skipped_city_ids: Sequence[str],
) -> None:
    if raw_rows:
        return
    city_issues = list(failures) + [
        f"{city_id}: unknown city ID; skipped" for city_id in skipped_city_ids
    ]
    body = (
        f"No city payloads were ingested.\nRun ID: {run_id}\n"
        f"Requested cities: {os.getenv('WEATHER_CITIES', 'all')}\n\n"
        + "\n".join(city_issues)
    )
    send_failure_summary("Open-Meteo pipeline FAILED: no cities ingested", body)
    logger.error("Bronze ingestion produced no payloads: run_id=%s", run_id)
    raise RuntimeError(
        "No city payloads ingested; check city IDs and API failures. "
        + " | ".join(failures + list(skipped_city_ids))
    )


def ingest_bronze(spark: SparkSession, run_id: str, metrics: dict | None = None) -> None:
    """Fetch forecasts for selected locations and persist Bronze and audit records."""
    _ensure_schemas(spark)
    raw_rows, schema_audit_rows, failures = [], [], []
    locations, skipped_city_ids = resolve_location_selection()
    _initialize_metrics(metrics, locations, skipped_city_ids)
    logger.info(
        "Starting Bronze ingestion: run_id=%s selected_cities=%s skipped_city_ids=%s",
        run_id,
        ",".join(location.location_id for location in locations) or "none",
        ",".join(skipped_city_ids) or "none",
    )
    audit_rows = _record_skipped_locations(run_id, skipped_city_ids)
    # A new session per task avoids sharing mutable HTTP state across tasks.
    import requests
    session = requests.Session()
    _ingest_locations(
        session,
        run_id,
        locations,
        raw_rows,
        audit_rows,
        schema_audit_rows,
        failures,
        metrics,
    )
    _log_ingestion_counts(run_id, locations, skipped_city_ids, raw_rows, failures, schema_audit_rows, metrics)
    _write_outputs(spark, raw_rows, audit_rows, schema_audit_rows)
    _send_warning_summary(run_id, failures, skipped_city_ids)
    _raise_if_no_payloads(run_id, raw_rows, failures, skipped_city_ids)
    logger.info("Bronze ingestion completed: run_id=%s ingested=%s", run_id, len(raw_rows))
