"""Task 1: ingest raw Open-Meteo responses into Bronze."""
import json
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from ..common.api_client import fetch_forecast
from ..common.config import selected_locations, table_name
from ..common.notifications import send_failure_summary
from ..common.utils import utc_now_iso, safe_identifier
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

def ingest_bronze(spark: SparkSession, run_id: str, metrics: dict | None = None) -> None:
    """Fetch forecasts for configured locations and persist raw Bronze and audit records.

    Each location is requested independently. Successful raw responses and schema
    drift are saved to Delta tables; per-location success or failure is recorded
    in the ingestion audit table. Failures trigger a summary notification.

    Args:
        spark: Active Spark session used to create schemas and write Delta tables.
        run_id: Identifier shared by all records produced by this pipeline run.
        metrics: Optional dictionary populated with attempted, ingested, and failed counts.

    Raises:
        RuntimeError: If every selected location fails to return a usable payload.
    """
    catalog = safe_identifier(os.getenv("WEATHER_CATALOG", "workspace"))
    schema = safe_identifier(os.getenv("WEATHER_SCHEMA", "weather_api_dev"))
    audit_schema = safe_identifier(os.getenv("WEATHER_AUDIT_SCHEMA", "audit"))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{audit_schema}`")
    bronze_table = table_name("bronze")
    audit_table = table_name("location_audit")

    raw_rows, audit_rows, schema_audit_rows, failures = [], [], [], []
    locations = selected_locations()
    if metrics is not None:
        metrics.update(input_row_count=len(locations), output_row_count=0, rejected_row_count=0)
    # A new session per task avoids sharing mutable HTTP state across tasks.
    import requests
    session = requests.Session()

    for location in locations:
        now = utc_now_iso()
        try:
            payload = fetch_forecast(session, location)
            fetched_at = utc_now_iso()
            raw_rows.append((
                str(run_id), location.location_id, location.name,
                str(location.latitude), str(location.longitude),
                fetched_at, json.dumps(payload, separators=(",", ":")),
            ))
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
            audit_rows.append((str(run_id), location.location_id, location.name, "FAILED", message, now))

    if metrics is not None:
        metrics.update(output_row_count=len(raw_rows), rejected_row_count=len(failures))

    if raw_rows:
        spark.createDataFrame(raw_rows, BRONZE_SCHEMA).write.format("delta").mode("append").saveAsTable(bronze_table)
    if audit_rows:
        spark.createDataFrame(audit_rows, AUDIT_SCHEMA).write.format("delta").mode("append").saveAsTable(audit_table)
    if schema_audit_rows:
        (spark.createDataFrame(schema_audit_rows, SCHEMA_AUDIT_SCHEMA)
            .write.format("delta").mode("append").saveAsTable(table_name("schema_audit")))
        for row in schema_audit_rows:
            logger.warning(
                "Open-Meteo schema change: run_id=%s location_id=%s field=%s change=%s expected=%s observed=%s",
                *row[:6],
            )

    if failures:
        requested_dates = f"{os.getenv('WEATHER_START_DATE', 'default forecast window')} to {os.getenv('WEATHER_END_DATE', 'default forecast window')}"
        body = (
            f"Weather ingestion had city-level failures.\nRun ID: {run_id}\n"
            f"Requested cities: {', '.join(x.name for x in locations)}\n"
            f"Requested date scope: {requested_dates}\n\nFailed cities:\n- "
            + "\n- ".join(failures)
        )
        send_failure_summary(
            "Open-Meteo pipeline: city ingestion failures",
            body,
        )
        logger.warning(body)

    if not raw_rows:
        body = (
            f"All selected city API calls failed after retries.\nRun ID: {run_id}\n"
            f"Cities: {', '.join(x.name for x in locations)}\n\n"
            + "\n".join(failures)
        )
        send_failure_summary("Open-Meteo pipeline FAILED: no cities ingested", body)
        raise RuntimeError("All selected city API calls failed after 3 attempts; stopping pipeline. " + " | ".join(failures))
