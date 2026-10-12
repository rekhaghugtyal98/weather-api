"""Task 2: flatten, validate, quarantine and merge Bronze into Silver."""
import logging
from pyspark.sql import SparkSession, functions as F
from ..common.config import table_name
from ..common.notifications import send_failure_summary
from ..common.data_io import merge_data, read_data, write_data
from ..common.schemas import API_RESPONSE_SCHEMA

logger = logging.getLogger(__name__)

def transform_silver(spark: SparkSession, run_id: str, metrics: dict | None = None) -> None:
    """Convert this run's raw Bronze payloads into validated hourly Silver records.

    The method parses and flattens hourly arrays, checks value ranges and duplicate
    keys, appends invalid records to quarantine, and merges valid records into the
    pre-provisioned Silver table. The configured catalog and schemas, plus the
    Bronze, Silver, and quarantine tables, must already exist. Invalid-record
    summaries are emailed when configured.

    Args:
        spark: Active Spark session used to read and write Delta tables.
        run_id: Run identifier used to select Bronze payloads and tag output rows.
        metrics: Optional dictionary populated with Bronze, valid, and rejected row counts.

    Raises:
        RuntimeError: If this run has no Bronze payloads or produces no valid rows.
    """
    logger.info("Starting Silver transform: run_id=%s", run_id)
    bronze = read_data(spark, "delta", table_name("bronze")).where(F.col("run_id") == str(run_id))
    bronze_count = bronze.count()
    logger.info("Silver input count: run_id=%s bronze_payloads=%s", run_id, bronze_count)
    if metrics is not None:
        metrics.update(input_row_count=bronze_count, output_row_count=0, rejected_row_count=0)
    if bronze_count == 0:
        logger.error("Silver has no Bronze input: run_id=%s", run_id)
        raise RuntimeError(f"No Bronze payloads found for run_id={run_id}")

    parsed = bronze.withColumn("api", F.from_json("payload_json", API_RESPONSE_SCHEMA))
    arrays = F.arrays_zip(
        F.col("api.hourly.time").alias("time"),
        F.col("api.hourly.temperature_2m").alias("temperature"),
        F.col("api.hourly.relative_humidity_2m").alias("humidity"),
        F.col("api.hourly.precipitation").alias("precipitation"),
        F.col("api.hourly.wind_speed_10m").alias("wind"),
    )
    flat = parsed.withColumn("hour", F.explode_outer(arrays)).select(
        "run_id", "location_id", "location_name",
        F.col("hour.time").alias("forecast_time"),
        F.col("hour.temperature").cast("double").alias("temperature_c"),
        F.col("hour.humidity").cast("int").alias("humidity_pct"),
        F.col("hour.precipitation").cast("double").alias("precipitation_mm"),
        F.col("hour.wind").cast("double").alias("wind_speed_kmh"),
        F.to_date(F.col("hour.time")).alias("forecast_date"),
        F.current_timestamp().cast("string").alias("ingested_at"),
    )
    valid_condition = (
        F.col("forecast_time").isNotNull()
        & F.col("temperature_c").between(-90, 65)
        & F.col("humidity_pct").between(0, 100)
        & (F.col("precipitation_mm") >= 0)
        & (F.col("wind_speed_kmh") >= 0)
    )
    duplicate_keys = (flat.groupBy("location_id", "forecast_time").count()
                      .where(F.col("count") > 1)
                      .select("location_id", "forecast_time")
                      .withColumn("is_duplicate", F.lit(True)))
    checked = flat.join(duplicate_keys, ["location_id", "forecast_time"], "left")
    is_duplicate = F.coalesce(F.col("is_duplicate"), F.lit(False))
    valid = checked.where(valid_condition & ~is_duplicate).drop("is_duplicate")
    invalid = (checked.where(~valid_condition | is_duplicate)
        .withColumn(
            "quarantine_reason",
            F.when(~valid_condition, F.lit("Null or out-of-range hourly weather value"))
             .otherwise(F.lit("Duplicate location_id and forecast_time")),
        ).drop("is_duplicate"))

    valid_count = valid.count()
    invalid_count = invalid.count()
    if metrics is not None:
        metrics.update(output_row_count=valid_count, rejected_row_count=invalid_count)
    logger.info(
        "Silver validation counts: run_id=%s valid_hourly_rows=%s rejected_hourly_rows=%s",
        run_id,
        valid_count,
        invalid_count,
    )

    # Summarize invalid city/date coverage for notification, without failing if some good rows remain.
    invalid_summary = []
    if invalid_count:
        invalid_summary = [
            f"{r['location_name']} on {r['forecast_date']}: {r['invalid_count']} invalid hourly record(s)"
            for r in invalid.withColumn("forecast_date", F.to_date("forecast_time")) \
                .groupBy("location_name", "forecast_date").count() \
                .withColumnRenamed("count", "invalid_count").orderBy("location_name", "forecast_date").collect()
        ]
        write_data(invalid, "delta", table_name("quarantine"), schema_mode="merge")

    if valid_count == 0:
        body = (
            f"No valid hourly weather records were produced. Silver task is failing.\nRun ID: {run_id}\n"
            + ("\n".join(invalid_summary) if invalid_summary else "No valid rows found; inspect Bronze/API response.")
        )
        send_failure_summary("Open-Meteo pipeline FAILED: all records invalid", body)
        logger.error("Silver produced no valid rows: run_id=%s rejected_hourly_rows=%s", run_id, invalid_count)
        raise RuntimeError(f"No valid hourly records produced for run_id={run_id}; stopping downstream tasks")

    merge_data(valid, table_name("silver"), ("location_id", "forecast_time"))
    logger.info(
        "Silver merge completed: run_id=%s source_valid_rows=%s quarantined_rows=%s",
        run_id,
        valid_count,
        invalid_count,
    )

    if invalid_summary:
        body = (
            f"Some hourly weather records were invalid and have been quarantined. "
            f"The pipeline will continue because valid records remain.\nRun ID: {run_id}\n\n"
            + "\n".join(invalid_summary)
        )
        send_failure_summary("Open-Meteo pipeline warning: invalid city/date records", body)
        logger.warning(body)
