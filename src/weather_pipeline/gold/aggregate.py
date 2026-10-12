"""Task 3: aggregate Silver hourly records into daily Gold metrics."""
import logging
from pyspark.sql import SparkSession, functions as F
from ..common.config import table_name
from ..common.utils import safe_identifier
from ..common.data_io import read_data

logger = logging.getLogger(__name__)

def build_gold(spark: SparkSession, run_id: str, metrics: dict | None = None) -> None:
    """Aggregate Silver weather observations into daily Gold metrics.

    Calculates daily temperature, humidity, precipitation, wind, and record-count
    metrics for each location, then merges the results into the Gold Delta table.

    Args:
        spark: Active Spark session used to read Silver and write Gold.
        run_id: Pipeline run identifier retained for a consistent task interface.
        metrics: Optional dictionary populated with Silver input and daily output counts.
    """
    logger.info("Starting Gold aggregation: run_id=%s", run_id)
    catalog = safe_identifier(__import__("os").getenv("WEATHER_CATALOG", "workspace"))
    schema = safe_identifier(__import__("os").getenv("WEATHER_SCHEMA", "weather_api_dev"))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    silver = read_data(spark, "delta", table_name("silver"))
    input_count = silver.count()
    logger.info("Gold input count: run_id=%s silver_hourly_rows=%s", run_id, input_count)
    if metrics is not None:
        metrics.update(input_row_count=input_count, output_row_count=0, rejected_row_count=0)
    daily = silver.withColumn("forecast_date", F.to_date("forecast_time")).groupBy(
        "location_id", "location_name", "forecast_date"
    ).agg(
        F.avg("temperature_c").alias("avg_temperature_c"),
        F.min("temperature_c").alias("min_temperature_c"),
        F.max("temperature_c").alias("max_temperature_c"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
        F.sum("precipitation_mm").alias("total_precipitation_mm"),
        F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
        F.count("*").alias("hourly_record_count"),
    ).withColumn("updated_at", F.current_timestamp().cast("string"))
    output_count = daily.count()
    logger.info("Gold aggregation count: run_id=%s daily_city_rows=%s", run_id, output_count)
    if metrics is not None:
        metrics.update(output_row_count=output_count)
    daily.createOrReplaceTempView("incoming_weather_gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name("gold")} (
          location_id STRING, location_name STRING, forecast_date DATE,
          avg_temperature_c DOUBLE, min_temperature_c DOUBLE, max_temperature_c DOUBLE,
          avg_humidity_pct DOUBLE, total_precipitation_mm DOUBLE,
          avg_wind_speed_kmh DOUBLE, hourly_record_count BIGINT, updated_at STRING
        ) USING DELTA
        PARTITIONED BY (forecast_date)
    """)
    spark.sql(f"""
        MERGE INTO {table_name("gold")} AS target
        USING incoming_weather_gold AS source
        ON target.location_id = source.location_id
           AND target.forecast_date = source.forecast_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info(
        "Gold merge completed: run_id=%s source_daily_rows=%s",
        run_id,
        output_count,
    )
