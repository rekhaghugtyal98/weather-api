"""Task 3: aggregate Silver hourly records into daily Gold metrics."""
import logging
from pyspark.sql import DataFrame, SparkSession, functions as F
from ..common.config import table_name
from ..common.data_io import merge_data, read_data

logger = logging.getLogger(__name__)


def _read_silver_data(spark: SparkSession) -> DataFrame:
    """Read hourly records from the pre-provisioned Silver table."""
    return read_data(spark, "delta", table_name("silver"))


def _transform_silver_data(silver: DataFrame) -> DataFrame:
    """Aggregate Silver hourly records into daily location metrics."""
    return silver.withColumn("forecast_date", F.to_date("forecast_time")).groupBy(
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


def _write_gold_data(daily: DataFrame) -> None:
    """Merge daily aggregates into the pre-provisioned Gold table."""
    merge_data(daily, table_name("gold"), ("location_id", "forecast_date"))


def build_gold(spark: SparkSession, run_id: str, metrics: dict | None = None) -> None:
    """Aggregate Silver weather observations into daily Gold metrics.

    Calculates daily temperature, humidity, precipitation, wind, and record-count
    metrics for each location, then merges the results into the pre-provisioned
    Gold Delta table. The configured catalog, schema, and Silver and Gold tables
    must already exist.

    Args:
        spark: Active Spark session used to read Silver and write Gold.
        run_id: Pipeline run identifier retained for a consistent task interface.
        metrics: Optional dictionary populated with Silver input and daily output counts.
    """
    logger.info("Starting Gold aggregation: run_id=%s", run_id)
    silver = _read_silver_data(spark)
    input_count = silver.count()
    logger.info("Gold input count: run_id=%s silver_hourly_rows=%s", run_id, input_count)
    if metrics is not None:
        metrics.update(input_row_count=input_count, output_row_count=0, rejected_row_count=0)
    daily = _transform_silver_data(silver)
    output_count = daily.count()
    logger.info("Gold aggregation count: run_id=%s daily_city_rows=%s", run_id, output_count)
    if metrics is not None:
        metrics.update(output_row_count=output_count)
    _write_gold_data(daily)
    logger.info(
        "Gold merge completed: run_id=%s source_daily_rows=%s",
        run_id,
        output_count,
    )
