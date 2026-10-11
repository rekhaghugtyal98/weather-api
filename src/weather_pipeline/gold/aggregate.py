"""Task 3: aggregate Silver hourly records into daily Gold metrics."""
from pyspark.sql import SparkSession, functions as F
from ..common.config import table_name
from ..common.utils import safe_identifier

def build_gold(spark: SparkSession, run_id: str) -> None:
    catalog = safe_identifier(__import__("os").getenv("WEATHER_CATALOG", "main"))
    schema = safe_identifier(__import__("os").getenv("WEATHER_SCHEMA", "weather_pipeline"))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    silver = spark.table(table_name("silver"))
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
    daily.createOrReplaceTempView("incoming_weather_gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name("gold")} (
          location_id STRING, location_name STRING, forecast_date DATE,
          avg_temperature_c DOUBLE, min_temperature_c DOUBLE, max_temperature_c DOUBLE,
          avg_humidity_pct DOUBLE, total_precipitation_mm DOUBLE,
          avg_wind_speed_kmh DOUBLE, hourly_record_count BIGINT, updated_at STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        MERGE INTO {table_name("gold")} AS target
        USING incoming_weather_gold AS source
        ON target.location_id = source.location_id
           AND target.forecast_date = source.forecast_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
