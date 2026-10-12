"""Shared fixtures for local Spark and layer integration tests."""
import json
import os
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark_session():
    """Create a local Delta-enabled Spark session for integration tests."""
    pytest.importorskip("pyspark", reason="Install the integration dependencies to run Spark tests")
    delta = pytest.importorskip("delta", reason="Install delta-spark to run Spark tests")
    from pyspark.sql import SparkSession

    java_executable = shutil.which("java")
    if not java_executable:
        pytest.skip("A local Java installation is required for Spark tests")
    os.environ.pop("SPARK_HOME", None)
    os.environ["JAVA_HOME"] = str(Path(java_executable).resolve().parent.parent)

    if sys.platform == "win32":
        hadoop_home = Path(os.environ.get("HADOOP_HOME", ""))
        hadoop_bin = hadoop_home / "bin"
        if not all((hadoop_bin / name).is_file() for name in ("hadoop.dll", "winutils.exe")):
            pytest.skip("Windows Spark tests need hadoop.dll and winutils.exe under HADOOP_HOME\\bin")
        os.environ["PATH"] = str(hadoop_bin) + os.pathsep + os.environ.get("PATH", "")

    warehouse = tempfile.TemporaryDirectory(prefix="weather-test-spark-")
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("weather-pipeline-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse.name)
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
        warehouse.cleanup()


@pytest.fixture
def weather_test_context(monkeypatch, spark_session):
    """Set isolated table names and load the shared source/expected data."""
    from weather_pipeline.common.config import table_name
    from pyspark.sql.types import (
        DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    )

    schema_suffix = uuid.uuid4().hex[:12]
    monkeypatch.setenv("WEATHER_CATALOG", "spark_catalog")
    monkeypatch.setenv("WEATHER_SCHEMA", f"weather_test_{schema_suffix}")
    monkeypatch.setenv("WEATHER_AUDIT_SCHEMA", f"weather_test_audit_{schema_suffix}")
    monkeypatch.setenv("WEATHER_CITIES", "bengaluru,delhi,mumbai")
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS `spark_catalog`.`weather_test_{schema_suffix}`")
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS `spark_catalog`.`weather_test_audit_{schema_suffix}`")

    # Production provisions the layer tables before these tasks run. Create empty
    # targets here so the integration tests exercise that same precondition.
    silver_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("forecast_time", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", IntegerType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("forecast_date", DateType(), True),
        StructField("ingested_at", StringType(), True),
    ])
    gold_schema = StructType([
        StructField("location_id", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("forecast_date", DateType(), True),
        StructField("avg_temperature_c", DoubleType(), True),
        StructField("min_temperature_c", DoubleType(), True),
        StructField("max_temperature_c", DoubleType(), True),
        StructField("avg_humidity_pct", DoubleType(), True),
        StructField("total_precipitation_mm", DoubleType(), True),
        StructField("avg_wind_speed_kmh", DoubleType(), True),
        StructField("hourly_record_count", LongType(), True),
        StructField("updated_at", StringType(), True),
    ])
    (spark_session.createDataFrame([], silver_schema).write.format("delta")
        .partitionBy("forecast_date").saveAsTable(table_name("silver")))
    (spark_session.createDataFrame([], gold_schema).write.format("delta")
        .partitionBy("forecast_date").saveAsTable(table_name("gold")))
    data_dir = Path(__file__).parent / "data"
    api_responses = {
        response_file.stem: json.loads(response_file.read_text(encoding="utf-8"))
        for response_file in sorted((data_dir / "api_responses").glob("*.json"))
    }
    return {
        "spark": spark_session,
        "schema": f"weather_test_{schema_suffix}",
        "run_id": f"test-{uuid.uuid4()}",
        "api_responses": api_responses,
        "bronze_expected": json.loads((data_dir / "bronze_expected.json").read_text(encoding="utf-8")),
        "silver_expected": json.loads((data_dir / "silver_expected.json").read_text(encoding="utf-8")),
        "gold_expected": json.loads((data_dir / "gold_expected.json").read_text(encoding="utf-8")),
    }


@pytest.fixture
def bronze_output(weather_test_context, monkeypatch):
    """Run Bronze with the deterministic API fixture instead of the network."""
    pytest.importorskip("pyspark")
    from weather_pipeline.bronze import ingest as bronze_module

    context = weather_test_context
    monkeypatch.setattr(
        bronze_module,
        "fetch_forecast",
        lambda _session, location: context["api_responses"][location.location_id],
    )
    bronze_module.ingest_bronze(context["spark"], context["run_id"])
    return context


@pytest.fixture
def silver_output(bronze_output):
    """Run Silver using the Bronze rows produced by the fixture."""
    pytest.importorskip("pyspark")
    from weather_pipeline.silver.transform import transform_silver

    transform_silver(bronze_output["spark"], bronze_output["run_id"])
    return bronze_output


@pytest.fixture
def gold_output(silver_output):
    """Run Gold using the Silver rows produced by the fixture."""
    pytest.importorskip("pyspark")
    from weather_pipeline.gold.aggregate import build_gold

    build_gold(silver_output["spark"], silver_output["run_id"])
    return silver_output
