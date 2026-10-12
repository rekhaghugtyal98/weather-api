"""Run the full pipeline locally against deterministic fixtures and Delta Lake."""
import os
import json
import shutil
import sys
import tempfile
import uuid
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

# Use the PySpark package selected for this environment instead of an unrelated
# system Spark installation, and derive JAVA_HOME from the executable on PATH.
os.environ.pop("SPARK_HOME", None)
java_executable = shutil.which("java")
if java_executable:
    os.environ["JAVA_HOME"] = str(Path(java_executable).resolve().parent.parent)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from weather_pipeline.common.config import table_name
from weather_pipeline.bronze import ingest as bronze_module
from weather_pipeline.bronze.ingest import ingest_bronze
from weather_pipeline.gold.aggregate import build_gold
from weather_pipeline.silver.transform import transform_silver


def _windows_hadoop_bin():
    if os.name != "nt":
        return None
    hadoop_home = Path(os.environ.get("HADOOP_HOME", ""))
    hadoop_bin = hadoop_home / "bin"
    required_files = (hadoop_bin / "hadoop.dll", hadoop_bin / "winutils.exe")
    if not all(path.is_file() for path in required_files):
        raise RuntimeError(
            "Local Spark on Windows needs hadoop.dll and winutils.exe under "
            "HADOOP_HOME\\bin, built for the Hadoop version bundled with PySpark. "
            "Alternatively, run this local fixture runner on Linux or macOS."
        )
    os.environ["PATH"] = str(hadoop_bin) + os.pathsep + os.environ.get("PATH", "")
    return hadoop_bin


def _assert_rows(actual_rows, expected_rows, fields):
    def stable(value):
        return value.isoformat() if isinstance(value, date) else value

    actual = {
        tuple(stable(row[field]) for field in fields)
        for row in actual_rows
    }
    expected = {
        tuple(stable(expected[field]) for field in fields)
        for expected in expected_rows
    }
    if actual != expected:
        raise AssertionError(f"Rows did not match. Expected {expected!r}; got {actual!r}")


def main():
    sample_data = json.loads((Path(__file__).parent / "sample_data.json").read_text(encoding="utf-8"))
    hadoop_bin = _windows_hadoop_bin()
    warehouse = tempfile.TemporaryDirectory(prefix="weather-pipeline-e2e-")
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("weather-pipeline-fixture-e2e")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse.name)
    )
    if hadoop_bin:
        java_library_path = f"-Djava.library.path={hadoop_bin}"
        builder = (
            builder
            .config("spark.driver.extraJavaOptions", java_library_path)
            .config("spark.executor.extraJavaOptions", java_library_path)
        )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    catalog = "spark_catalog"
    schema = f"pipeline_fixture_{uuid.uuid4().hex[:12]}"
    audit_schema = f"pipeline_fixture_audit_{uuid.uuid4().hex[:12]}"
    os.environ["WEATHER_CATALOG"] = catalog
    os.environ["WEATHER_SCHEMA"] = schema
    os.environ["WEATHER_AUDIT_SCHEMA"] = audit_schema
    os.environ["WEATHER_CITIES"] = "bengaluru"

    run_id = f"fixture-{uuid.uuid4()}"
    original_fetch_forecast = bronze_module.fetch_forecast
    bronze_module.fetch_forecast = lambda _session, _location: sample_data["api_data"]
    try:
        ingest_bronze(spark, run_id)
        bronze_rows = spark.table(table_name("bronze")).where(f"run_id = '{run_id}'").collect()
        if len(bronze_rows) != 1:
            raise AssertionError(f"Expected one Bronze row; got {len(bronze_rows)}")
        bronze_row = bronze_rows[0].asDict()
        import json
        if (
            bronze_row["location_id"] != sample_data["bronze_data"]["location_id"]
            or bronze_row["location_name"] != sample_data["bronze_data"]["location_name"]
            or bronze_row["latitude"] != sample_data["bronze_data"]["latitude"]
            or bronze_row["longitude"] != sample_data["bronze_data"]["longitude"]
            or json.loads(bronze_row["payload_json"]) != sample_data["bronze_data"]["payload"]
        ):
            raise AssertionError("Bronze output did not match the fixture")

        transform_silver(spark, run_id)
        silver_rows = (
            spark.table(table_name("silver"))
            .where(f"run_id = '{run_id}'")
            .collect()
        )
        _assert_rows(
            silver_rows,
            sample_data["silver_data"],
            (
                "location_id", "location_name", "forecast_time", "temperature_c",
                "humidity_pct", "precipitation_mm", "wind_speed_kmh", "forecast_date",
            ),
        )

        build_gold(spark, run_id)
        gold_rows = (
            spark.table(table_name("gold"))
            .where("location_id = 'bengaluru' AND forecast_date = DATE '2026-10-10'")
            .collect()
        )
        if len(gold_rows) != 1:
            raise AssertionError(f"Expected one Gold row; got {len(gold_rows)}")
        gold_row = gold_rows[0].asDict()
        for field, expected in sample_data["gold_data"].items():
            actual = gold_row[field]
            if field == "forecast_date" and isinstance(actual, date):
                actual = actual.isoformat()
            if actual != expected:
                raise AssertionError(f"Gold field {field}: expected {expected!r}, got {actual!r}")

        print(f"Fixture pipeline succeeded: run_id={run_id}, schema={schema}")
    finally:
        bronze_module.fetch_forecast = original_fetch_forecast
        spark.stop()
        warehouse.cleanup()


if __name__ == "__main__":
    main()
