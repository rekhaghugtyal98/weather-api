"""Databricks Python-file entrypoint for the weather pipeline jobs."""
import argparse
import os
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True, choices=("ingest_bronze", "transform_silver", "build_gold"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cities", default="all")
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="weather_api_dev")
    parser.add_argument("--audit-schema", default="audit")
    args = parser.parse_args()

    # Databricks may execute this file via exec(compile(...)), where __file__ is unset.
    source_root = str(Path(main.__code__.co_filename).resolve().parents[1])
    if source_root not in sys.path:
        sys.path.insert(0, source_root)

    # Job parameters are passed as CLI arguments; pipeline configuration reads env vars.
    os.environ["WEATHER_CATALOG"] = args.catalog
    os.environ["WEATHER_SCHEMA"] = args.schema
    os.environ["WEATHER_AUDIT_SCHEMA"] = args.audit_schema
    if args.cities.strip().lower() != "all":
        os.environ["WEATHER_CITIES"] = args.cities
    else:
        os.environ.pop("WEATHER_CITIES", None)
    if args.start_date and args.end_date:
        os.environ["WEATHER_START_DATE"] = args.start_date
        os.environ["WEATHER_END_DATE"] = args.end_date
    else:
        os.environ.pop("WEATHER_START_DATE", None)
        os.environ.pop("WEATHER_END_DATE", None)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    if args.task == "ingest_bronze":
        from weather_pipeline.bronze.ingest import ingest_bronze
        ingest_bronze(spark, args.run_id)
    elif args.task == "transform_silver":
        from weather_pipeline.silver.transform import transform_silver
        transform_silver(spark, args.run_id)
    else:
        from weather_pipeline.gold.aggregate import build_gold
        build_gold(spark, args.run_id)


if __name__ == "__main__":
    main()
