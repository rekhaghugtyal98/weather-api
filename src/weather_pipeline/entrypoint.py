"""Databricks Python-file entrypoint for the weather pipeline jobs."""
import argparse
import logging
import os
import sys
from pathlib import Path


def main() -> None:
    """Parse Databricks job arguments, configure runtime settings, and run a pipeline task.

    The task name, run ID, location/date scope, catalog, API endpoint, and forecast
    length are read from command-line arguments. Runtime environment variables are
    set before importing pipeline configuration. The selected Bronze, Silver, or
    Gold task runs with audit status and row counts recorded.

    Raises:
        Exception: Re-raises task failures after recording a failed audit status.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True, choices=("ingest_bronze", "transform_silver", "build_gold"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cities", default="all")
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="weather_api_dev")
    parser.add_argument("--audit-schema", default="audit")
    parser.add_argument("--api-url")
    parser.add_argument("--forecast-days", type=int)
    args = parser.parse_args()

    # Databricks may execute this file via exec(compile(...)), where __file__ is unset.
    source_root = str(Path(main.__code__.co_filename).resolve().parents[1])
    if source_root not in sys.path:
        sys.path.insert(0, source_root)

    # Job parameters are passed as CLI arguments; pipeline configuration reads env vars.
    os.environ["WEATHER_CATALOG"] = args.catalog
    os.environ["WEATHER_SCHEMA"] = args.schema
    os.environ["WEATHER_AUDIT_SCHEMA"] = args.audit_schema
    if args.api_url:
        os.environ["OPEN_METEO_API_URL"] = args.api_url
    if args.forecast_days is not None:
        if not 3 <= args.forecast_days <= 7:
            parser.error("--forecast-days must be between 3 and 7")
        os.environ["WEATHER_FORECAST_DAYS"] = str(args.forecast_days)
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
    from weather_pipeline.common.job_audit import start_job_audit, finish_job_audit

    task_layers = {
        "ingest_bronze": "bronze",
        "transform_silver": "silver",
        "build_gold": "gold",
    }
    parameters = {
        "cities": args.cities,
        "start_date": args.start_date or None,
        "end_date": args.end_date or None,
        "catalog": args.catalog,
        "schema": args.schema,
        "audit_schema": args.audit_schema,
        "api_url": args.api_url or os.getenv("OPEN_METEO_API_URL", "https://api.open-meteo.com/v1/forecast"),
        "forecast_days": args.forecast_days or int(os.getenv("WEATHER_FORECAST_DAYS", "7")),
    }
    audit_id = start_job_audit(spark, args.run_id, task_layers[args.task], parameters)
    metrics = {}
    try:
        if args.task == "ingest_bronze":
            from weather_pipeline.bronze.ingest import ingest_bronze
            ingest_bronze(spark, args.run_id, metrics)
        elif args.task == "transform_silver":
            from weather_pipeline.silver.transform import transform_silver
            transform_silver(spark, args.run_id, metrics)
        else:
            from weather_pipeline.gold.aggregate import build_gold
            build_gold(spark, args.run_id, metrics)
    except Exception as exc:
        logging.exception("Task %s failed for run_id=%s", args.task, args.run_id)
        try:
            finish_job_audit(
                spark, audit_id, "FAILED", f"{type(exc).__name__}: {exc}"[:4000], metrics
            )
        except Exception:
            logging.exception("Could not finalize failed job audit for audit_id=%s", audit_id)
        raise
    else:
        finish_job_audit(spark, audit_id, "SUCCESS", metrics=metrics)


if __name__ == "__main__":
    main()
