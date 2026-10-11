"""Persist one status row per Bronze, Silver, or Gold task execution."""
import json
import os
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .config import table_name
from .utils import safe_identifier


def _ensure_audit_table(spark):
    """Ensure the configured job-audit schema and Delta table exist.

    Args:
        spark: Active Spark session used to issue the schema and table DDL.
    """
    catalog = safe_identifier(os.getenv("WEATHER_CATALOG", "workspace"))
    audit_schema = safe_identifier(os.getenv("WEATHER_AUDIT_SCHEMA", "audit"))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{audit_schema}`")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name("job_audit")} (
          audit_id STRING NOT NULL,
          run_id STRING NOT NULL,
          layer_type STRING NOT NULL,
          status STRING NOT NULL,
          start_time TIMESTAMP NOT NULL,
          end_time TIMESTAMP,
          reason STRING,
          parameters_json STRING
        ) USING DELTA
    """)


def start_job_audit(spark, run_id, layer_type, parameters):
    """Insert or merge a RUNNING audit row for a pipeline task.

    Args:
        spark: Active Spark session used to write the audit table.
        run_id: Identifier for the overall pipeline run.
        layer_type: Pipeline layer name, such as ``bronze``, ``silver``, or ``gold``.
        parameters: JSON-serializable task parameters to retain for diagnostics.

    Returns:
        The generated UUID string identifying this audit row.
    """
    _ensure_audit_table(spark)
    audit_id = str(uuid.uuid4())
    start_schema = StructType([
        StructField("audit_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("layer_type", StringType(), False),
        StructField("status", StringType(), False),
        StructField("parameters_json", StringType(), True),
    ])
    row = [(audit_id, str(run_id), layer_type, "RUNNING", json.dumps(parameters, sort_keys=True))]
    start = (spark.createDataFrame(row, start_schema)
        .withColumn("start_time", F.current_timestamp())
        .withColumn("end_time", F.lit(None).cast("timestamp"))
        .withColumn("reason", F.lit(None).cast("string"))
        .select("audit_id", "run_id", "layer_type", "status", "start_time",
                "end_time", "reason", "parameters_json"))
    view = f"job_audit_start_{uuid.uuid4().hex}"
    start.createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            MERGE INTO {table_name("job_audit")} AS target
            USING {view} AS source
            ON target.audit_id = source.audit_id
            WHEN MATCHED THEN UPDATE SET
              target.run_id = source.run_id,
              target.layer_type = source.layer_type,
              target.status = source.status,
              target.start_time = source.start_time,
              target.end_time = source.end_time,
              target.reason = source.reason,
              target.parameters_json = source.parameters_json
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(view)
    return audit_id


def finish_job_audit(spark, audit_id, status, reason=None):
    """Set the final status, end time, and optional reason on an audit row.

    Args:
        spark: Active Spark session used to update the audit table.
        audit_id: UUID returned by :func:`start_job_audit`.
        status: Final status value, normally ``SUCCESS`` or ``FAILED``.
        reason: Optional failure detail stored with the audit record.
    """
    update_schema = StructType([
        StructField("audit_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("reason", StringType(), True),
    ])
    update = (spark.createDataFrame([(audit_id, status, reason)], update_schema)
        .withColumn("end_time", F.current_timestamp()))
    view = f"job_audit_update_{uuid.uuid4().hex}"
    update.createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            MERGE INTO {table_name("job_audit")} AS target
            USING {view} AS source
            ON target.audit_id = source.audit_id
            WHEN MATCHED THEN UPDATE SET
              target.status = source.status,
              target.end_time = source.end_time,
              target.reason = source.reason
        """)
    finally:
        spark.catalog.dropTempView(view)
