"""Shared Spark table read and write helpers."""
from __future__ import annotations

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def read_data(
    spark: SparkSession,
    format: str,
    table_name: str,
    schema_mode: str | None = None,
) -> DataFrame:
    """Read a registered table using the requested Spark data format.

    ``schema_mode`` controls schema merging (for example, ``"merge"`` or
    ``"mergeSchema"``). Leave it unset to use the format's default behavior.
    """
    merge_schema = _schema_merge_enabled(schema_mode) if schema_mode is not None else None
    reader = spark.read.format(format)
    if merge_schema is not None:
        reader = reader.option("mergeSchema", merge_schema)
    return reader.table(table_name)


def write_data(
    dataframe: DataFrame,
    format: str,
    table_name: str,
    schema_mode: str | None = None,
    partitionby: str | list[str] | tuple[str, ...] | None = None,
    mode: str = "append",
) -> None:
    """Write a DataFrame to a registered table with optional schema merging.

    ``schema_mode`` controls Spark's ``mergeSchema`` writer option. ``mode``
    controls the save mode independently. ``partitionby`` accepts one column
    name or a sequence of column names.
    """
    merge_schema = _schema_merge_enabled(schema_mode) if schema_mode is not None else None
    writer = dataframe.write.format(format).mode(mode)
    if merge_schema is not None:
        writer = writer.option("mergeSchema", merge_schema)
    if partitionby:
        columns = [partitionby] if isinstance(partitionby, str) else list(partitionby)
        writer = writer.partitionBy(*columns)
    writer.saveAsTable(table_name)


def merge_data(
    dataframe: DataFrame,
    table_name: str,
    merge_keys: str | Sequence[str],
) -> None:
    """Upsert a DataFrame into an existing Delta table by its key columns.

    Matching rows are updated from the source DataFrame and new rows are
    inserted. The source and target tables must have compatible columns, and
    the target Delta table must already exist.
    """
    keys = (merge_keys,) if isinstance(merge_keys, str) else tuple(merge_keys)
    if not keys:
        raise ValueError("merge_keys must contain at least one column")
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) for key in keys):
        raise ValueError("merge_keys must contain simple column names")

    from delta.tables import DeltaTable

    condition = " AND ".join(
        f"target.`{key}` = source.`{key}`" for key in keys
    )
    target = DeltaTable.forName(dataframe.sparkSession, table_name).alias("target")
    (target.merge(dataframe.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def _schema_merge_enabled(schema_mode: str) -> bool:
    normalized = schema_mode.strip().lower()
    if normalized in {"merge", "mergeschema", "true", "on"}:
        return True
    if normalized in {"none", "strict", "false", "off"}:
        return False
    raise ValueError(
        "schema_mode must be one of: merge, strict, true, false, on, off"
    )
