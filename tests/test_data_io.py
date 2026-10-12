import sys
import types
from unittest.mock import Mock, patch

import pytest

from weather_pipeline.common.data_io import merge_data, read_data, write_data


def test_read_data_returns_requested_table_dataframe():
    spark = Mock()
    expected_dataframe = Mock(name="expected_dataframe")
    spark.read.format.return_value.option.return_value.table.return_value = expected_dataframe

    result = read_data(spark, "delta", "catalog.schema.weather", schema_mode="merge")

    assert result is expected_dataframe
    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.option.assert_called_once_with("mergeSchema", True)
    spark.read.format.return_value.option.return_value.table.assert_called_once_with(
        "catalog.schema.weather"
    )


def test_write_data_applies_requested_mode_schema_and_partitions():
    dataframe = Mock()
    writer = dataframe.write.format.return_value.mode.return_value
    writer.option.return_value.partitionBy.return_value.saveAsTable = Mock()

    write_data(
        dataframe,
        "delta",
        "catalog.schema.weather",
        schema_mode="merge",
        partitionby=["forecast_date", "location_id"],
        mode="overwrite",
    )

    dataframe.write.format.assert_called_once_with("delta")
    dataframe.write.format.return_value.mode.assert_called_once_with("overwrite")
    writer.option.assert_called_once_with("mergeSchema", True)
    writer.option.return_value.partitionBy.assert_called_once_with(
        "forecast_date", "location_id"
    )
    writer.option.return_value.partitionBy.return_value.saveAsTable.assert_called_once_with(
        "catalog.schema.weather"
    )


def test_read_and_write_failures_are_propagated():
    spark = Mock()
    dataframe = Mock()

    with pytest.raises(ValueError, match="schema_mode"):
        read_data(spark, "delta", "catalog.schema.weather", schema_mode="unknown")
    with pytest.raises(ValueError, match="schema_mode"):
        write_data(dataframe, "delta", "catalog.schema.weather", schema_mode="unknown")
    spark.read.format.assert_not_called()
    dataframe.write.format.assert_not_called()

    spark.read.format.return_value.table.side_effect = RuntimeError("read failed")
    with pytest.raises(RuntimeError, match="read failed"):
        read_data(spark, "delta", "catalog.schema.weather")

    dataframe.write.format.return_value.mode.return_value.saveAsTable.side_effect = (
        RuntimeError("write failed")
    )
    with pytest.raises(RuntimeError, match="write failed"):
        write_data(dataframe, "delta", "catalog.schema.weather")


def test_merge_data_uses_delta_merge_with_requested_keys():
    dataframe = Mock()
    source = Mock(name="source_dataframe")
    dataframe.alias.return_value = source
    target = Mock(name="delta_target")
    target.alias.return_value = target
    merge_builder = Mock(name="merge_builder")
    target.merge.return_value = merge_builder
    merge_builder.whenMatchedUpdateAll.return_value = merge_builder
    merge_builder.whenNotMatchedInsertAll.return_value = merge_builder
    delta_table = Mock()
    delta_table.forName.return_value = target
    delta_module = types.ModuleType("delta")
    delta_module.__path__ = []
    delta_tables_module = types.ModuleType("delta.tables")
    delta_tables_module.DeltaTable = delta_table
    delta_module.tables = delta_tables_module

    with patch.dict(sys.modules, {"delta": delta_module, "delta.tables": delta_tables_module}):
        merge_data(dataframe, "catalog.schema.weather", ("location_id", "forecast_time"))

    delta_table.forName.assert_called_once_with(dataframe.sparkSession, "catalog.schema.weather")
    target.merge.assert_called_once_with(
        source,
        "target.`location_id` = source.`location_id` AND "
        "target.`forecast_time` = source.`forecast_time`",
    )
    merge_builder.whenMatchedUpdateAll.assert_called_once_with()
    merge_builder.whenNotMatchedInsertAll.assert_called_once_with()
    merge_builder.execute.assert_called_once_with()


@pytest.mark.parametrize("merge_keys", [(), ("location_id; DROP TABLE weather",)])
def test_merge_data_rejects_empty_or_unsafe_merge_keys(merge_keys):
    with pytest.raises(ValueError, match="merge_keys"):
        merge_data(Mock(), "catalog.schema.weather", merge_keys)
