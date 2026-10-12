from unittest.mock import Mock

import pytest

from weather_pipeline.common.data_io import read_data, write_data


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
