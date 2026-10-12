from weather_pipeline.gold import aggregate


def test_transform_silver_data_groups_and_calculates_daily_metrics(monkeypatch):
    from unittest.mock import MagicMock

    silver = MagicMock(name="silver_dataframe")
    with_date = silver.withColumn.return_value
    grouped = with_date.groupBy.return_value
    aggregated = grouped.agg.return_value
    with_updated_at = aggregated.withColumn.return_value
    functions = MagicMock()
    monkeypatch.setattr(aggregate, "F", functions)

    result = aggregate._transform_silver_data(silver)

    assert result is with_updated_at
    functions.to_date.assert_called_once_with("forecast_time")
    silver.withColumn.assert_called_once_with("forecast_date", functions.to_date.return_value)
    with_date.groupBy.assert_called_once_with(
        "location_id", "location_name", "forecast_date"
    )
    assert grouped.agg.call_count == 1
    assert grouped.agg.call_args.args == (
        functions.avg.return_value.alias.return_value,
        functions.min.return_value.alias.return_value,
        functions.max.return_value.alias.return_value,
        functions.avg.return_value.alias.return_value,
        functions.sum.return_value.alias.return_value,
        functions.avg.return_value.alias.return_value,
        functions.count.return_value.alias.return_value,
    )
    assert [entry.args[0] for entry in functions.avg.return_value.alias.call_args_list] == [
        "avg_temperature_c", "avg_humidity_pct", "avg_wind_speed_kmh",
    ]
    functions.min.return_value.alias.assert_called_once_with("min_temperature_c")
    functions.max.return_value.alias.assert_called_once_with("max_temperature_c")
    functions.sum.return_value.alias.assert_called_once_with("total_precipitation_mm")
    functions.count.assert_called_once_with("*")
    functions.count.return_value.alias.assert_called_once_with("hourly_record_count")
    aggregated.withColumn.assert_called_once_with(
        "updated_at", functions.current_timestamp.return_value.cast.return_value
    )
    functions.current_timestamp.return_value.cast.assert_called_once_with("string")
