import pytest

pytest.importorskip("pyspark", reason="Install the integration dependencies to run Spark tests")
pytest.importorskip("delta", reason="Install delta-spark to run Spark tests")

from weather_pipeline.common.config import table_name


def test_gold_aggregates_daily_weather_metrics(gold_output):
    context = gold_output
    rows = context["spark"].table(table_name("gold")).orderBy(
        "location_id", "forecast_date"
    ).collect()

    expected_rows = context["gold_expected"]
    assert len(rows) == len(expected_rows)
    for row, expected_row in zip(rows, expected_rows):
        actual_row = row.asDict()
        for field, expected in expected_row.items():
            actual = actual_row[field]
            if field == "forecast_date":
                actual = actual.isoformat()
            assert actual == expected
