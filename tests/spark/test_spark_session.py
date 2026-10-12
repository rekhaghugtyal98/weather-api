import pytest


def test_local_spark_session_is_available(spark_session):
    assert spark_session.sparkContext.master.startswith("local")
    assert spark_session.range(1).count() == 1


def test_delta_table_provider_is_configured(spark_session):
    pytest.importorskip("delta")

    assert "DeltaCatalog" in spark_session.conf.get("spark.sql.catalog.spark_catalog")
