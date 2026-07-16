import pytest
from pyspark.sql.types import IntegerType, StructField, StructType

from etl.quality import (
    DataQualityError,
    check_non_negative,
    check_not_null,
    check_referential_integrity,
    check_unique,
    check_volume_anomaly,
    run_checks,
)


def test_not_null_detects_nulls(spark):
    schema = StructType([StructField("x", IntegerType(), True)])
    df = spark.createDataFrame([(1,), (None,)], schema)
    assert check_not_null(df, "x").passed is False


def test_non_negative_passes(spark):
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    assert check_non_negative(df, "x").passed is True


def test_unique_detects_duplicates(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], ["id"])
    assert check_unique(df, "id").passed is False


def test_run_checks_raises_on_failure(spark):
    schema = StructType([StructField("x", IntegerType(), True)])
    df = spark.createDataFrame([(None,)], schema)
    with pytest.raises(DataQualityError):
        run_checks([check_not_null(df, "x")])


def test_referential_integrity_detects_orphans(spark):
    orders = spark.createDataFrame([("O1", "C1"), ("O2", "C9")], ["order_id", "customer_id"])
    customers = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])

    result = check_referential_integrity(orders, "customer_id", customers, "customer_id")

    assert result.passed is False
    assert "1 orphan" in result.detail  # C9 has no customer row


def test_referential_integrity_passes_when_all_keys_resolve(spark):
    orders = spark.createDataFrame([("O1", "C1"), ("O2", "C1")], ["order_id", "customer_id"])
    customers = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])

    result = check_referential_integrity(orders, "customer_id", customers, "customer_id")

    assert result.passed is True
    assert result.name == "referential_integrity[customer_id->customer_id]"


# --- volume anomaly (pure function, no Spark needed) ---


def test_volume_anomaly_passes_without_enough_history():
    # Fewer than min_history runs recorded -> no baseline yet, so it passes.
    assert check_volume_anomaly(1_000_000, [100, 100]).passed is True


def test_volume_anomaly_passes_within_band():
    assert check_volume_anomaly(120, [100, 110, 100], tolerance=0.5).passed is True


def test_volume_anomaly_fails_when_too_low():
    # Source truncated to a trickle vs a ~100/run baseline.
    result = check_volume_anomaly(10, [100, 110, 90], tolerance=0.5)
    assert result.passed is False
    assert "baseline" in result.detail


def test_volume_anomaly_fails_when_too_high():
    # Duplicated/fan-out load, far above baseline.
    assert check_volume_anomaly(500, [100, 110, 90], tolerance=0.5).passed is False


def test_volume_anomaly_passes_with_zero_baseline():
    assert check_volume_anomaly(0, [0, 0, 0]).passed is True
