import pytest

from etl.quality import (
    DataQualityError,
    check_non_negative,
    check_not_null,
    check_unique,
    run_checks,
)


def test_not_null_detects_nulls(spark):
    df = spark.createDataFrame([(1,), (None,)], ["x"])
    assert check_not_null(df, "x").passed is False


def test_non_negative_passes(spark):
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    assert check_non_negative(df, "x").passed is True


def test_unique_detects_duplicates(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], ["id"])
    assert check_unique(df, "id").passed is False


def test_run_checks_raises_on_failure(spark):
    df = spark.createDataFrame([(None,)], ["x"])
    with pytest.raises(DataQualityError):
        run_checks([check_not_null(df, "x")])
