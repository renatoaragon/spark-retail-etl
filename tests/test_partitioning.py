import os

from pyspark.sql import functions as F

MART_COLS = ["order_date", "category", "total_revenue", "orders", "units"]


def _mart(spark, rows):
    return spark.createDataFrame(rows, MART_COLS).withColumn(
        "order_date", F.to_date("order_date")
    )


def test_curated_is_partitioned_by_date(spark, tmp_path, parquet_ok):
    out = str(tmp_path / "curated")
    _mart(
        spark,
        [
            ("2025-01-01", "books", 50.0, 2, 3),
            ("2025-01-02", "home", 15.0, 1, 1),
        ],
    ).write.partitionBy("order_date").mode("overwrite").parquet(out)

    parts = {d for d in os.listdir(out) if d.startswith("order_date=")}
    assert parts == {"order_date=2025-01-01", "order_date=2025-01-02"}


def test_dynamic_overwrite_touches_only_affected_day(spark, tmp_path, parquet_ok):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out = str(tmp_path / "curated")

    _mart(
        spark,
        [
            ("2025-01-01", "books", 50.0, 2, 3),
            ("2025-01-02", "home", 15.0, 1, 1),
        ],
    ).write.partitionBy("order_date").mode("overwrite").parquet(out)

    # Reprocess only 2025-01-02 with a corrected revenue.
    _mart(spark, [("2025-01-02", "home", 99.0, 1, 1)]).write.partitionBy(
        "order_date"
    ).mode("overwrite").parquet(out)

    result = {
        (str(r["order_date"]), r["category"]): r["total_revenue"]
        for r in spark.read.parquet(out).collect()
    }
    assert result[("2025-01-01", "books")] == 50.0  # untouched day survives
    assert result[("2025-01-02", "home")] == 99.0  # touched day replaced
