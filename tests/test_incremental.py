from etl.incremental import (
    append_volume,
    high_watermark,
    read_volume_history,
    read_watermark,
    select_new,
    write_watermark,
)

ORDER_COLS = ["order_id", "customer_id", "order_ts", "category", "quantity", "unit_price"]


def test_watermark_roundtrip(tmp_path):
    path = str(tmp_path / "state" / "orders.watermark")
    assert read_watermark(path) is None  # nothing stored yet

    write_watermark(path, "2025-01-01 10:00:00")
    assert read_watermark(path) == "2025-01-01 10:00:00"

    write_watermark(path, "2025-01-02 08:30:00")  # overwrites
    assert read_watermark(path) == "2025-01-02 08:30:00"


def test_volume_history_roundtrip(tmp_path):
    path = str(tmp_path / "state" / "volumes.log")
    assert read_volume_history(path) == []  # nothing recorded yet

    append_volume(path, 100)
    append_volume(path, 120)
    append_volume(path, 95)
    assert read_volume_history(path) == [100, 120, 95]  # order preserved


def test_select_new_returns_all_when_no_watermark(spark):
    df = spark.createDataFrame(
        [
            ("O1", "C1", "2025-01-01 10:00:00", "books", 1, 5.0),
            ("O2", "C2", "2025-01-02 10:00:00", "home", 1, 5.0),
        ],
        ORDER_COLS,
    )
    assert select_new(df, None).count() == 2


def test_select_new_keeps_only_newer_rows(spark):
    df = spark.createDataFrame(
        [
            ("O1", "C1", "2025-01-01 10:00:00", "books", 1, 5.0),  # older
            ("O2", "C2", "2025-01-02 10:00:00", "home", 1, 5.0),   # on boundary
            ("O3", "C3", "2025-01-03 10:00:00", "toys", 1, 5.0),   # newer
        ],
        ORDER_COLS,
    )
    kept = {r["order_id"] for r in select_new(df, "2025-01-02 10:00:00").collect()}
    assert kept == {"O3"}  # strict '>' excludes the boundary row


def test_high_watermark_returns_max(spark):
    df = spark.createDataFrame(
        [
            ("O1", "C1", "2025-01-01 10:00:00", "books", 1, 5.0),
            ("O2", "C2", "2025-01-03 09:15:00", "home", 1, 5.0),
            ("O3", "C3", "2025-01-02 23:59:00", "toys", 1, 5.0),
        ],
        ORDER_COLS,
    )
    assert high_watermark(df) == "2025-01-03 09:15:00"


def test_high_watermark_empty_is_none(spark):
    empty = spark.createDataFrame([], schema="order_id string, order_ts string")
    assert high_watermark(empty) is None
