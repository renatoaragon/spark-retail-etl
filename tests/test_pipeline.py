"""End-to-end test of the incremental pipeline over two runs.

Writes Parquet, so it skips on Windows without winutils and runs on CI (Linux).
It exercises the full orchestration in ``pipeline.run`` — extract, clean,
quality gate, clean-layer append, watermark/volume state, and the partitioned
mart rebuild — which the unit tests deliberately do not touch.
"""

import dataclasses

from etl import pipeline
from etl.config import Paths
from etl.incremental import read_volume_history, read_watermark

CUSTOMERS = "customer_id,country\nC1,PT\nC2,ES\nC3,FR\n"

DAY1 = (
    "order_id,customer_id,order_ts,category,quantity,unit_price\n"
    "O1,C1,2025-01-01 10:00:00,books,2,10.0\n"
    "O2,C2,2025-01-01 11:00:00,books,1,30.0\n"
)
DAY2_APPENDED = DAY1 + "O3,C3,2025-01-02 09:00:00,home,1,15.0\n"


def _paths(tmp_path):
    root = tmp_path
    (root / "raw").mkdir()
    (root / "raw" / "customers.csv").write_text(CUSTOMERS, encoding="utf-8")
    return Paths(
        raw_orders=str(root / "raw" / "orders.csv"),
        raw_customers=str(root / "raw" / "customers.csv"),
        clean=str(root / "clean" / "orders"),
        curated=str(root / "curated" / "mart"),
        watermark=str(root / "_state" / "orders.watermark"),
        volume_history=str(root / "_state" / "volumes.log"),
    )


def _mart(spark, paths):
    return {
        (str(r["order_date"]), r["category"]): r
        for r in spark.read.parquet(paths.curated).collect()
    }


def test_incremental_pipeline_end_to_end(spark, tmp_path, parquet_ok, monkeypatch):
    paths = _paths(tmp_path)
    monkeypatch.setattr(pipeline, "PATHS", paths)

    # --- First run: only day 1 exists ---
    (tmp_path / "raw" / "orders.csv").write_text(DAY1, encoding="utf-8")
    pipeline.run(spark, full_refresh=False)

    mart = _mart(spark, paths)
    assert mart[("2025-01-01", "books")]["total_revenue"] == 50.0
    assert mart[("2025-01-01", "books")]["orders"] == 2
    assert read_watermark(paths.watermark) == "2025-01-01 11:00:00"
    assert read_volume_history(paths.volume_history) == [2]

    # --- Second run: day 2 appended to the source ---
    (tmp_path / "raw" / "orders.csv").write_text(DAY2_APPENDED, encoding="utf-8")
    pipeline.run(spark, full_refresh=False)

    mart = _mart(spark, paths)
    # New day added...
    assert mart[("2025-01-02", "home")]["total_revenue"] == 15.0
    # ...and the untouched day-1 partition is still intact.
    assert mart[("2025-01-01", "books")]["total_revenue"] == 50.0
    assert read_watermark(paths.watermark) == "2025-01-02 09:00:00"
    assert read_volume_history(paths.volume_history) == [2, 1]


def test_full_refresh_resets_state(spark, tmp_path, parquet_ok, monkeypatch):
    paths = _paths(tmp_path)
    monkeypatch.setattr(pipeline, "PATHS", paths)

    (tmp_path / "raw" / "orders.csv").write_text(DAY2_APPENDED, encoding="utf-8")
    pipeline.run(spark, full_refresh=False)
    pipeline.run(spark, full_refresh=True)  # rebuild from scratch

    # History is reset and holds only the single full-refresh run.
    assert read_volume_history(paths.volume_history) == [3]
    mart = _mart(spark, paths)
    assert mart[("2025-01-01", "books")]["total_revenue"] == 50.0
    assert mart[("2025-01-02", "home")]["total_revenue"] == 15.0


def test_paths_is_replaceable():
    # Guards the monkeypatch approach above: Paths must accept field overrides.
    base = Paths()
    replaced = dataclasses.replace(base, curated="/tmp/x")
    assert replaced.curated == "/tmp/x"
