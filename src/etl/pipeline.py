"""Pipeline entry point: wires extract -> clean -> quality -> enrich -> curated.

The pipeline is incremental by default: it processes only source rows newer
than the high-watermark of the last successful run, appends them to an
accumulating clean fact layer, then rebuilds the curated mart from that layer.
Both layers are partitioned by ``order_date`` so an incremental run only
rewrites the day-partitions its batch actually touched. Pass ``--full-refresh``
to ignore the watermark and rebuild everything.
"""

import argparse
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from etl.config import PATHS
from etl.extract import read_customers, read_orders
from etl.incremental import (
    append_volume,
    high_watermark,
    read_volume_history,
    read_watermark,
    select_new,
    write_watermark,
)
from etl.quality import (
    check_non_negative,
    check_not_null,
    check_unique,
    check_volume_anomaly,
    run_checks,
)
from etl.transform import (
    clean_orders,
    daily_category_revenue,
    distinct_dates,
    enrich_orders,
    latest_per_order,
)

PARTITION_COL = "order_date"


def build_spark(app_name: str = "retail-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        # Only replace the partitions present in the written DataFrame, leaving
        # untouched day-partitions in place on an incremental run.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def _clear(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


def run(spark: SparkSession, full_refresh: bool = False) -> None:
    if full_refresh:
        # "From scratch": drop the clean layer, mart and watermark so no stale
        # day-partition survives a rebuild.
        _clear(PATHS.clean)
        _clear(PATHS.curated)
        _clear(PATHS.watermark)
        _clear(PATHS.volume_history)

    orders = read_orders(spark, PATHS.raw_orders)

    watermark = None if full_refresh else read_watermark(PATHS.watermark)
    new_orders = select_new(orders, watermark)

    # Advance the watermark from the raw batch (before cleaning drops rows),
    # so invalid rows are not re-read on the next run just because they were
    # filtered out here.
    batch_high = high_watermark(new_orders)
    if batch_high is None:
        print("No new source rows since last run; nothing to do.")
        return

    clean = clean_orders(new_orders)
    batch_count = clean.count()
    history = [] if full_refresh else read_volume_history(PATHS.volume_history)
    run_checks(
        [
            check_not_null(clean, "order_id"),
            check_unique(clean, "order_id"),
            check_non_negative(clean, "unit_price"),
            check_volume_anomaly(batch_count, history),
        ]
    )

    first_load = full_refresh or watermark is None
    # First load overwrites the clean layer; later runs append new partitions.
    clean.write.partitionBy(PARTITION_COL).mode(
        "overwrite" if first_load else "append"
    ).parquet(PATHS.clean)

    # Rebuild the mart from the clean layer, de-duplicating across batches so a
    # corrected order in a later batch supersedes its earlier version. On an
    # incremental run, only the day-partitions this batch touched are recomputed
    # and (via dynamic overwrite) rewritten; every other day is left untouched.
    all_clean = latest_per_order(spark.read.parquet(PATHS.clean))
    if not first_load:
        touched = distinct_dates(clean, PARTITION_COL)
        all_clean = all_clean.filter(F.col(PARTITION_COL).isin(touched))

    customers = read_customers(spark, PATHS.raw_customers)
    curated = daily_category_revenue(enrich_orders(all_clean, customers))

    curated.write.partitionBy(PARTITION_COL).mode("overwrite").parquet(PATHS.curated)

    # Advance the watermark and record this run's volume only after the mart
    # write succeeds. If anything above fails, the watermark stays put and the
    # whole batch is retried on the next run rather than being silently skipped
    # with a stale mart.
    append_volume(PATHS.volume_history, batch_count)
    write_watermark(PATHS.watermark, batch_high)
    print(
        f"Processed up to watermark {batch_high}; "
        f"wrote curated mart to {PATHS.curated}"
    )
    curated.show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail ETL pipeline.")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore the watermark and rebuild the clean layer and mart from scratch.",
    )
    args = parser.parse_args()
    spark = build_spark()
    try:
        run(spark, full_refresh=args.full_refresh)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
