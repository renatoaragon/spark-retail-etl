"""Pipeline entry point: wires extract -> clean -> quality -> enrich -> curated.

The pipeline is incremental by default: it processes only source rows newer
than the high-watermark of the last successful run, appends them to an
accumulating clean fact layer, then rebuilds the curated mart from that layer.
Pass ``--full-refresh`` to ignore the watermark and rebuild everything.
"""

import argparse

from pyspark.sql import SparkSession

from etl.config import PATHS
from etl.extract import read_customers, read_orders
from etl.incremental import (
    high_watermark,
    read_watermark,
    select_new,
    write_watermark,
)
from etl.quality import (
    check_non_negative,
    check_not_null,
    check_unique,
    run_checks,
)
from etl.transform import (
    clean_orders,
    daily_category_revenue,
    enrich_orders,
    latest_per_order,
)


def build_spark(app_name: str = "retail-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run(spark: SparkSession, full_refresh: bool = False) -> None:
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
    run_checks(
        [
            check_not_null(clean, "order_id"),
            check_unique(clean, "order_id"),
            check_non_negative(clean, "unit_price"),
        ]
    )

    # First run (or full refresh) overwrites the clean layer; later runs append.
    append = not full_refresh and watermark is not None
    clean.write.mode("append" if append else "overwrite").parquet(PATHS.clean)
    write_watermark(PATHS.watermark, batch_high)

    # Rebuild the mart from the whole clean layer, de-duplicating across batches
    # so a corrected order in a later batch supersedes its earlier version.
    all_clean = latest_per_order(spark.read.parquet(PATHS.clean))
    customers = read_customers(spark, PATHS.raw_customers)
    curated = daily_category_revenue(enrich_orders(all_clean, customers))

    curated.write.mode("overwrite").parquet(PATHS.curated)
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
