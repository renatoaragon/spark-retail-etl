"""Pipeline entry point: wires extract -> clean -> quality -> enrich -> curated."""

import argparse

from pyspark.sql import SparkSession

from etl.config import PATHS
from etl.extract import read_customers, read_orders
from etl.quality import (
    check_non_negative,
    check_not_null,
    check_unique,
    run_checks,
)
from etl.transform import clean_orders, daily_category_revenue, enrich_orders


def build_spark(app_name: str = "retail-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run(spark: SparkSession) -> None:
    orders = read_orders(spark, PATHS.raw_orders)
    customers = read_customers(spark, PATHS.raw_customers)

    clean = clean_orders(orders)

    run_checks(
        [
            check_not_null(clean, "order_id"),
            check_unique(clean, "order_id"),
            check_non_negative(clean, "unit_price"),
        ]
    )

    curated = daily_category_revenue(enrich_orders(clean, customers))

    curated.write.mode("overwrite").parquet(PATHS.curated)
    print(f"Wrote curated mart to {PATHS.curated}")
    curated.show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail ETL pipeline.")
    parser.parse_args()
    spark = build_spark()
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
