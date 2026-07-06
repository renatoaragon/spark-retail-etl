"""Transform layer: clean, enrich and aggregate.

Each function takes and returns a DataFrame so steps are easy to unit test
in isolation.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_orders(orders: DataFrame) -> DataFrame:
    """Drop invalid rows, cast types and deduplicate.

    Rules:
      - order_id and customer_id must be present
      - quantity and unit_price must be positive
      - keep the latest row per order_id (by parsed timestamp)
    """
    parsed = orders.withColumn(
        "order_ts", F.to_timestamp("order_ts")
    ).withColumn("order_date", F.to_date("order_ts"))

    valid = parsed.filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") > 0)
    )

    dedup_window = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc())
    return (
        valid.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def enrich_orders(clean: DataFrame, customers: DataFrame) -> DataFrame:
    """Add revenue and customer country via a left join."""
    with_revenue = clean.withColumn(
        "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    return with_revenue.join(
        customers.select("customer_id", "country"), on="customer_id", how="left"
    )


def daily_category_revenue(enriched: DataFrame) -> DataFrame:
    """Curated mart: revenue and order counts per day and category."""
    return (
        enriched.groupBy("order_date", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
        )
        .orderBy("order_date", "category")
    )
