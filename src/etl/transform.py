"""Transform layer: clean, enrich and aggregate.

Each function takes and returns a DataFrame so steps are easy to unit test
in isolation.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def latest_per_order(orders: DataFrame) -> DataFrame:
    """Keep the latest row per ``order_id`` by parsed ``order_ts``.

    Used both within a single batch (in :func:`clean_orders`) and across the
    whole accumulating clean layer when the mart is rebuilt, so a corrected
    order landing in a later incremental batch supersedes the earlier one.
    """
    dedup_window = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc())
    return (
        orders.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


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

    return latest_per_order(valid)


def distinct_dates(orders: DataFrame, col: str = "order_date") -> list:
    """Return the distinct partition dates present in a batch.

    Used to scope the incremental mart rebuild to only the day-partitions the
    current batch touched.
    """
    return [row[col] for row in orders.select(col).distinct().collect()]


def enrich_orders(clean: DataFrame, customers: DataFrame) -> DataFrame:
    """Add revenue and customer country via a left join."""
    with_revenue = clean.withColumn(
        "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    return with_revenue.join(
        customers.select("customer_id", "country"), on="customer_id", how="left"
    )


def daily_category_revenue(enriched: DataFrame) -> DataFrame:
    """Curated mart: revenue, order/customer counts and average order value.

    Beyond the raw totals, two figures an analyst always asks for next:
    ``customers`` (distinct buyers, so repeat business doesn't read as reach)
    and ``avg_order_value`` (the average ticket, computed from the same
    de-duplicated order count rather than from row totals).
    """
    return (
        enriched.groupBy("order_date", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.sum("quantity").alias("units"),
        )
        # Ticket médio = revenue / distinct orders. Guard against a zero order
        # count defensively; a group only exists because it has rows, but the
        # division should never be the thing that fails a run.
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_revenue") / F.when(F.col("orders") > 0, F.col("orders")), 2),
        )
        .orderBy("order_date", "category")
    )
