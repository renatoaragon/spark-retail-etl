from etl.transform import clean_orders, daily_category_revenue, enrich_orders

ORDER_COLS = ["order_id", "customer_id", "order_ts", "category", "quantity", "unit_price"]


def test_clean_drops_invalid_and_dedupes(spark):
    rows = [
        ("O1", "C1", "2025-01-01 10:00:00", "books", 2, 10.0),
        ("O1", "C1", "2025-01-01 11:00:00", "books", 3, 10.0),  # newer dup
        ("O2", None, "2025-01-01 10:00:00", "books", 1, 5.0),   # null customer
        ("O3", "C2", "2025-01-01 10:00:00", "home", 0, 5.0),    # zero qty
        ("O4", "C3", "2025-01-01 10:00:00", "home", 1, -5.0),   # negative price
    ]
    df = spark.createDataFrame(rows, ORDER_COLS)
    clean = clean_orders(df)

    result = {r["order_id"]: r for r in clean.collect()}
    # Only O1 survives, and it keeps the newer row (quantity 3).
    assert list(result) == ["O1"]
    assert result["O1"]["quantity"] == 3


def test_enrich_computes_revenue_and_country(spark):
    orders = spark.createDataFrame(
        [("O1", "C1", "2025-01-01 10:00:00", "books", 2, 10.0)], ORDER_COLS
    )
    clean = clean_orders(orders)
    customers = spark.createDataFrame([("C1", "PT")], ["customer_id", "country"])

    enriched = enrich_orders(clean, customers).collect()[0]
    assert enriched["revenue"] == 20.0
    assert enriched["country"] == "PT"


def test_daily_category_revenue_aggregates(spark):
    orders = spark.createDataFrame(
        [
            ("O1", "C1", "2025-01-01 10:00:00", "books", 2, 10.0),
            ("O2", "C2", "2025-01-01 12:00:00", "books", 1, 30.0),
            ("O3", "C3", "2025-01-02 09:00:00", "home", 1, 15.0),
        ],
        ORDER_COLS,
    )
    clean = clean_orders(orders)
    customers = spark.createDataFrame(
        [("C1", "PT"), ("C2", "ES"), ("C3", "FR")], ["customer_id", "country"]
    )
    mart = {
        (str(r["order_date"]), r["category"]): r
        for r in daily_category_revenue(enrich_orders(clean, customers)).collect()
    }

    books = mart[("2025-01-01", "books")]
    assert books["total_revenue"] == 50.0
    assert books["orders"] == 2
    assert books["units"] == 3
    assert books["customers"] == 2          # two distinct buyers
    assert books["avg_order_value"] == 25.0  # 50.0 / 2 orders


def test_mart_counts_repeat_customer_once(spark):
    # Same customer, two orders, same day/category: two orders but one customer,
    # so avg_order_value stays per-order while customers reflects real reach.
    orders = spark.createDataFrame(
        [
            ("O1", "C1", "2025-01-01 10:00:00", "books", 1, 10.0),
            ("O2", "C1", "2025-01-01 14:00:00", "books", 1, 30.0),
        ],
        ORDER_COLS,
    )
    clean = clean_orders(orders)
    customers = spark.createDataFrame([("C1", "PT")], ["customer_id", "country"])

    row = daily_category_revenue(enrich_orders(clean, customers)).collect()[0]
    assert row["orders"] == 2
    assert row["customers"] == 1            # one buyer, twice
    assert row["avg_order_value"] == 20.0    # 40.0 / 2 orders
