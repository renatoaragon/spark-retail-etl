"""Generate synthetic raw data for the pipeline.

Deterministic (fixed seed) so the committed sample is reproducible. No real or
personal data is used.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
CATEGORIES = ["electronics", "books", "home", "sports", "beauty"]
COUNTRIES = ["PT", "ES", "FR", "DE", "US"]


def generate(n_orders: int = 500, n_customers: int = 60, seed: int = 42) -> None:
    random.seed(seed)
    RAW.mkdir(parents=True, exist_ok=True)

    customers = [f"C{n:04d}" for n in range(1, n_customers + 1)]
    with (RAW / "customers.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "country", "signup_date"])
        for cid in customers:
            signup = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 500))
            w.writerow([cid, random.choice(COUNTRIES), signup.date().isoformat()])

    base = datetime(2025, 1, 1, 8, 0, 0)
    with (RAW / "orders.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            ["order_id", "customer_id", "order_ts", "category", "quantity", "unit_price"]
        )
        for i in range(1, n_orders + 1):
            oid = f"O{i:06d}"
            ts = base + timedelta(minutes=random.randint(0, 60 * 24 * 30))
            row = [
                oid,
                random.choice(customers),
                ts.isoformat(sep=" "),
                random.choice(CATEGORIES),
                random.randint(1, 5),
                round(random.uniform(4.99, 199.99), 2),
            ]
            w.writerow(row)
            # Inject a few dirty rows the pipeline is expected to handle.
            if i % 97 == 0:
                w.writerow([oid, row[1], ts.isoformat(sep=" "), row[3], 1, 9.99])  # dup
            if i % 89 == 0:
                w.writerow([f"O{i:06d}X", "", ts.isoformat(sep=" "), row[3], 0, -1.0])

    print(f"Generated {n_orders} orders and {n_customers} customers in {RAW}")


if __name__ == "__main__":
    generate()
