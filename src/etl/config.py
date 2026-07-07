"""Pipeline configuration.

Paths are relative to the project root by default so the pipeline runs
out of the box with the sample data committed under ``data/``.
"""

from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Paths:
    raw_orders: str = str(PROJECT_ROOT / "data" / "raw" / "orders.csv")
    raw_customers: str = str(PROJECT_ROOT / "data" / "raw" / "customers.csv")
    # Accumulating clean fact layer, appended to on each incremental run.
    clean: str = str(PROJECT_ROOT / "data" / "clean" / "orders")
    curated: str = str(PROJECT_ROOT / "data" / "curated" / "daily_category_revenue")
    # Small state file holding the high-watermark of the last successful run.
    watermark: str = str(PROJECT_ROOT / "data" / "_state" / "orders.watermark")
    # Row counts of past successful runs, for the volume-anomaly check.
    volume_history: str = str(PROJECT_ROOT / "data" / "_state" / "volumes.log")


PATHS = Paths()
