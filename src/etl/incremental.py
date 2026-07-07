"""Incremental load support: a file-based high-watermark on ``order_ts``.

The pipeline processes only source rows newer than the last successful run
instead of re-reading the whole source every time. The watermark is the
maximum ``order_ts`` seen so far, persisted as a plain-text ISO timestamp so
it survives across runs and is trivial to inspect or reset by hand.
"""

from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

WATERMARK_COL = "order_ts"
_TS_FMT = "%Y-%m-%d %H:%M:%S"


def read_watermark(path: str) -> Optional[str]:
    """Return the stored watermark, or ``None`` on the first ever run."""
    p = Path(path)
    if not p.exists():
        return None
    value = p.read_text(encoding="utf-8").strip()
    return value or None


def write_watermark(path: str, value: str) -> None:
    """Persist the watermark, creating the state directory if needed."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(value, encoding="utf-8")


def read_volume_history(path: str) -> list:
    """Return the recorded row counts of past successful runs (oldest first)."""
    p = Path(path)
    if not p.exists():
        return []
    counts = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            counts.append(int(line))
        except ValueError:
            continue
    return counts


def append_volume(path: str, count: int) -> None:
    """Record this run's row count, creating the state directory if needed."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(f"{count}\n")


def select_new(
    orders: DataFrame, watermark: Optional[str], col: str = WATERMARK_COL
) -> DataFrame:
    """Keep only rows strictly newer than ``watermark`` (all rows if ``None``).

    Strict ``>`` means a row exactly on the watermark is not reprocessed; the
    trade-off is that two rows sharing the same timestamp as the watermark on
    the boundary run are skipped. ``order_ts`` carries sub-second resolution in
    practice, so collisions on the exact boundary are unlikely.
    """
    if watermark is None:
        return orders
    return orders.filter(
        F.to_timestamp(F.col(col)) > F.to_timestamp(F.lit(watermark))
    )


def high_watermark(orders: DataFrame, col: str = WATERMARK_COL) -> Optional[str]:
    """Return the max timestamp in this batch as an ISO string (``None`` if empty)."""
    row = orders.select(F.max(F.to_timestamp(F.col(col))).alias("hw")).collect()[0]
    hw = row["hw"]
    return hw.strftime(_TS_FMT) if hw is not None else None
