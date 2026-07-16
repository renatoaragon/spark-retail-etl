"""Lightweight data quality checks.

Each check returns a ``CheckResult``; ``run_checks`` raises if any fails so the
pipeline stops before writing bad data downstream.
"""

from dataclasses import dataclass
from statistics import median

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(Exception):
    """Raised when one or more data quality checks fail."""


def check_not_null(df: DataFrame, column: str) -> CheckResult:
    nulls = df.filter(F.col(column).isNull()).count()
    return CheckResult(
        name=f"not_null[{column}]",
        passed=nulls == 0,
        detail=f"{nulls} null(s)",
    )


def check_non_negative(df: DataFrame, column: str) -> CheckResult:
    negatives = df.filter(F.col(column) < 0).count()
    return CheckResult(
        name=f"non_negative[{column}]",
        passed=negatives == 0,
        detail=f"{negatives} negative value(s)",
    )


def check_unique(df: DataFrame, column: str) -> CheckResult:
    total = df.count()
    distinct = df.select(column).distinct().count()
    return CheckResult(
        name=f"unique[{column}]",
        passed=total == distinct,
        detail=f"{total - distinct} duplicate(s)",
    )


def check_referential_integrity(
    df: DataFrame, column: str, dimension: DataFrame, dim_column: str
) -> CheckResult:
    """Every ``column`` value must exist in the dimension's ``dim_column``.

    An orphan passes every single-column check and then silently loses its
    enrichment downstream (the customer join is a left join: the row survives
    with a null country instead of failing). Cheaper to fail here than to
    reverse-engineer a mart discrepancy later.
    """
    orphans = (
        df.select(column)
        .distinct()
        .join(dimension.select(dim_column).distinct(), df[column] == dimension[dim_column], "left_anti")
        .count()
    )
    return CheckResult(
        name=f"referential_integrity[{column}->{dim_column}]",
        passed=orphans == 0,
        detail=f"{orphans} orphan value(s)",
    )


def check_volume_anomaly(
    current: int,
    history: list[int],
    tolerance: float = 0.5,
    min_history: int = 3,
) -> CheckResult:
    """Flag a run whose row count deviates abnormally from recent history.

    Compares ``current`` against the **median** of prior run counts (robust to a
    single outlier) and fails if it falls outside ``median * (1 ± tolerance)``.
    Until ``min_history`` runs have been recorded there is no baseline to judge
    against, so the check passes. A non-positive baseline (e.g. all-zero history)
    also passes, since a ratio band is meaningless there.

    Catches upstream breakage that the per-row checks cannot see: a truncated
    source (far too few rows) or a duplicated/fan-out load (far too many).
    """
    n = len(history)
    if n < min_history:
        return CheckResult(
            name="volume_anomaly",
            passed=True,
            detail=f"insufficient history (n={n} < {min_history})",
        )

    baseline = median(history)
    if baseline <= 0:
        return CheckResult(
            name="volume_anomaly",
            passed=True,
            detail=f"no positive baseline (median={baseline:g})",
        )

    lower = baseline * (1 - tolerance)
    upper = baseline * (1 + tolerance)
    return CheckResult(
        name="volume_anomaly",
        passed=lower <= current <= upper,
        detail=(
            f"{current} rows vs baseline {baseline:g} "
            f"(allowed {lower:g}..{upper:g}, ±{tolerance:.0%})"
        ),
    )


def run_checks(checks: list[CheckResult]) -> list[CheckResult]:
    failed = [c for c in checks if not c.passed]
    if failed:
        summary = "; ".join(f"{c.name}: {c.detail}" for c in failed)
        raise DataQualityError(f"Data quality checks failed -> {summary}")
    return checks
