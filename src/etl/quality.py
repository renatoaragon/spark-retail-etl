"""Lightweight data quality checks.

Each check returns a ``CheckResult``; ``run_checks`` raises if any fails so the
pipeline stops before writing bad data downstream.
"""

from dataclasses import dataclass

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


def run_checks(checks: list[CheckResult]) -> list[CheckResult]:
    failed = [c for c in checks if not c.passed]
    if failed:
        summary = "; ".join(f"{c.name}: {c.detail}" for c in failed)
        raise DataQualityError(f"Data quality checks failed -> {summary}")
    return checks
