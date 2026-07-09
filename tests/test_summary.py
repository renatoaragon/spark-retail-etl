"""Tests for the run summary: pure serialization, no Spark needed."""

from dataclasses import asdict

from etl.quality import CheckResult
from etl.summary import (
    STATUS_SUCCESS,
    RunSummary,
    checks_as_dicts,
    read_summary,
    write_summary,
)


def _summary(**overrides):
    base = dict(
        status=STATUS_SUCCESS,
        mode="incremental",
        started_at="2025-01-01T10:00:00+00:00",
        duration_seconds=1.234,
        rows_in_batch=42,
        watermark_before="2025-01-01 09:00:00",
        watermark_after="2025-01-01 10:00:00",
        checks=[{"name": "not_null[order_id]", "passed": True, "detail": "0 null(s)"}],
    )
    base.update(overrides)
    return RunSummary(**base)


def test_summary_roundtrips_via_json(tmp_path):
    path = str(tmp_path / "_state" / "last_run.json")
    summary = _summary()

    write_summary(path, summary)  # also creates the _state directory

    assert read_summary(path) == asdict(summary)


def test_read_summary_returns_none_when_never_written(tmp_path):
    assert read_summary(str(tmp_path / "missing.json")) is None


def test_checks_as_dicts_flattens_check_results():
    checks = [
        CheckResult(name="unique[order_id]", passed=True, detail="0 duplicate(s)"),
        CheckResult(name="volume_anomaly", passed=False, detail="0 rows vs baseline 10"),
    ]
    flat = checks_as_dicts(checks)
    assert flat == [
        {"name": "unique[order_id]", "passed": True, "detail": "0 duplicate(s)"},
        {"name": "volume_anomaly", "passed": False, "detail": "0 rows vs baseline 10"},
    ]


def test_watermarks_may_be_null_on_first_run(tmp_path):
    path = str(tmp_path / "last_run.json")
    write_summary(path, _summary(watermark_before=None, checks=[]))

    stored = read_summary(path)
    assert stored["watermark_before"] is None
    assert stored["checks"] == []
