"""Structured summary of the last successful pipeline run.

Each run that completes writes one small JSON document under ``data/_state/``:
what mode ran, how many rows moved, how the watermark advanced, what every
quality check said, and how long it took. It is the machine-readable answer to
"what did the last run do?" — inspectable by hand, and the natural hook for a
scheduler or alerting to read instead of scraping logs.

Deliberately written **only on success** (including the "no new data" case): if
a run fails mid-way, the file keeps describing the last run whose outputs can be
trusted, mirroring how the watermark itself behaves.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from etl.quality import CheckResult

STATUS_SUCCESS = "success"
STATUS_NO_NEW_DATA = "no_new_data"


@dataclass(frozen=True)
class RunSummary:
    status: str  # success | no_new_data
    mode: str  # incremental | full_refresh
    started_at: str  # ISO timestamp, UTC
    duration_seconds: float
    rows_in_batch: int  # cleaned rows this run (0 when no new data)
    watermark_before: Optional[str]
    watermark_after: Optional[str]
    checks: list = field(default_factory=list)  # one dict per quality check


def checks_as_dicts(checks: list[CheckResult]) -> list[dict]:
    """Flatten CheckResults to plain dicts so the summary is JSON-serializable."""
    return [asdict(c) for c in checks]


def write_summary(path: str, summary: RunSummary) -> None:
    """Persist the summary, creating the state directory if needed."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(asdict(summary), indent=2) + "\n", encoding="utf-8")


def read_summary(path: str) -> Optional[dict]:
    """Return the last run's summary as a dict, or ``None`` if never written."""
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))
