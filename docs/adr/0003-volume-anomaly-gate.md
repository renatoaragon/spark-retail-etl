# 3. Volume anomaly gate on the median of recent runs

- Status: Accepted
- Date: 2026-07

## Context

The per-row quality checks (`not_null`, `unique`, `non_negative`) validate each
row in isolation. A whole class of upstream failures produces rows that are all
individually valid: a truncated export delivers 40 perfect rows instead of
4,000; a duplicated extract or a fan-out join delivers 8,000. The pipeline
would clean, validate and publish either batch without complaint — and the mart
would quietly report half or double the real revenue.

Catching this requires judging the batch **as a whole**, against what "normal"
has looked like recently.

## Decision

`check_volume_anomaly` compares the cleaned batch's row count against the
**median** of recent successful runs (history persisted as a plain-text file
under `data/_state/`, one count per line) and fails the quality gate when the
count falls outside `median ± 50%`.

Three deliberate softenings keep it from crying wolf:

1. **Median, not mean** — one legitimate spike (Black Friday) skews a mean for
   weeks; the median barely moves. The baseline should describe the *typical*
   run, and the median is the typical run.
2. **Silent until history exists** (`min_history = 3`) — with nothing to
   compare against, the check passes rather than blocking a cold start or a
   fresh `--full-refresh`, which clears the history along with the rest of the
   state.
3. **Counts recorded only after full success** — a run that failed the gate (or
   anything else) does not contaminate the baseline with its own anomalous
   count. The same advance-last discipline as the watermark (ADR 0001).

## Consequences

**Positive**

- Catches exactly the failures the row-level checks are blind to: silent
  truncation and silent duplication, before either reaches the mart.
- The history file is human-readable and trivially resettable — consistent with
  every other piece of state this pipeline keeps (ADR 0001).
- Zero new dependencies; the median of a short list needs no statistics engine.

**Negative / trade-offs**

- **±50% is generous.** A 30% truncation passes. The band errs toward "never
  block a legitimate run" for a portfolio pipeline; production would tighten it
  per-source once real variance is known (seasonality-aware bands are the
  natural evolution).
- **Median of the recent window adapts** — a slow drift in volume follows the
  baseline, so gradual erosion is not flagged. This gate targets step-change
  breakage, not trend monitoring; the run summary (`last_run.json`) carries the
  counts a trend monitor would consume.
- A genuinely changed business (real 2× growth) fails the gate once until the
  history refills. Acceptable: one explicit review of a real regime change is a
  feature, not a bug.

## Alternatives considered

- **Fixed thresholds** ("fail under 1,000 rows") — rot silently as the business
  grows; nobody remembers to update them. Rejected.
- **Standard-deviation bands (z-scores)** — sounder statistics, but the mean
  and σ are exactly what a single outlier corrupts, and a short history makes
  both unstable. The median band is cruder and more robust at n=5.
- **Anomaly detection on a time series** (Prophet-style) — the production-grade
  answer at scale, and wild overkill for a nightly batch with a file of ten
  integers as state.
